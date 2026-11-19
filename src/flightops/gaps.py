"""Recoverable gap ledger: detection, controlled backfill and crash recovery.

Why this module exists
----------------------
When flight operation event sequences jump (1, 2, 5, 6) the missing numbers
are not automatically "records that do not exist". They can be:

* a genuine hole that the source will later backfill;
* a legitimate skip (the sequence will never exist); or
* a temporary blind spot caused by a source-side block.

Only a controlled, evidence-bearing process may tell those apart, and even
then an unproven gap must stay visibly *open*. This module keeps three things
persistent in SQLite (see :mod:`flightops.store`):

* the ranges still owed, with the neighbours that prove them;
* every attempt made against each gap and the source evidence returned;
* the lifecycle of each run, so a process killed mid-run is recoverable.

Guarantees
----------
* Reconciliation is set-based and order-independent: late re-deliveries,
  cross-page holes and contiguous gaps all converge to the same ledger.
* Registration and resolution are idempotent. Backfilled events already held
  in the store are never duplicated (identity is the stable source id, with
  content-hash dedup as a second line of defence).
* An unresolved gap is never marked complete. Failed probes stay ``open``
  behind a cooldown; after ``max_attempts`` the gap is *exhausted* (still
  open, no longer auto-retried) until an operator reopens it.
* Retries are controlled by cooldown + budget and never touch the progress
  recorded for other gaps.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from .kernel import seq_of

#: Seconds a failed probe stays out of the retry queue. Exponential growth is
#: capped so permanently unavailable sources cannot wedge the schedule.
BASE_BACKOFF = 30.0
MAX_BACKOFF = 3600.0


def _utcnow():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.isoformat().replace("+00:00", "Z")


def backoff_for(attempt_no):
    return min(BASE_BACKOFF * (2 ** max(0, attempt_no - 1)), MAX_BACKOFF)


def consecutive_ranges(seqs):
    """Turn an iterable of ints into inclusive ``(low, high)`` ranges."""
    out = []
    for s in sorted(seqs):
        if out and s == out[-1][1] + 1:
            out[-1] = (out[-1][0], s)
        else:
            out.append((s, s))
    return out


class GapManager:
    """Coordinates the gap ledger against one :class:`~flightops.store.Store`."""

    def __init__(self, store, now=None, max_attempts=5, backfill_limit=10):
        self.store = store
        self._now = now or _utcnow
        self.max_attempts = max_attempts
        self.backfill_limit = backfill_limit

    # ------------------------------------------------------------------ #
    # Time helpers (injectable for deterministic tests).
    # ------------------------------------------------------------------ #
    def now_dt(self):
        return self._now()

    def now_iso(self):
        return _iso(self.now_dt())

    def now_epoch(self):
        dt = self.now_dt()
        return dt.timestamp()

    def new_run_id(self):
        return "run-%s-%s" % (self.now_dt().strftime("%Y%m%dT%H%M%S"),
                              uuid.uuid4().hex[:8])

    # ------------------------------------------------------------------ #
    # Run lifecycle.
    # ------------------------------------------------------------------ #
    def begin_run(self, mode):
        """Start a run, first recovering any run killed by a process death."""
        run_id = self.new_run_id()
        stale = self.store.mark_stale_runs(self.now_iso(), run_id)
        self.store.start_gap_run(run_id, mode, self.now_iso())
        return run_id, stale

    def finish_run(self, run_id, outcome, detail=None):
        self.store.finish_gap_run(run_id, outcome, detail, self.now_iso())

    # ------------------------------------------------------------------ #
    # Reconciliation.
    # ------------------------------------------------------------------ #
    def reconcile(self, observed, run_id, page_evidence=None):
        """Register every sequence missing between observed neighbours.

        ``observed`` are the events seen in *this* batch. Detection is purely
        local (adjacent observed sequences), so no assumption is made about
        whether a hole is real, skipped or temporarily invisible. Already
        stored sequences are excluded, which makes a backfilled gap disappear
        without explicit coordination regardless of arrival order.

        Returns ``{"registered": [...], "already_open": n, "ranges": [...]}``.
        """
        page_evidence = page_evidence or {}
        seqs = sorted({seq_of(r) for r in observed if seq_of(r) > 0})
        held = {seq_of(r) for r in self.store.all_records()}
        missing = set()
        neighbours = {}
        for low, high in zip(seqs, seqs[1:]):
            for s in range(low + 1, high):
                if s not in held:
                    missing.add(s)
                    neighbours[s] = (low, high)

        registered, already_open = [], 0
        for s in sorted(missing):
            low, high = neighbours[s]
            note = {
                "kind": "detected",
                "run_id": run_id,
                "ts": self.now_iso(),
                "between": [low, high],
                "source": page_evidence,
            }
            # One detection note per run: register_gap stores it on first
            # sight, later sightings append a fresh run-scoped note, and
            # neither path rewrites the attempt history.
            gap, created = self.store.register_gap(
                s, low, high, run_id, self.now_iso(), note)
            if created:
                registered.append(s)
            else:
                self.store.append_gap_note(s, note)
                already_open += 1
        open_seqs = [g["seq"] for g in self.store.list_gaps("open")]
        return {
            "registered": registered,
            "already_open": already_open,
            "open_count": len(open_seqs),
            "ranges": [list(r) for r in consecutive_ranges(open_seqs)],
        }

    # ------------------------------------------------------------------ #
    # Scheduling.
    # ------------------------------------------------------------------ #
    def is_due(self, gap):
        if gap["status"] != "open":
            return False
        if gap["attempts"] >= self.max_attempts:
            return False
        gate = gap["next_attempt_after"]
        return gate is None or self.now_epoch() >= gate

    def reopen(self, seq):
        """Operator action: give an exhausted gap a fresh, controlled budget."""
        return self.store.reopen_gap(seq, self.new_run_id(), self.now_iso())

    # ------------------------------------------------------------------ #
    # Backfill.
    # ------------------------------------------------------------------ #
    def backfill_pending(self, run_id, fetcher, ingest, limit=None):
        """Probe due gaps, recording evidence for every attempt.

        ``fetcher(seq)`` -> ``(record_or_None, http_status, http_attempts,
        error)`` (see :func:`flightops.framework.fetch_event`).
        ``ingest(record)`` -> ``(stored: bool, content_hash)`` and is expected
        to be idempotent; an event already present is reported as such, never
        copied twice.

        Only due gaps are visited, at most once per run and bounded by
        ``limit``. A gap whose probe fails stays open with a cooldown; when
        the attempt budget is spent it becomes ``exhausted`` - still open,
        still visible, but excluded from automatic retries.
        """
        limit = self.backfill_limit if limit is None else limit
        stats = {"visited": 0, "resolved": [], "failed": [], "exhausted": [],
                 "duplicates": 0, "http_attempts": 0}
        due = [g for g in self.store.list_gaps("open") if self.is_due(g)]
        for gap in due[:limit]:
            seq = gap["seq"]
            stats["visited"] += 1
            attempt_no = gap["attempts"] + 1
            try:
                record, http_status, http_attempts, error = fetcher(seq)
            except Exception as exc:  # transport-level blow-up: evidence, not crash
                stats["http_attempts"] += 1
                self._record_failure(
                    run_id, gap, attempt_no, None, str(exc),
                    {"kind": "exception", "error": str(exc)})
                stats["failed"].append(seq)
                if self._is_exhausted(seq):
                    stats["exhausted"].append(seq)
                continue

            stats["http_attempts"] += http_attempts
            evidence = {
                "kind": "probe",
                "endpoint": "/event",
                "seq": seq,
                "http_status": http_status,
                "http_attempts": http_attempts,
            }
            if record is None:
                # 404/5xx: the absence of a response is not evidence of
                # absence. The gap stays open.
                self._record_failure(run_id, gap, attempt_no, http_status,
                                     error, evidence)
                stats["failed"].append(seq)
                if self._is_exhausted(seq):
                    stats["exhausted"].append(seq)
                continue

            got_seq = seq_of(record)
            if got_seq != seq:
                # Source answered with the wrong record: do not ingest it
                # against this gap.
                evidence["mismatch"] = {"expected": seq, "returned": got_seq}
                self._record_failure(
                    run_id, gap, attempt_no, http_status,
                    "source returned seq=%s for gap %s" % (got_seq, seq), evidence)
                stats["failed"].append(seq)
                if self._is_exhausted(seq):
                    stats["exhausted"].append(seq)
                continue

            stored, content_hash = ingest(record)
            evidence["record_id"] = str(record.get("id", ""))
            evidence["content_hash"] = content_hash
            evidence["stored"] = stored
            self.store.record_gap_attempt(
                seq, run_id, attempt_no, self.now_iso(), "ok", http_status,
                None, evidence, None)
            if stored:
                self.store.resolve_gap(seq, run_id, content_hash, self.now_iso())
                stats["resolved"].append(seq)
            else:
                # Event had already been collected (duplicate input or late
                # re-delivery): close the gap against the existing record
                # without inserting a second copy.
                self.store.resolve_gap(seq, run_id, content_hash, self.now_iso())
                stats["duplicates"] += 1
                stats["resolved"].append(seq)
        return stats

    def _record_failure(self, run_id, gap, attempt_no, http_status, detail, evidence):
        cooldown = backoff_for(attempt_no)
        next_after = self.now_epoch() + cooldown
        evidence["cooldown_seconds"] = cooldown
        result = "not_found" if http_status == 404 else (
            "http_error" if http_status else "error")
        self.store.record_gap_attempt(
            gap["seq"], run_id, attempt_no, self.now_iso(), result,
            http_status, detail, evidence, next_after)
        self.store.append_gap_note(gap["seq"], {
            "kind": "attempt_failed",
            "run_id": run_id,
            "attempt_no": attempt_no,
            "http_status": http_status,
            "detail": detail,
            "cooldown_seconds": cooldown,
            "ts": self.now_iso(),
        })

    def _is_exhausted(self, seq):
        gap = self.store.get_gap(seq)
        return bool(gap and gap["status"] == "open"
                    and gap["attempts"] >= self.max_attempts)

    # ------------------------------------------------------------------ #
    # Operator views.
    # ------------------------------------------------------------------ #
    def snapshot(self):
        """Human/operator view of the ledger state."""
        gaps = self.store.list_gaps()
        open_gaps = [g for g in gaps if g["status"] == "open"]
        exhausted, waiting, due = [], [], []
        for g in open_gaps:
            if g["attempts"] >= self.max_attempts:
                exhausted.append(g["seq"])
            elif self.is_due(g):
                due.append(g["seq"])
            else:
                waiting.append(g["seq"])
        return {
            "open": [self._view(g) for g in open_gaps],
            "resolved": [self._view(g) for g in gaps if g["status"] == "resolved"],
            "open_ranges": [list(r) for r in consecutive_ranges(g["seq"] for g in open_gaps)],
            "due": due,
            "waiting_cooldown": waiting,
            "exhausted": exhausted,
            "runs": self.store.list_gap_runs(),
        }

    def _view(self, gap):
        return {
            "seq": gap["seq"],
            "status": gap["status"],
            "between": [gap["low"], gap["high"]],
            "attempts": gap["attempts"],
            "last_http_status": gap["last_http_status"],
            "last_error": gap["last_error"],
            "first_seen_run": gap["first_seen_run"],
            "resolved_run": gap["resolved_run"],
            "last_attempt_ts": gap["last_attempt_ts"],
            "next_attempt_after": gap["next_attempt_after"],
            "evidence_notes": gap["evidence"],
        }
