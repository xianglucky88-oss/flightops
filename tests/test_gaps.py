"""End-to-end proofs for the recoverable gap workflow.

Scenarios covered (mapped to the operational requirements):

* gap detection from cross-page blocks and contiguous holes;
* duplicate input never creates a second copy (store-level + pipeline-level);
* partial failure: 404/503 probes stay open with evidence and cooldown;
* controlled retry: cooldown gating, exhaustion, operator reopen;
* late backfill in any order converges to one consistent result;
* process interruption leaves a recoverable marker and resumes safely;
* full run followed by incremental runs keeps one ledger and old data intact;
* legacy database (created without gap tables) opens and gains the ledger.
"""
import json

import pytest

from flightops.framework import Http, Sleeper, Transport, fetch_event, run
from flightops.gaps import GapManager, backoff_for, consecutive_ranges
from flightops.kernel import seq_of
from flightops.store import Store


# --------------------------------------------------------------------------- #
# Fixture control helpers. The mock service is a process-global singleton, so
# every test starts by restoring its pristine state.
# --------------------------------------------------------------------------- #
@pytest.fixture()
def ctx():
    con = Store().connect()
    Store(con).reset()
    client = Transport().client
    client.get("/scenario/reset")
    try:
        yield {"con": con, "client": client}
    finally:
        client.get("/scenario/reset")
        con.close()


def block(client, *seqs):
    client.get("/scenario/block", params={"seqs": ",".join(map(str, seqs))})


def set_flaky(client, *seqs):
    client.get("/scenario/flaky", params={"seqs": ",".join(map(str, seqs))})


def backfill(client, seq):
    return client.get("/scenario/backfill", params={"seq": seq}).json()


def unblock(client, *seqs):
    params = {"seqs": ",".join(map(str, seqs))} if seqs else {}
    client.get("/scenario/unblock", params=params)


def clear_cooldowns(con):
    """Simulate elapsed time: drop every armed retry gate."""
    con.execute("UPDATE gaps SET next_attempt_after=NULL")
    con.commit()


class Clock:
    """Injectable monotonic clock for cooldown/retry determinism."""

    def __init__(self, start=1_000_000.0):
        self.t = start

    def __call__(self):
        from datetime import datetime, timezone
        return datetime.fromtimestamp(self.t, tz=timezone.utc)

    def advance(self, seconds):
        self.t += seconds


def fresh_backfill_http():
    # A fresh sleeper/transport per probe group keeps attempt accounting local.
    return Http(Transport(), Sleeper(), max_retries=1,
                base_backoff=0.5, politeness=0.0)


# --------------------------------------------------------------------------- #
# 1. Detection: a source-side block across pages leaves an open gap ledger.
# --------------------------------------------------------------------------- #
def test_blocked_sequences_are_registered_as_open_gaps_with_ranges(ctx):
    client, con = ctx["client"], ctx["con"]
    block(client, 3, 4)  # contiguous hole spanning the page boundary

    result = run(con=con)

    store = Store(con)
    gaps = store.list_gaps("open")
    assert {g["seq"] for g in gaps} == {3, 4}
    assert result["gaps_detected"]["ranges"] == [[3, 4]]
    assert result["gaps_open"] == 2
    # Neighbours proving the hole are persisted.
    by_seq = {g["seq"]: g for g in gaps}
    assert (by_seq[3]["low"], by_seq[3]["high"]) == (2, 5)
    assert (by_seq[4]["low"], by_seq[4]["high"]) == (2, 5)
    # Every gap carries detection evidence pointing at the source.
    note = by_seq[3]["evidence"][0]
    assert note["kind"] == "detected" and note["between"] == [2, 5]
    assert note["source"]["endpoint"] == "/items"


# --------------------------------------------------------------------------- #
# 2. Duplicate input: backfill of an already-held event never copies it.
# --------------------------------------------------------------------------- #
def test_backfill_does_not_duplicate_existing_events(ctx):
    client, con = ctx["client"], ctx["con"]
    block(client, 3)
    run(con=con)
    store = Store(con)
    assert store.list_gaps("open")[0]["seq"] == 3
    # Nine distinct source events (1-9 minus the blocked 3), regardless of how
    # the kernel annotates them.
    before = len({r.get("id") for r in store.all_records()})
    # Source publishes the event but it is still blocked: first real attempt
    # is a 404 and the gap stays open.
    backfill(client, 3)
    run(con=con)
    assert {g["seq"] for g in store.list_gaps("open")} == {3}
    assert len({r.get("id") for r in store.all_records()}) == before

    # Restore visibility; the next run probes and ingests exactly once.
    unblock(client, 3)
    clear_cooldowns(con)
    result = run(con=con)
    assert result["gap_backfill"]["resolved"] == [3]
    assert len({r.get("id") for r in store.all_records()}) == before + 1
    assert store.list_gaps("open") == []

    # Re-running with the event visible both in the listing and at /event must
    # not insert a second copy, and no gap is re-registered.
    run(con=con)
    assert len({r.get("id") for r in store.all_records()}) == before + 1
    assert store.list_gaps("open") == []
    ids = [json.loads(p).get("id") for (_h, p)
           in con.execute("SELECT content_hash,payload FROM records")]
    assert ids.count("FLI-0003") == 1


def test_backfill_ingest_guard_blocks_duplicate_by_business_id(ctx):
    from flightops.framework import _ingest_backfill

    store = Store(ctx["con"])
    a = {"id": "FLI-0009", "ts": "t", "v": 1}
    b = {"id": "FLI-0009", "ts": "t", "v": 2, "extra": "annotated differently"}
    assert store.upsert(a) is True
    # Same source id, different content hash (e.g. kernel annotations that
    # differ between batch and single-event processing): the backfill guard
    # must recognise the existing event instead of inserting a second row.
    stored, chash = _ingest_backfill(store, b)
    assert stored is False
    assert chash == store.hash(a)
    assert store.count() == 1
    assert store.find_by_id("FLI-0009")[0] == store.hash(a)


# --------------------------------------------------------------------------- #
# 3. Partial failure: unresolved gaps are never disguised as completed.
# --------------------------------------------------------------------------- #
def test_failed_probes_keep_gap_open_and_record_evidence(ctx):
    client, con = ctx["client"], ctx["con"]
    block(client, 3)
    run(con=con)
    run(con=con)  # second run: one real backfill attempt, still 404

    store = Store(con)
    gap = store.get_gap(3)
    assert gap["status"] == "open"
    assert gap["attempts"] == 1
    assert gap["last_http_status"] == 404
    assert gap["next_attempt_after"] is not None  # cooldown armed
    attempts = store.gap_attempts(3)
    assert len(attempts) == 1
    assert attempts[0]["result"] == "not_found"
    assert attempts[0]["evidence"]["endpoint"] == "/event"
    assert attempts[0]["evidence"]["http_attempts"] >= 1


def test_transient_5xx_is_retried_by_http_layer_then_reported(ctx):
    client, con = ctx["client"], ctx["con"]
    block(client, 3)
    set_flaky(client, 3)
    run(con=con)
    store = Store(con)
    gap = store.get_gap(3)
    assert gap["status"] == "open"  # failure must not resolve the gap
    attempt = store.gap_attempts(3)[-1]
    assert attempt["http_status"] == 503
    # max_retries=1 means two HTTP tries were spent on the flaky endpoint.
    assert attempt["evidence"]["http_attempts"] == 2


# --------------------------------------------------------------------------- #
# 4. Controlled retry: cooldown, exhaustion, reopen - other progress untouched.
# --------------------------------------------------------------------------- #
def test_cooldown_gates_retries_and_budget_exhausts_then_reopens(ctx):
    client, con = ctx["client"], ctx["con"]
    clock = Clock()
    block(client, 3, 5)
    store = Store(con)
    gm = GapManager(store, now=clock, max_attempts=3, backfill_limit=10)
    sleeper = Sleeper()

    # Seed two open gaps without the pipeline's own backfill step, so the
    # controlled-retry exercise starts from attempt zero.
    run(con=con, gap_backfill=False)
    assert {g["seq"] for g in store.list_gaps("open")} == {3, 5}

    def fetcher(seq):
        return fetch_event(Http(Transport(), sleeper, max_retries=0,
                                politeness=0.0), seq)

    def ingest(rec):
        return False, store.hash(rec)

    # Attempt 1 for both gaps.
    run_id, _ = gm.begin_run("backfill")
    stats = gm.backfill_pending(run_id, fetcher, ingest)
    gm.finish_run(run_id, "completed")
    assert set(stats["failed"]) == {3, 5}
    assert store.get_gap(3)["attempts"] == 1

    # Immediately re-running: cooldown blocks every due gap, no new attempts.
    run_id, _ = gm.begin_run("backfill")
    stats = gm.backfill_pending(run_id, fetcher, ingest)
    gm.finish_run(run_id, "completed")
    assert stats["visited"] == 0
    assert store.get_gap(3)["attempts"] == 1  # progress untouched

    # Advance past the first cooldown (30s), then keep failing to exhaustion.
    clock.advance(backoff_for(1) + 1)
    for expected_attempt in (2, 3):
        run_id, _ = gm.begin_run("backfill")
        gm.backfill_pending(run_id, fetcher, ingest)
        gm.finish_run(run_id, "completed")
        assert store.get_gap(3)["attempts"] == expected_attempt
        clock.advance(backoff_for(expected_attempt) + 1)

    # Exhausted: still open and visible, but auto-retry no longer visits it.
    snap = gm.snapshot()
    assert set(snap["exhausted"]) == {3, 5}
    assert snap["open_ranges"] == [[3, 3], [5, 5]]
    run_id, _ = gm.begin_run("backfill")
    stats = gm.backfill_pending(run_id, fetcher, ingest)
    gm.finish_run(run_id, "completed")
    assert stats["visited"] == 0

    # Operator reopen restores eligibility without erasing history.
    history_before = len(store.gap_attempts(3))
    assert gm.reopen(3) is True
    assert store.get_gap(3)["attempts"] == 0
    assert len(store.gap_attempts(3)) == history_before + 1
    assert store.gap_attempts(3)[-1]["result"] == "reopened"
    # The other gap's state is unaffected by reopening this one.
    assert store.get_gap(5)["attempts"] == 3


# --------------------------------------------------------------------------- #
# 5. Order independence: late delivery, contiguous and cross-page converge.
# --------------------------------------------------------------------------- #
def test_gaps_converge_regardless_of_arrival_order(ctx):
    client, con = ctx["client"], ctx["con"]
    block(client, 2, 3, 5)  # contiguous 2-3 and isolated 5
    run(con=con)
    store = Store(con)
    assert {g["seq"] for g in store.list_gaps("open")} == {2, 3, 5}

    # Backfill in an arbitrary order: middle first, then neighbours.
    backfill(client, 3)
    unblock(client, 3)
    clear_cooldowns(con)
    run(con=con)
    assert {g["seq"] for g in store.list_gaps("open")} == {2, 5}

    backfill(client, 5)
    unblock(client, 5)
    backfill(client, 2)
    unblock(client, 2)
    clear_cooldowns(con)
    run(con=con)

    assert store.list_gaps("open") == []
    resolved = {g["seq"]: g for g in store.list_gaps("resolved")}
    assert set(resolved) == {2, 3, 5}
    for seq, gap in resolved.items():
        assert gap["resolved_content_hash"]
        assert gap["resolved_run"]
    # The stored event set is the complete sequence, one row per event.
    seqs = sorted({seq_of(r) for r in store.all_records()})
    assert seqs == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    ids = [r.get("id") for r in store.all_records()]
    assert len(ids) == len(set(ids)) == 9


def test_repeated_runs_are_idempotent_and_keep_one_ledger(ctx):
    client, con = ctx["client"], ctx["con"]
    block(client, 4)
    run(con=con)
    run(con=con)
    store = Store(con)
    assert [g["seq"] for g in store.list_gaps()] == [4]
    # Re-registering the same gap widens nothing and rewrites no history:
    # the first run created the ledger row, and each later run only appends
    # one fresh detection note (two notes for the two later runs).
    gap = store.get_gap(4)
    assert gap["first_seen_run"]
    assert len([n for n in gap["evidence"]
                if isinstance(n, dict) and n.get("kind") == "detected"]) == 2


# --------------------------------------------------------------------------- #
# 6. Process interruption: stale run marker recovered, work resumes.
# --------------------------------------------------------------------------- #
def test_interrupted_run_is_marked_and_recovery_does_not_lose_progress(ctx):
    client, con = ctx["client"], ctx["con"]
    clock = Clock()
    block(client, 3)
    store = Store(con)
    gm = GapManager(store, now=clock, max_attempts=5)

    # First run detects the gap but performs no backfill.
    run(con=con, gap_backfill=False)
    assert store.get_gap(3)["status"] == "open"
    assert store.get_gap(3)["attempts"] == 0

    # Simulate a run that started, performed one failed attempt, then died
    # before finish_run() was called.
    dead_run, _ = gm.begin_run("full")
    gm.backfill_pending(
        dead_run,
        lambda seq: fetch_event(
            Http(Transport(), Sleeper(), max_retries=0, politeness=0.0), seq),
        lambda rec: (True, store.hash(rec)))
    assert store.active_gap_run() == dead_run
    assert store.get_gap(3)["attempts"] == 1

    # New process: begin_run recovers the stale marker.
    clock.advance(backoff_for(1) + 1)
    new_run, stale = gm.begin_run("full")
    assert stale == [dead_run]
    runs = {r["run_id"]: r for r in store.list_gap_runs()}
    assert runs[dead_run]["outcome"] == "interrupted"
    assert runs[new_run]["outcome"] == "running"
    # The dead run's attempt evidence is preserved and the counter continues.
    assert store.get_gap(3)["attempts"] == 1

    # Source now publishes the missing event; the resumed run closes the gap.
    backfill(client, 3)
    unblock(client, 3)
    stats = gm.backfill_pending(
        new_run,
        lambda seq: fetch_event(
            Http(Transport(), Sleeper(), max_retries=0, politeness=0.0), seq),
        lambda rec: (True, store.hash(rec)))
    gm.finish_run(new_run, "completed")
    assert stats["resolved"] == [3]
    assert store.list_gaps("open") == []
    assert store.gap_attempts(3)[0]["run_id"] == dead_run
    assert store.gap_attempts(3)[-1]["run_id"] == new_run


def test_pipeline_failure_marks_run_failed_and_keeps_prior_data(ctx):
    client, con = ctx["client"], ctx["con"]
    block(client, 4)
    # A healthy run first, so old data exists and must survive the failure.
    run(con=con, gap_backfill=False)
    store = Store(con)
    healthy_count = store.count()
    assert {g["seq"] for g in store.list_gaps("open")} == {4}

    gm = GapManager(store)
    run_id, _ = gm.begin_run("full")

    def boom(seq):
        raise RuntimeError("connection reset by peer")

    stats = gm.backfill_pending(run_id, boom, lambda r: (True, "x"))
    gm.finish_run(run_id, "failed", "simulated transport crash")
    assert stats["failed"]
    assert store.count() == healthy_count  # nothing lost, nothing duplicated
    outcome = {r["run_id"]: r["outcome"]
               for r in store.list_gap_runs()}[run_id]
    assert outcome == "failed"
    # Evidence of the exception is retained on the gap.
    gap = store.list_gaps("open")[0]
    attempt = store.gap_attempts(gap["seq"])[-1]
    assert attempt["result"] == "error"
    assert "connection reset" in attempt["detail"]


# --------------------------------------------------------------------------- #
# 7. Full -> incremental switch keeps one ledger and the watermark flow.
# --------------------------------------------------------------------------- #
def test_full_then_incremental_share_one_consistent_ledger(ctx):
    client, con = ctx["client"], ctx["con"]
    block(client, 3)
    full = run(con=con, mode="full")
    assert full["mode"] == "full"
    store = Store(con)
    watermark = store.watermark()
    assert watermark and store.get_gap(3)["status"] == "open"

    inc1 = run(con=con, mode="incremental")
    assert inc1["mode"] == "incremental"
    # Incremental filtering does not erase or duplicate previously held data.
    assert store.count() == full["count"]
    assert store.watermark() == watermark
    assert store.get_gap(3)["status"] == "open"  # still unproven

    # Late delivery resolved through the same ledger while in incremental mode.
    backfill(client, 3)
    unblock(client, 3)
    clear_cooldowns(con)
    inc2 = run(con=con, mode="incremental")
    assert inc2["gap_backfill"]["resolved"] == [3]
    assert store.list_gaps("open") == []
    assert seq_of(store.find_by_id("FLI-0003")[1]) == 3


# --------------------------------------------------------------------------- #
# 8. Operator visibility and CLI.
# --------------------------------------------------------------------------- #
def test_snapshot_exposes_pending_ranges_attempts_and_evidence(ctx):
    client, con = ctx["client"], ctx["con"]
    block(client, 2, 3)
    run(con=con)
    run(con=con)
    snap = GapManager(Store(con)).snapshot()
    assert snap["open_ranges"] == [[2, 3]]
    view = {g["seq"]: g for g in snap["open"]}
    assert view[2]["attempts"] == 1
    assert view[2]["last_http_status"] == 404
    assert view[2]["between"] == [1, 4]
    assert any(n.get("kind") == "detected" for n in view[2]["evidence_notes"])
    assert all(r["outcome"] in ("completed",) for r in snap["runs"])


def test_cli_gaps_and_reopen_commands(ctx, capsys):
    from flightops import cli

    client, con = ctx["client"], ctx["con"]
    block(client, 3)
    run(con=con)
    # Drive the gap to exhaustion through the public manager API. The pipeline
    # run above already spent attempt #1 (with a cooldown); start the manager
    # exercise from a clean scheduling gate with the history preserved.
    clock = Clock()
    store = Store(con)
    clear_cooldowns(con)
    gm = GapManager(store, now=clock, max_attempts=2)
    for _ in range(2):
        rid, _stale = gm.begin_run("backfill")
        gm.backfill_pending(
            rid,
            lambda seq: fetch_event(
                Http(Transport(), Sleeper(), max_retries=0, politeness=0.0), seq),
            lambda rec: (True, store.hash(rec)))
        gm.finish_run(rid, "completed")
        clock.advance(backoff_for(2) + 1)
    assert gm.snapshot()["exhausted"] == [3]

    cli.main(["reopen", "3"])
    out = json.loads(capsys.readouterr().out)
    assert out["reopened"] == [3]
    assert Store(con).get_gap(3)["attempts"] == 0

    cli.main(["gaps"])
    snap = json.loads(capsys.readouterr().out)
    assert [g["seq"] for g in snap["open"]] == [3]
    assert "open_ranges" in snap and "runs" in snap


# --------------------------------------------------------------------------- #
# 9. Legacy database compatibility.
# --------------------------------------------------------------------------- #
def test_legacy_database_without_gap_tables_is_upgraded(ctx):
    con = ctx["con"]
    # Build the pre-feature schema and data, then drop the new tables to
    # emulate a database written by the previous release.
    con.execute("DROP TABLE IF EXISTS gaps")
    con.execute("DROP TABLE IF EXISTS gap_attempts")
    con.execute("DROP TABLE IF EXISTS gap_runs")
    con.execute("INSERT INTO records(content_hash,id,ts,payload) "
                "VALUES('legacyhash','LEG-1','t','{}')")
    con.commit()

    store = Store(con)  # re-init creates the missing tables
    assert store.count() == 1  # old data untouched
    gap, created = store.register_gap(7, 6, 8, "legacy-run",
                                      "2026-11-18T00:00:00Z",
                                      {"kind": "detected"})
    assert created and gap["status"] == "open"
    assert store.find_by_id("LEG-1") is not None
    # Watermark and record APIs keep their old behaviour.
    store.set_watermark("wm-1")
    assert store.watermark() == "wm-1"


# --------------------------------------------------------------------------- #
# 10. Pure helpers.
# --------------------------------------------------------------------------- #
def test_consecutive_ranges_collapses_contiguous_sequences():
    assert consecutive_ranges([1, 2, 3, 5, 7, 8, 11]) == [
        (1, 3), (5, 5), (7, 8), (11, 11)]
    assert consecutive_ranges([]) == []
