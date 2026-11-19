
"""Real sqlite3 store: content-hash primary key, INSERT-OR-IGNORE dedup, watermark.

The store also owns the recoverable gap ledger:

* ``gaps``          - one row per missing sequence, open or resolved, with the
  enclosing observed neighbours and retry scheduling state.
* ``gap_attempts``  - append-only evidence of every backfill probe against a
  gap (HTTP status, attempts used, payload summary, errors, reopen notes).
* ``gap_runs``      - lifecycle of every pipeline run, so a process that dies
  mid-run leaves an auditable ``running`` marker that the next run recovers.

All tables are created with ``IF NOT EXISTS``: opening a database written by an
older version simply adds the new tables, and existing rows/meta are untouched.
"""
import json, sqlite3
from .config import DB_PATH


class Store:
    def __init__(self, con=None):
        self.con = con
        if con is not None:
            self._init()

    def connect(self):
        con = sqlite3.connect(DB_PATH)
        self.__init__(con)
        return con

    def _init(self):
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS records("
            "content_hash TEXT PRIMARY KEY, id TEXT, ts TEXT, payload TEXT NOT NULL)")
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
        # ------------------------------------------------------------------ #
        # Recoverable gap ledger.
        # ------------------------------------------------------------------ #
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS gaps("
            "seq INTEGER PRIMARY KEY,"
            "status TEXT NOT NULL,"                 # open | resolved
            "low INTEGER NOT NULL,"                 # neighbour observed below
            "high INTEGER NOT NULL,"                # neighbour observed above
            "first_seen_run TEXT,"
            "resolved_run TEXT,"
            "resolved_content_hash TEXT,"
            "attempts INTEGER NOT NULL DEFAULT 0,"
            "last_attempt_run TEXT,"
            "last_attempt_ts TEXT,"
            "next_attempt_after REAL,"             # epoch seconds, retry gate
            "last_http_status INTEGER,"
            "last_error TEXT,"
            "evidence TEXT NOT NULL DEFAULT '[]',"
            "updated_ts TEXT NOT NULL)")
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS gap_attempts("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "seq INTEGER NOT NULL,"
            "run_id TEXT,"
            "attempt_no INTEGER NOT NULL,"
            "ts TEXT NOT NULL,"
            "result TEXT NOT NULL,"                 # ok | http_error | not_found | error | reopened
            "http_status INTEGER,"
            "detail TEXT,"
            "evidence TEXT NOT NULL DEFAULT '{}')")
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS gap_runs("
            "run_id TEXT PRIMARY KEY,"
            "mode TEXT,"
            "started_ts TEXT NOT NULL,"
            "finished_ts TEXT,"
            "outcome TEXT NOT NULL,"                # running | interrupted | completed | failed
            "detail TEXT)")
        self.con.commit()

    @staticmethod
    def hash(record):
        blob = json.dumps(record, sort_keys=True, ensure_ascii=False)
        import hashlib
        return hashlib.sha256(blob.encode()).hexdigest()[:24]

    def upsert(self, record):
        cur = self.con.execute(
            "INSERT OR IGNORE INTO records(content_hash,id,ts,payload) VALUES(?,?,?,?)",
            (self.hash(record), str(record.get("id", "")),
             str(record.get("ts", "")), json.dumps(record, ensure_ascii=False)))
        self.con.commit()
        return cur.rowcount > 0

    def count(self):
        return self.con.execute("SELECT COUNT(*) FROM records").fetchone()[0]

    def all_records(self):
        return [json.loads(p) for (_h, p) in
                self.con.execute("SELECT content_hash,payload FROM records")]

    def get(self, content_hash):
        row = self.con.execute(
            "SELECT payload FROM records WHERE content_hash=?", (content_hash,)).fetchone()
        return json.loads(row[0]) if row else None

    def find_by_id(self, record_id):
        """Return ``(content_hash, payload_dict)`` for a business id, or None.

        Backfilled events can carry kernel annotations that differ from the
        same event ingested inside a larger batch, so identity for "do we
        already hold this event" is checked on the stable source id, never on
        a content hash alone.
        """
        row = self.con.execute(
            "SELECT content_hash,payload FROM records WHERE id=? LIMIT 1",
            (str(record_id),)).fetchone()
        return (row[0], json.loads(row[1])) if row else None

    def watermark(self):
        row = self.con.execute("SELECT v FROM meta WHERE k='watermark'").fetchone()
        return row[0] if row else None

    def set_watermark(self, value):
        self.con.execute(
            "INSERT INTO meta(k,v) VALUES('watermark',?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(value),))
        self.con.commit()

    # ------------------------------------------------------------------ #
    # Gap ledger.
    # ------------------------------------------------------------------ #
    _GAP_COLS = ("seq,status,low,high,first_seen_run,resolved_run,"
                 "resolved_content_hash,attempts,last_attempt_run,last_attempt_ts,"
                 "next_attempt_after,last_http_status,last_error,evidence,updated_ts")

    def _gap_row(self, row):
        d = dict(zip(self._GAP_COLS.split(","), row))
        d["evidence"] = json.loads(d["evidence"] or "[]")
        return d

    def register_gap(self, seq, low, high, run_id, now_iso, evidence_note):
        """Idempotently register a missing sequence.

        Returns ``(gap, created)``. Re-registering an existing gap (duplicate
        input, repeated runs) never overwrites its attempt history; only the
        observed neighbours are widened.
        """
        existing = self.get_gap(seq)
        if existing is not None:
            if existing["status"] == "open" and (low < existing["low"] or high > existing["high"]):
                self.con.execute(
                    "UPDATE gaps SET low=?, high=?, updated_ts=? WHERE seq=?",
                    (min(low, existing["low"]), max(high, existing["high"]), now_iso, seq))
                self.con.commit()
                existing = self.get_gap(seq)
            return existing, False
        # ``evidence_note`` is a structured, JSON-serialisable dict so the
        # ledger's evidence list stays homogeneous for operators and tooling.
        evidence = json.dumps([evidence_note], ensure_ascii=False)
        self.con.execute(
            "INSERT INTO gaps(seq,status,low,high,first_seen_run,attempts,"
            "evidence,updated_ts) VALUES(?,?,?,?,?,0,?,?)",
            (seq, "open", low, high, run_id, evidence, now_iso))
        self.con.commit()
        return self.get_gap(seq), True

    def get_gap(self, seq):
        row = self.con.execute(
            "SELECT %s FROM gaps WHERE seq=?" % self._GAP_COLS, (seq,)).fetchone()
        return self._gap_row(row) if row else None

    def list_gaps(self, status=None):
        if status:
            cur = self.con.execute(
                "SELECT %s FROM gaps WHERE status=? ORDER BY seq" % self._GAP_COLS, (status,))
        else:
            cur = self.con.execute(
                "SELECT %s FROM gaps ORDER BY seq" % self._GAP_COLS)
        return [self._gap_row(r) for r in cur.fetchall()]

    def record_gap_attempt(self, seq, run_id, attempt_no, now_iso, result,
                           http_status, detail, evidence, next_attempt_after):
        self.con.execute(
            "INSERT INTO gap_attempts(seq,run_id,attempt_no,ts,result,"
            "http_status,detail,evidence) VALUES(?,?,?,?,?,?,?,?)",
            (seq, run_id, attempt_no, now_iso, result, http_status, detail,
             json.dumps(evidence or {}, ensure_ascii=False)))
        self.con.execute(
            "UPDATE gaps SET attempts=?, last_attempt_run=?, last_attempt_ts=?,"
            "next_attempt_after=?, last_http_status=?, last_error=?, updated_ts=? "
            "WHERE seq=?",
            (attempt_no, run_id, now_iso, next_attempt_after, http_status, detail, now_iso, seq))
        self.con.commit()

    def append_gap_note(self, seq, note):
        gap = self.get_gap(seq)
        if gap is None:
            return
        evidence = gap["evidence"] + [note]
        self.con.execute("UPDATE gaps SET evidence=? WHERE seq=?",
                         (json.dumps(evidence, ensure_ascii=False), seq))
        self.con.commit()

    def resolve_gap(self, seq, run_id, content_hash, now_iso):
        self.con.execute(
            "UPDATE gaps SET status='resolved', resolved_run=?, "
            "resolved_content_hash=?, next_attempt_after=NULL, "
            "last_error=NULL, updated_ts=? WHERE seq=?",
            (run_id, content_hash, now_iso, seq))
        self.con.commit()

    def reopen_gap(self, seq, run_id, now_iso):
        """Put an exhausted gap back into the controlled retry queue.

        Attempt rows are preserved as history; the scheduling counter is
        restarted so the retry budget applies to the new investigation.
        """
        gap = self.get_gap(seq)
        if gap is None or gap["status"] != "open":
            return False
        self.con.execute(
            "UPDATE gaps SET attempts=0, next_attempt_after=NULL, "
            "last_error=NULL, updated_ts=? WHERE seq=?", (now_iso, seq))
        self.con.execute(
            "INSERT INTO gap_attempts(seq,run_id,attempt_no,ts,result,"
            "http_status,detail,evidence) VALUES(?,?,?,?, 'reopened', NULL, ?, ?)",
            (seq, run_id, gap["attempts"], now_iso,
             "manual reopen after %d attempts" % gap["attempts"],
             json.dumps({"reopened_by": run_id}, ensure_ascii=False)))
        self.con.commit()
        return True

    def gap_attempts(self, seq):
        cur = self.con.execute(
            "SELECT id,seq,run_id,attempt_no,ts,result,http_status,detail,evidence "
            "FROM gap_attempts WHERE seq=? ORDER BY id", (seq,))
        cols = ["id", "seq", "run_id", "attempt_no", "ts", "result",
                "http_status", "detail", "evidence"]
        out = []
        for row in cur.fetchall():
            d = dict(zip(cols, row))
            d["evidence"] = json.loads(d["evidence"] or "{}")
            out.append(d)
        return out

    # ------------------------------------------------------------------ #
    # Run lifecycle (crash recovery markers).
    # ------------------------------------------------------------------ #
    def start_gap_run(self, run_id, mode, now_iso):
        self.con.execute(
            "INSERT OR IGNORE INTO gap_runs(run_id,mode,started_ts,outcome) "
            "VALUES(?,?,?,'running')", (run_id, mode, now_iso))
        self.con.commit()

    def finish_gap_run(self, run_id, outcome, detail, now_iso):
        self.con.execute(
            "UPDATE gap_runs SET finished_ts=?, outcome=?, detail=? WHERE run_id=?",
            (now_iso, outcome, detail, run_id))
        self.con.commit()

    def active_gap_run(self):
        row = self.con.execute(
            "SELECT run_id FROM gap_runs WHERE outcome='running' "
            "ORDER BY started_ts DESC LIMIT 1").fetchone()
        return row[0] if row else None

    def mark_stale_runs(self, now_iso, recovered_by):
        """Convert runs left ``running`` by a dead process into interrupted."""
        rows = self.con.execute(
            "SELECT run_id FROM gap_runs WHERE outcome='running'").fetchall()
        stale = [r[0] for r in rows]
        for run_id in stale:
            self.con.execute(
                "UPDATE gap_runs SET finished_ts=?, outcome='interrupted', "
                "detail=? WHERE run_id=?",
                (now_iso, "recovered by %s" % recovered_by, run_id))
        self.con.commit()
        return stale

    def list_gap_runs(self):
        cur = self.con.execute(
            "SELECT run_id,mode,started_ts,finished_ts,outcome,detail "
            "FROM gap_runs ORDER BY started_ts")
        cols = ["run_id", "mode", "started_ts", "finished_ts", "outcome", "detail"]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def reset(self):
        self.con.execute("DELETE FROM records")
        self.con.execute("DELETE FROM meta")
        self.con.execute("DELETE FROM gaps")
        self.con.execute("DELETE FROM gap_attempts")
        self.con.execute("DELETE FROM gap_runs")
        self.con.commit()
