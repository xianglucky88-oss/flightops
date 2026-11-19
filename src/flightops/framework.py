"""Local transport, parsing, storage, and session helpers."""
from __future__ import annotations
import hashlib
import importlib
import json

from lxml import html as _lh

from .config import SPEC
from .gaps import GapManager
from .store import Store


# --------------------------------------------------------------------------- #
# Transport helpers.
# --------------------------------------------------------------------------- #
class Transport:
    """Client for the bundled local service."""

    def __init__(self):
        import os
        import sys
        for p in (str(SPEC_ROOT()), str(SPEC_ROOT() / "src")):
            if p not in sys.path:
                sys.path.insert(0, p)
        from starlette.testclient import TestClient
        app = importlib.import_module("mockapp.main").app
        self.client = TestClient(app)
        self.cookies: dict[str, str] = {}

    def request(self, method, path, params=None, json_body=None, headers=None):
        q = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        resp = self.client.request(method, path, params=q, json=json_body,
                                   headers=headers or {}, cookies=self.cookies)
        for k, v in resp.cookies.items():
            self.cookies[k] = v
        return resp.status_code, dict(resp.headers), resp.text


def SPEC_ROOT():
    from .config import ROOT
    return ROOT


class Sleeper:
    """Records requested delays without blocking the caller."""

    def __init__(self):
        self.slept = 0.0
        self.calls = 0

    def sleep(self, seconds):
        self.slept += seconds
        self.calls += 1


class Http:
    """HTTP client with bounded retry support."""

    def __init__(self, transport: Transport, sleeper: Sleeper,
                 max_retries=4, base_backoff=0.5, politeness=0.25):
        self.t = transport
        self.sleeper = sleeper
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.politeness = politeness

    def get(self, path, params=None, headers=None):
        attempt = 0
        while True:
            self.sleeper.sleep(self.politeness)
            status, hdr, body = self.t.request("GET", path, params, headers=headers)
            if status in (429, 500, 502, 503) and attempt < self.max_retries:
                self.sleeper.sleep(self.base_backoff * (2 ** attempt))
                attempt += 1
                continue
            return status, hdr, body, attempt


# --------------------------------------------------------------------------- #
# Response parsing.
# --------------------------------------------------------------------------- #
def _coerce(name, val):
    if name in SPEC.int_fields:
        try:
            return int(val)
        except (TypeError, ValueError):
            return val
    return val


def parse(resp_body, fmt):
    cols = SPEC.columns
    if fmt == "json":
        items = json.loads(resp_body).get("items", [])
        return [{c: _coerce(c, it.get(c)) for c in cols} for it in items]
    if fmt == "html":
        doc = _lh.fromstring(resp_body)
        rows = []
        for tr in doc.xpath("//table//tr[./td]"):
            cells = [td.text_content().strip() for td in tr.xpath("./td")]
            if len(cells) != len(cols):
                continue
            rows.append({c: _coerce(c, v) for c, v in zip(cols, cells)})
        return rows
    if fmt == "text":
        out = []
        for line in resp_body.splitlines():
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split("\t")]
            d = {c: _coerce(c, parts[i]) if i < len(parts) else _coerce(c, "")
                 for i, c in enumerate(cols)}
            out.append(d)
        return out
    raise ValueError("unknown fmt: %s" % fmt)


def fetch_all(http: Http):
    """Collect records from the local source."""
    records, attempts, page = [], 0, 1
    while True:
        status, _hdr, body, att = http.get("/" + SPEC.endpoint, params={"page": page})
        attempts += att + 1
        if status != 200:
            raise RuntimeError("fetch failed status=%s page=%s" % (status, page))
        page_recs = parse(body, SPEC.fmt)
        records.extend(page_recs)
        if len(page_recs) < SPEC.page_size:
            break
        page += 1
        if page > 200:
            break
    return records, attempts


def fetch_event(http: Http, seq):
    """Probe the source for one missing sequence.

    Returns ``(record_or_None, status, http_attempts, error_text)``. A 404 is
    a *non-answer* - the record may simply be temporarily invisible - so it is
    reported, not raised. Backfill uses a tightly bounded retry budget so that
    transient 5xx/429 responses cannot burn the gap's retry budget silently.
    """
    status, _hdr, body, att = http.get("/event", params={"seq": seq})
    attempts = att + 1
    if status == 200:
        items = parse(body, SPEC.fmt)
        return (items[0] if items else None), status, attempts, None
    if status == 404:
        return None, status, attempts, "not currently visible at source"
    return None, status, attempts, "backfill probe failed http=%s" % status


# --------------------------------------------------------------------------- #
# Record identity helpers.
# --------------------------------------------------------------------------- #
def canonical(d):
    return json.dumps(d, sort_keys=True, ensure_ascii=False)


def content_hash(d):
    return hashlib.sha256(canonical(d).encode()).hexdigest()[:24]


def dedup(records):
    seen, out, dup = set(), [], 0
    for r in records:
        h = content_hash(r)
        if h in seen:
            dup += 1
            continue
        seen.add(h)
        out.append(r)
    return out, dup


def normalize(r):
    d = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
    if isinstance(d.get("status"), str):
        d["status"] = d["status"].lower()
    return d


def _apply_kernel(records):
    k = importlib.import_module(".kernel", __package__)
    for r in records:
        if hasattr(k, "process"):
            r.update(k.process(r))
    if hasattr(k, "reduce"):
        records = k.reduce(records)
    return records


# --------------------------------------------------------------------------- #
# Collection pipeline.
# --------------------------------------------------------------------------- #
def _ingest_backfill(store, record):
    """Idempotent ingest for one backfilled event.

    Identity is checked on the source id first so kernel annotations that
    differ between batch and single-event processing can never create a
    duplicate row; content-hash upsert remains the second line of defence.
    """
    record_id = str(record.get("id", ""))
    existing = store.find_by_id(record_id)
    if existing is not None:
        return False, existing[0]
    record = _apply_kernel([dict(record)])[0]
    inserted = store.upsert(record)
    if not inserted:
        row = store.find_by_id(record_id)
        return False, (row[0] if row else store.hash(record))
    return True, store.hash(record)


def _ingest_batch(store, records):
    """Idempotent batch ingest.

    The kernel's ``reduce`` step annotates the *first* record of a batch with
    batch-wide fields (e.g. ``gap_count``), so the same event can hash
    differently depending on which other events were fetched with it. Business
    identity is therefore the stable source id: events already held are
    skipped before the kernel runs, never copied with a new content hash.

    Returns ``(ingested, already_held)``.
    """
    ingested = already_held = 0
    for r in records:
        if store.find_by_id(str(r.get("id", ""))) is not None:
            already_held += 1
            continue
        if store.upsert(r):
            ingested += 1
        else:
            already_held += 1
    return ingested, already_held


def run(mode="full", con=None, gap_backfill=True):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    sleeper = Sleeper()
    http = Http(Transport(), sleeper)
    gm = GapManager(store)
    run_id, stale_runs = gm.begin_run(mode)
    try:
        records, attempts = fetch_all(http)
        norm = [normalize(dict(r)) for r in records]
        norm, dup_in_batch = dedup(norm)
        norm = _apply_kernel(norm)

        wm = store.watermark()
        if mode == "incremental" and wm:
            norm = [r for r in norm if str(r.get("ts", "")) > wm]

        ingested, already_held = _ingest_batch(store, norm)
        total = store.count()
        new_wm = max([str(r.get("ts", "")) for r in norm] + [wm or ""])
        if new_wm:
            store.set_watermark(new_wm)

        # Reconciliation runs on the full page set even in incremental mode:
        # a cross-page hole is about sequence adjacency, not the ts window.
        reconciliation = gm.reconcile(
            records, run_id,
            page_evidence={"endpoint": "/" + SPEC.endpoint,
                           "http_attempts": attempts,
                           "records_in_batch": len(records)})
        gap_stats = {"visited": 0, "resolved": [], "failed": [], "exhausted": [],
                     "duplicates": 0, "http_attempts": 0}
        if gap_backfill:
            backfill_http = Http(Transport(), sleeper, max_retries=1,
                                 base_backoff=0.5, politeness=0.25)
            gap_stats = gm.backfill_pending(
                run_id,
                lambda seq: fetch_event(backfill_http, seq),
                lambda rec: _ingest_backfill(store, rec))
        gm.finish_run(run_id, "completed")
        return {"engine": "crawl", "mode": mode, "fetched": len(records),
                "duplicates": dup_in_batch, "ingested": ingested,
                "already_held": already_held, "count": total,
                "http_attempts": attempts, "slept": round(sleeper.slept, 4),
                "watermark": store.watermark(),
                "gap_run_id": run_id, "recovered_runs": stale_runs,
                "gaps_detected": reconciliation,
                "gap_backfill": gap_stats,
                "gaps_open": len(store.list_gaps("open"))}
    except Exception:
        gm.finish_run(run_id, "failed", "pipeline raised before completion")
        raise
    finally:
        if own:
            con.close()


# --------------------------------------------------------------------------- #
# Session pipeline.
# --------------------------------------------------------------------------- #
class Rpa:
    def __init__(self):
        self.t = Transport()
        self.sleeper = Sleeper()
        self.log = []
        self.captcha_solved = False

    def login(self):
        status, _h, _b = self.t.request("POST", "/login",
                                        json_body={"u": "operator", "p": "local"})
        if status != 200:
            raise RuntimeError("login failed status=%s" % status)
        self.log.append("login")
        return True

    def _csrf(self):
        return {"x-csrf": self.t.cookies.get("csrf", "")}

    def _query_page(self, page):
        status, _h, body = self.t.request("GET", "/query", params={"page": page},
                                          headers=self._csrf())
        if status != 200:
            raise RuntimeError("query failed status=%s" % status)
        return json.loads(body)

    def collect(self):
        out, page = [], 1
        while True:
            self.sleeper.sleep(0.1)
            data = self._query_page(page)
            out.extend(data.get("items", []))
            if data.get("captcha_required") and not self.captcha_solved:
                ok = self._solve_captcha()
                if not ok:
                    raise RuntimeError("captcha solve rejected")
                self.captcha_solved = True
                page += 1
                continue
            if not data.get("has_more"):
                break
            page += 1
            if page > 200:
                break
        self.log.append("collect:%d" % len(out))
        return out

    def _solve_captcha(self):
        status, _h, body = self.t.request("POST", "/captcha",
                                          json_body={"code": self.t.cookies.get("challenge", "")},
                                          headers=self._csrf())
        ok = status == 200 and json.loads(body).get("ok") is True
        self.log.append("captcha:%s" % ("solved" if ok else "failed"))
        return ok

    def submit(self, record):
        token = content_hash(record)
        status, _h, body = self.t.request("POST", "/submit",
                                          json_body={"token": token, "record": record},
                                          headers=self._csrf())
        data = json.loads(body)
        self.log.append("submit:%s" % ("created" if data.get("created") else "dedup"))
        return bool(data.get("created"))


def run_rpa(con=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    r = Rpa()
    gm = GapManager(store)
    run_id, stale_runs = gm.begin_run("rpa")
    try:
        logged = r.login()
        items = r.collect()
        norm = [normalize(dict(it)) for it in items]
        norm, dup = dedup(norm)
        norm = _apply_kernel(norm)
        ingested, _already = _ingest_batch(store, norm)
        total = store.count()
        created = dedup_submit = 0
        for x in norm[:2]:
            if r.submit(x):
                created += 1
            else:
                dedup_submit += 1
        for x in norm[:1]:
            if not r.submit(x):
                dedup_submit += 1
        wm = max([str(x.get("ts", "")) for x in norm] + [""])
        if wm:
            store.set_watermark(wm)
        reconciliation = gm.reconcile(
            items, run_id,
            page_evidence={"endpoint": "/query", "collected": len(items)})
        gm.finish_run(run_id, "completed")
        return {"engine": "rpa", "logged_in": logged, "captcha_solved": r.captcha_solved,
                "collected": len(items), "fetched": len(items), "duplicates": dup,
                "ingested": ingested, "count": total,
                "submit_created": created, "submit_dedup": dedup_submit, "log": r.log,
                "gap_run_id": run_id, "recovered_runs": stale_runs,
                "gaps_detected": reconciliation,
                "gaps_open": len(store.list_gaps("open"))}
    except Exception:
        gm.finish_run(run_id, "failed", "rpa pipeline raised before completion")
        raise
    finally:
        if own:
            con.close()
