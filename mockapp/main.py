
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import hashlib
from fastapi import FastAPI, Request, Response
from flightops.config import SPEC
from flightops.mockdata import build_rows, extra_row, backfill_row
from flightops.kernel import seq_of

app = FastAPI(title="Local source")
_ALL = build_rows()
_extra = []
_verified_sessions = set()
_accepted_tokens = set()

# --------------------------------------------------------------------------- #
# Gap-scenario state.
#
# _blocked  : sequences hidden from every view (simulates a source outage that
#             makes records temporarily invisible). A single-sequence probe
#             reports them as 404, i.e. "not currently confirmable", never as
#             proof that the sequence does not exist.
# _flaky    : sequences whose single-sequence probe fails with a transient 503
#             before becoming available again.
# _backfill: sequences the operator has published via /backfill; they become
#             visible to the single-sequence probe (and, once unblocked, to
#             the regular listing again).
# These endpoints are local-fixture controls used to demonstrate and test the
# recoverable gap workflow; the package itself only ever reads source data.
# --------------------------------------------------------------------------- #
_blocked = set()
_flaky = set()
_backfilled = set()


def _visible_rows():
    return [r for r in (_ALL + _extra) if seq_of(r) not in _blocked]


def _data():
    return _ALL + _extra

def _page_items(page):
    lo, hi = (page - 1) * SPEC.page_size, page * SPEC.page_size
    return _visible_rows()[lo:hi]

@app.get("/items")
def items(page: int = 1):
    data = _page_items(page)
    if SPEC.fmt == "json":
        return {"items": data, "page": page}
    if SPEC.fmt == "text":
        lines = ["\t".join(str(it.get(c, "")) for c in SPEC.columns) for it in data]
        return Response(content="\n".join(lines), media_type="text/plain")
    th = "".join("<th>%s</th>" % c for c in SPEC.columns)
    trs = "".join("<tr>" + "".join("<td>%s</td>" % it.get(c, "") for c in SPEC.columns)
                  + "</tr>" for it in data)
    return Response(content="<html><table><tr>%s</tr>%s</table></html>" % (th, trs),
                    media_type="text/html")

@app.get("/event")
def event(seq: int):
    """Single-sequence probe used by backfill attempts."""
    if seq in _blocked:
        if seq in _flaky:
            return Response(
                content='{"error":"upstream temporarily unavailable","seq":%d}' % seq,
                status_code=503, media_type="application/json")
        return Response(
            content='{"error":"sequence not currently visible","seq":%d}' % seq,
            status_code=404, media_type="application/json")
    if seq in _flaky:
        return Response(
            content='{"error":"upstream temporarily unavailable","seq":%d}' % seq,
            status_code=503, media_type="application/json")
    for row in _data():
        if seq_of(row) == seq:
            return {"items": [row], "seq": seq}
    if seq in _backfilled:
        return {"items": [backfill_row(seq)], "seq": seq, "backfilled": True}
    return Response(
        content='{"error":"no record for sequence","seq":%d}' % seq,
        status_code=404, media_type="application/json")

@app.get("/scenario/block")
def block(seqs: str):
    """Hide comma-separated sequences from the source (temporary outage)."""
    _blocked.update(int(s) for s in seqs.split(",") if s.strip())
    return {"blocked": sorted(_blocked)}

@app.get("/scenario/flaky")
def flaky(seqs: str):
    """Make comma-separated sequences fail single-sequence probes transiently."""
    _flaky.update(int(s) for s in seqs.split(",") if s.strip())
    return {"flaky": sorted(_flaky)}

@app.get("/scenario/backfill")
def do_backfill(seq: int):
    """Publish a previously missing sequence at the source (late delivery)."""
    _backfilled.add(seq)
    _flaky.discard(seq)
    return {"backfilled": sorted(_backfilled), "blocked": sorted(_blocked),
            "visible_now": seq not in _blocked}

@app.get("/scenario/unblock")
def unblock(seqs: str = ""):
    """Restore hidden sequences; empty argument restores everything."""
    if seqs.strip():
        for s in (int(x) for x in seqs.split(",") if x.strip()):
            _blocked.discard(s)
    else:
        _blocked.clear()
    return {"blocked": sorted(_blocked)}

@app.get("/scenario/reset")
def scenario_reset():
    """Return the fixture to its pristine deterministic state."""
    _blocked.clear()
    _flaky.clear()
    _backfilled.clear()
    return {"ok": True, "rows": len(_data())}

@app.get("/refresh")
def refresh():
    _extra.append(extra_row(len(_extra) + 1))
    return {"total": len(_data())}

@app.get("/probe")
def probe():
    app.__dict__["_flaky_n"] = app.__dict__.get("_flaky_n", 0) + 1
    if app.__dict__["_flaky_n"] <= 2:
        return Response(status_code=429)
    return {"ok": True, "attempts": app.__dict__["_flaky_n"]}

@app.get("/health")
def health():
    return {"ok": True, "rows": len(_data())}

@app.post("/login")
def login(response: Response):
    sid = "SID-local"
    csrf = "CSRF-local"
    challenge = hashlib.sha256((sid + csrf).encode()).hexdigest()[:4]
    response.set_cookie("sid", sid)
    response.set_cookie("csrf", csrf)
    response.set_cookie("challenge", challenge)
    return {"role": "operator"}

@app.get("/query")
def query(request: Request, page: int = 1):
    if not request.cookies.get("sid"):
        return Response(status_code=401)
    if request.headers.get("x-csrf") != request.cookies.get("csrf"):
        return Response(status_code=403)
    items_page = _page_items(page)
    nxt = _page_items(page + 1)
    sid = request.cookies.get("sid")
    gate = (SPEC.engine == "rpa" and page == 1 and sid not in _verified_sessions
            and len(_visible_rows()) > SPEC.page_size)
    return {"items": items_page, "page": page, "has_more": bool(nxt),
            "captcha_required": bool(gate)}

@app.post("/captcha")
async def captcha(request: Request):
    body = await request.json()
    ok = body.get("code") == request.cookies.get("challenge")
    if ok:
        _verified_sessions.add(request.cookies.get("sid"))
    return {"ok": bool(ok)}

@app.post("/submit")
async def submit(request: Request):
    body = await request.json()
    token = body.get("token")
    created = token not in _accepted_tokens
    if created:
        _accepted_tokens.add(token)
    return {"created": bool(created), "token": token}
