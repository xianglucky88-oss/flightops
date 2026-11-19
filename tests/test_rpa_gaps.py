"""RPA (session) pipeline keeps working with the gap ledger present."""
import pytest

from flightops.config import SPEC
from flightops.framework import run_rpa
from flightops.store import Store


@pytest.fixture()
def rpa_ctx(monkeypatch):
    # The captcha gate only exists when the configured engine is rpa.
    monkeypatch.setattr(SPEC, "engine", "rpa")
    con = Store().connect()
    Store(con).reset()
    client = __import__("flightops.framework", fromlist=["Transport"]).Transport().client
    client.get("/scenario/reset")
    try:
        yield con
    finally:
        client.get("/scenario/reset")
        monkeypatch.setattr(SPEC, "engine", "crawl")
        con.close()


def test_rpa_run_includes_gap_ledger_and_preserves_session_flow(rpa_ctx):
    result = run_rpa(con=rpa_ctx)
    assert result["engine"] == "rpa"
    assert result["captcha_solved"] is True
    assert result["submit_created"] >= 1
    # Gap ledger is populated and the run is recorded as completed.
    assert "gaps_detected" in result and "gap_run_id" in result
    store = Store(rpa_ctx)
    runs = store.list_gap_runs()
    assert runs and runs[-1]["outcome"] == "completed"
    # No gaps for the pristine fixture.
    assert result["gaps_open"] == 0
