
"""Build deterministic source rows for the local fixture."""
import hashlib
from datetime import datetime, timedelta, timezone
from .config import SPEC

NAMES = ["Alpha Widget", "Beta Gadget", "Gamma Set", "Delta Unit"]
STATUS = ["new", "active", "pending", "sold", "withdrawn"]
SOURCE_START = datetime(2030, 1, 1, tzinfo=timezone.utc)

def _h(salt, i):
    return int(hashlib.md5(("%s-%s-%d" % (SPEC.slug, salt, i)).encode()).hexdigest()[:6], 16)

def canonical_row(i):
    """Deterministic row for sequence ``i``.

    Every source view (paged listing, single-sequence backfill, late
    re-delivery) builds the same row from the same sequence, so a backfilled
    event is byte-identical to what the listing would have returned.
    """
    d = {}
    for c in SPEC.columns:
        if c == "id":
            d[c] = "%s-%04d" % (SPEC.slug[:3].upper(), i)
        elif c == "name":
            d[c] = NAMES[(i - 1) % len(NAMES)]
        elif c == "value":
            d[c] = _h("v", i)
        elif c == "status":
            d[c] = STATUS[(i - 1) % len(STATUS)]
        elif c == "ts":
            d[c] = (SOURCE_START + timedelta(days=i % 5)).isoformat().replace("+00:00", "Z")
        else:
            d[c] = "%s_%d" % (c, _h(c, i) % 97)
    return d

def build_rows():
    ps = SPEC.page_size
    n = ps * 2 + 1
    rows = [canonical_row(i) for i in range(1, n + 1)]
    rows.append(dict(rows[0]))  # in-batch duplicate, exercises dedup
    return rows

def backfill_row(seq):
    """The canonical row for a sequence that was hidden from the listing."""
    return canonical_row(seq)

def extra_row(k):
    d = {}
    for c in SPEC.columns:
        if c == "id":
            d[c] = "%s-X%04d" % (SPEC.slug[:3].upper(), k)
        elif c == "ts":
            d[c] = (SOURCE_START + timedelta(days=30 + k)).isoformat().replace("+00:00", "Z")
        elif c == "name":
            d[c] = NAMES[k % len(NAMES)]
        elif c == "value":
            d[c] = _h("x", k)
        elif c == "status":
            d[c] = "active"
        else:
            d[c] = "%s_new_%d" % (c, k)
    return d
