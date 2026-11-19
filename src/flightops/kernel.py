
"""Flight operation event transforms.

The sequence number is the operational identity used for gap detection: the
numeric part of the source id (``FLI-0007`` -> 7). Extraction lives here so the
store, pipeline and fixture all interpret ids identically.
"""


def seq_of(rec):
    s = "".join(ch for ch in str(rec.get("id", "")) if ch.isdigit())
    return int(s) if s else 0


def _seq(rec):
    return seq_of(rec)


def process(record):
    return {"seq": _seq(record)}


def reduce(records):
    seqs = sorted({r["seq"] for r in records})
    gapset = set()
    for a, b in zip(seqs, seqs[1:]):
        gapset.update(range(a + 1, b))
    for r in records:
        r["next_is_gap"] = int((r["seq"] + 1) in gapset)
    if records:
        records[0]["gap_count"] = len(gapset)
    return records
