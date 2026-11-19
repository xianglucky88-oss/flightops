
import json, sys
from . import pipeline
from .gaps import GapManager
from .store import Store


def _gap_snapshot(con):
    store = Store(con)
    return GapManager(store).snapshot()


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    mode = "full"
    if "--incremental" in argv:
        mode = "incremental"
    if "--full" in argv:
        mode = "full"

    if "gaps" in argv:
        con = Store().connect()
        try:
            print(json.dumps(_gap_snapshot(con), ensure_ascii=False, indent=2))
        finally:
            con.close()
        return

    if "reopen" in argv:
        seqs = [int(a) for a in argv[argv.index("reopen") + 1:]
                if a.lstrip("-").isdigit()]
        con = Store().connect()
        try:
            gm = GapManager(Store(con))
            reopened = [s for s in seqs if gm.reopen(s)]
            print(json.dumps({"reopened": reopened,
                              "not_reopened": [s for s in seqs if s not in reopened]},
                             ensure_ascii=False))
        finally:
            con.close()
        return

    print(json.dumps(pipeline.run(mode=mode), ensure_ascii=False))

if __name__ == "__main__":
    main()
