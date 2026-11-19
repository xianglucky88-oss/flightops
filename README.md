# flightops

A small offline data-processing service with a local HTTP fixture and SQLite storage. It is self-contained and does not require network access.

## Usage

```text
make run       # process the bundled source (full mode)
make run_inc   # process using the incremental command-line mode
make test      # run the test suite
make mock      # start the local fixture service
```

The package exposes a Python entry point under `src/flightops`. The fixture service supports the source formats used by the package, and the command-line entry point writes results to the local SQLite database. Re-running commands is safe for an existing database.

## Recoverable gap handling

Flight operation events carry a monotonic sequence (the numeric part of the
source id, e.g. `FLI-0007` -> `7`). When a run observes `1, 2, 5, 6`, the
missing numbers cannot be declared absent from late arrivals alone: they may
be a genuine hole the source will backfill, a legitimate skip, or a temporary
blind spot caused by a source-side block. The service keeps a persistent,
auditable gap ledger in SQLite so operators can tell those states apart.

### What is recorded

| Table | Purpose |
| --- | --- |
| `gaps` | One row per missing sequence: open/resolved, the neighbours proving the hole, attempt count, retry gate, last HTTP status/error, and structured evidence notes. |
| `gap_attempts` | Append-only source evidence for every backfill probe: run id, attempt number, timestamp, result (`ok`/`not_found`/`http_error`/`error`/`reopened`), HTTP status and probe details. |
| `gap_runs` | Lifecycle of every pipeline run (`running`, `interrupted`, `completed`, `failed`) so a process killed mid-run is recoverable. |

### Behaviour and guarantees

- **Detection is set-based and order-independent.** Contiguous holes,
  cross-page gaps and late re-deliveries all reconcile to the same ledger.
- **Backfills never duplicate.** Ingestion is guarded by the stable source id
  before the content-hash upsert, so an event arriving a second time (via a
  page, a single-sequence probe, or with different batch annotations) closes
  the gap against the existing row instead of inserting a copy.
- **Unproven gaps never look finished.** A `404`/`5xx` probe is recorded as
  evidence and the gap stays `open` behind an exponential cooldown. Only a
  record whose sequence matches the gap can resolve it.
- **Retries are controlled.** Each gap has a bounded attempt budget. Once
  spent it becomes *exhausted* - still open and visible, but no longer
  auto-retried - until an operator reopens it. Other gaps' progress is never
  affected.
- **Interruptions are recoverable.** A new run converts any `running` marker
  left by a dead process into `interrupted`, preserves every attempt already
  made, and resumes from the persisted state.
- **Old data and flows stay valid.** The ledger tables are created with
  `CREATE TABLE IF NOT EXISTS`, so a database from a previous release opens
  unchanged; full and incremental modes share one ledger and watermark.

### Operator commands

```text
python -m flightops.cli                # full collection + reconciliation + backfill
python -m flightops.cli --incremental  # incremental collection against the watermark
python -m flightops.cli gaps           # pending ranges, attempts, evidence, run history
python -m flightops.cli reopen 3 7     # give exhausted gaps a fresh controlled budget
```

The run summary reports `gaps_detected` (registered sequences and open ranges),
`gap_backfill` (visited, resolved, failed, exhausted sequences and HTTP
attempts), `gaps_open`, the `gap_run_id` and any `recovered_runs`.

### Fixture scenario controls

The bundled fixture exposes local endpoints used to exercise the workflow:

```text
GET /scenario/block?seqs=3,4       # hide sequences (temporary source outage)
GET /scenario/flaky?seqs=3         # transient 503 on the single-sequence probe
GET /scenario/backfill?seq=3       # publish a previously missing sequence
GET /scenario/unblock?seqs=3       # restore visibility (empty arg restores all)
GET /scenario/reset                # return the fixture to its pristine state
GET /event?seq=3                   # the single-sequence probe used by backfill
```
