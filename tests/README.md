# Test and verification layout

- `tests/processor/`: processor unit tests.
- `tests/integration/`: deployment preflight and bounded OCI/MySQL verification
  harnesses. These load ignored `deploy/env.sh`; set
  `FLOW_MANAGED_STREAM=true` so the FIFO/Parallel verifier owns a fresh Stream,
  creates disposable resources, and preserves them on failure by default.
- `tests/fixtures/sql/`: non-production target schemas used only by integration
  verification.
- `tests/performance/`: deterministic data generators and the unattended FIFO
  performance campaign. `run_fifo_campaign.sh` provisions disposable
  one-partition Streams, Events rules, Container Instances, mappings, and
  targets; runs 10/100/500/1024 MiB plus 1/5/10-mapping workloads; verifies
  durable/TX/row/partition correctness; then removes only resources tagged
  with its unique run id. Generated data, metrics, logs, and HTML reports
  remain outside Git under `/tmp` and `report/`.
- `tests/test_*.py`: shared loader, schema-inventory, and repository-layout
  tests.
- `ui/tests/`: Flask UI tests kept beside the independently buildable UI
  package.

Production code is under `processor/`, `loader_core/`, `ui/myapp/`, and
`deploy/`. Processor and UI container images do not copy test harnesses or
disposable SQL fixtures.

Run the complete campaign from a prepared validation VM without interactive
checkpoints:

```bash
PERF_RUN_ID="$(date -u +%Y%m%d-%H%M%S)" \
  tests/performance/run_fifo_campaign.sh
```

At peak, the default ten-mapping phase creates ten
`CI.Standard.E4.Flex` Container Instances at 1 OCPU and 16 GiB each. Confirm
tenancy service limits and non-production database capacity before starting.
