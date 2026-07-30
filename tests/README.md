# Test and verification layout

- `tests/processor/`: processor unit tests.
- `tests/integration/`: deployment preflight and bounded OCI/MySQL verification
  harnesses. These load ignored `deploy/env.sh`; set
  `FLOW_MANAGED_STREAM=true` so the FIFO/Parallel verifier owns a fresh Stream,
  creates disposable resources, and preserves them on failure by default.
- `tests/fixtures/sql/`: non-production target schemas used only by integration
  verification.
- `tests/performance/`: deterministic data generators for bounded throughput
  runs. Generated data and metrics remain outside Git.
- `tests/test_*.py`: shared loader, schema-inventory, and repository-layout
  tests.
- `ui/tests/`: Flask UI tests kept beside the independently buildable UI
  package.

Production code is under `processor/`, `loader_core/`, `ui/myapp/`, and
`deploy/`. Processor and UI container images do not copy test harnesses or
disposable SQL fixtures.
