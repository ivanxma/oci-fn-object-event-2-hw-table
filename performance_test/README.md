# Performance-test setup

`setup.sh` prepares a pre-created empty control/target database for a repeatable
Object Storage ingestion test. It validates the deployed Function and OCI Events
rule, creates the partitioned target table and resource mapping, and adds a
non-secret direct-MySQL UI profile. It never stores the MySQL password in the UI
profile. `--smoke-test` uploads 100 rows, waits for successful ingestion, deletes
the object, waits for successful partition cleanup, and verifies zero final rows.
`--reset` clears only mutable batch/target state and deliberately preserves
Event TX and error audit history.

```bash
cd /home/opc/oci-object-event-2-table
./performance_test/setup.sh
./performance_test/setup.sh --reset
./performance_test/setup.sh --reset --smoke-test
```

The control and target databases must already exist and be granted to the user in
`deploy/env.sh`. The script deliberately does not request server-level `CREATE
DATABASE` privilege. Override defaults with `PERF_TARGET_DATABASE`,
`PERF_TARGET_TABLE`, `PERF_TEST_PREFIX`, `PERF_COMPARTMENT_NAME`,
`PERF_INVOCATION_MODE`, `PERF_DEPLOY_SUFFIX`, or `PERF_PROFILE_NAME` before
running it. By default, the numeric suffix is derived from `FUNCTION_NAME`.

Use `generate_csv.py` with Python 3.13 or later to create larger deterministic
inputs:

```bash
python3.13 performance_test/generate_csv.py --rows 100000 --payload-bytes 128 --output /tmp/perf-100k.csv
```

Run one fully measured case after setup. The runner generates an exact byte
target, changes the mapping mode, uploads, waits for terminal create/delete
status, verifies rows and staging cleanup, and appends one JSON object:

```bash
export PERF_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
./performance_test/run_case.sh 500MiB 524288000 SYNC \
  "performance_test/results/$PERF_RUN_ID/results.jsonl"
./performance_test/run_case.sh 1GiB 1073741824 DETACHED \
  "performance_test/results/$PERF_RUN_ID/results.jsonl"
```

Set `PERF_PAYLOAD_BYTES=12` for the narrow-row comparison workload or leave it
unset for the 480-byte primary payload. Run cases sequentially because each
successful case deletes its object and verifies a zero-row target before the
next case starts.

Object Storage folders are prefixes rather than directory resources. The test
prefix therefore becomes visible when the first CSV is uploaded; creating a
zero-byte folder marker is intentionally avoided because it can emit an unwanted
Object Storage event.
