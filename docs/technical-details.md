# Technical deployment and operations guide

This document contains the detailed material intentionally kept out of the
project summary in the root [README](../README.md).

## Repository layout

```text
function/  OCI Function entry point and streaming partition loader
ui/        Flask operations application
deploy/    Oracle Linux bootstrap and idempotent deployment scripts
tests/     Function tests and fixtures
ui/tests/  Flask UI and OCI-management tests
blog/      Architecture and operational design articles
docs/      Versioned technical implementation documentation
external-reports/  Versioned final measured reports for external sharing
reports/   Git-ignored HTML plans and working assessment artifacts
```

## Runtime flow

1. Object Storage emits a create, update, or delete CloudEvent.
2. An OCI Events rule filters compartment, bucket, and object name, then invokes
   the Function.
3. The intake records the raw event in `object_event`, resolves
   `object_storage_mappings`, and records the selected execution mode.
4. The Function inserts an idempotent work entry into a durable queue bound to
   the target table (default) or mapping (explicit independent ownership).
5. A Sync worker continues in the intake invocation; Detached intake submits a
   short self-invocation. Both must atomically own the queue binding before they
   can claim its first non-terminal entry.
6. The owner drains entries in event-time/received-time/ID order while the
   predicted next operation fits within its safe runtime budget. It heartbeats
   its lease and submits exactly one detached continuation when required.
7. Create/update allocates or retries a per-target batch lease, streams bounded
   Object Storage ranges, preserves CSV record boundaries, and applies
   back-pressure to parallel MySQL writers.
8. Writers load a UUID-suffixed staging table. Validation completes before one
   partition exchange publishes the batch atomically.
9. Delete locates the file's active batch and truncates or retires its
   partition.
10. The loader records completion timing, queue attempt/transport, transaction audit, and detailed errors
   and removes the staging table when possible.

The Function never creates a complete local copy of the CSV. OCI Functions use
memory-backed temporary storage, so downloading large objects first duplicates
I/O, consumes constrained memory/storage, and increases latency. Range streaming
keeps memory bounded by the range, parser, queue, and batch configuration.

## Control and target data

The control database stores resource mappings, raw Object Storage events,
per-file batch ownership, ordered queue lanes/entries/attempts, transaction
audit, and errors. A mapping contains the target database/table, resource
pattern, Sync/Detached mode, writer-worker setting, and queue scope. TABLE scope
serializes every mapping that targets the same table; MAPPING scope is allowed
only for independent non-overlapping ownership. Function timeout and memory are
global OCI resource properties rather than per-mapping values.

Each create/update uses a unique staging table such as
`employees_stage_a1b2c3d4e5f6`. Success and handled failures drop it. A hard
timeout can orphan it; the Registered Table view lists residual stages and
offers individual or confirmed Clean all actions. Cleanup is blocked while a
recent `LOADING` lease indicates active work.

## Operations UI

The Flask UI separates control-plane operations from the Function data plane:

- **Data Import** creates a compatible table from a reviewed CSV and loads it
  with MySQL `LOAD DATA` for an operator-driven import.
- **Resource Mappings / Mappings** maintains routing and selects Sync or
  Detached mode.
- **Resource Mappings / OCI Rules** reads live rules from OCI, identifies rules
  targeting the configured Function, and supports enable/disable, edit, and
  confirmed delete.
- **OCI Function Configuration** reads and updates Sync/Detached timeout,
  memory, provisioned concurrency, default workers, batch rows, range bytes,
  and range-read timeout without discarding unrelated configuration.
- **Object Storage Upload** selects a mapping, derives its bucket/prefix,
  uploads CSV test objects, creates virtual folders by prefix, lists matches,
  and deletes selected objects.
- **Event TX** shows recent and per-table transactions, raw Object Storage
  lifecycle, execution mode, timing in seconds, audit, errors, and full-value
  dialogs for truncated fields.
- **Registered Table / Show Data** server-pages visible target rows and exposes
  staging cleanup.
- **Detached Processes** provides the operational view for long-running work.
- **Queue** shows depth, state, table/mapping bindings, leases, heartbeats,
  completion watermarks, requested mode versus worker transport, attempts, and
  errors. Operators can manually enqueue matching objects, safely edit pending
  scheduling metadata, retry blocked entries, cancel/delete pending work while
  retaining audit history, and wake a detached worker.

This consolidated UI supports operational excellence by making configuration,
test injection, status, latency, error correlation, target verification, and
safe cleanup part of one repeatable workflow. It reduces manual cross-checking
between OCI Console, SQL clients, and Function logs while keeping credentials
and privileged calls server-side.

## Function configuration

Important protected `deploy/env.sh` values include:

| Area | Values |
| --- | --- |
| OCI | compartment, region, subnet, application, repository, bucket, rule and log group |
| Database | `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, control database |
| Execution | `FUNCTION_TIMEOUT` (Sync, max 300), `DETACHED_TIMEOUT_SECONDS` (max 3600), `FUNCTION_MEMORY` |
| Queue | `QUEUE_LEASE_SECONDS`, `QUEUE_REORDER_GRACE_SECONDS`, `QUEUE_SHUTDOWN_RESERVE_SECONDS`, `QUEUE_MINIMUM_START_SECONDS` |
| Streaming | `BATCH_ROWS`, `WRITER_WORKERS`, `OBJECT_STORAGE_RANGE_BYTES`, `OBJECT_STORAGE_READ_TIMEOUT_SECONDS` |
| Detached | `DETACHED_ENABLED`; Function OCID and invoke endpoint are discovered and injected by deployment |
| UI | Flask secret, TLS certificate/key or explicit test-only self-signed setting, OCI-management feature flags |

The deployed defaults are 10,000 rows per writer batch and four workers unless
overridden. Increasing workers increases concurrent MySQL connections and can
move the bottleneck to database storage. Tune against CPU, IOPS, volume
throughput, row width, indexes, and concurrent loads.

## Deployment

```sh
cd deploy
./bootstrap.sh
cp env.sh.example env.sh
chmod 600 env.sh
# Edit the protected file.
./deploy.sh
./deploy_ui.sh
```

The Function deployment creates or updates the application and image, discovers
the Function OCID and invoke endpoint, applies both timeout modes and protected
configuration, configures invocation logging when enabled, and creates/updates
the base Events rule. The UI deployment builds a separate container, binds
Flask only to localhost, installs a systemd service, and exposes nginx HTTPS.

Set `OBJECT_STORAGE_BUCKET_NAME` and optionally
`OBJECT_STORAGE_OBJECT_NAME_PATTERN` (for example `folder/*.csv`). Mapping-
managed rules must be mutually exclusive; OCI invokes every matching rule.
The UI rejects exact duplicates, but operators remain responsible for avoiding
broader wildcard overlap.

## IAM and network requirements

Use separate principals with least privilege:

| Principal | Required purpose |
| --- | --- |
| Deployment/UI Compute instance principal | Manage the scoped Function application, repository, Events rules, Function logs, and UI test objects |
| Function resource principal | Read source objects and invoke the intended Function for Detached mappings |
| OCI Events service | Deliver matching same-tenancy events to the Function |

Prefer an explicit Function OCID in the Function dynamic group:

```text
resource.id = '<function-ocid>'
```

Representative scoped policies are:

```text
Allow dynamic-group <function-dg> to read objects in compartment <bucket-compartment> where all {target.bucket.name='<bucket-name>'}
Allow dynamic-group <function-dg> to use functions-family in compartment <function-compartment> where target.function.id='<function-ocid>'
Allow dynamic-group <deployment-dg> to manage functions-family in compartment <function-compartment>
Allow dynamic-group <deployment-dg> to manage cloudevents-rules in compartment <rule-compartment>
Allow dynamic-group <deployment-dg> to manage logging-family in compartment <logging-compartment>
```

The deployment principal also needs repository and subnet use permissions and,
when UI upload/delete testing is enabled, appropriately scoped Object Storage
permissions. The Function subnet must reach MySQL (normally TCP/3306). MySQL
privileges must cover the control schema and only the approved target/staging
operations. IAM changes can take time to propagate.

Enable events on every source bucket:

```sh
oci os bucket update --name '<bucket-name>' --object-events-enabled true
```

Rules must include the required event types:

```text
com.oraclecloud.objectstorage.createobject
com.oraclecloud.objectstorage.updateobject
com.oraclecloud.objectstorage.deleteobject
```

## Ordering, retries, and file ownership

OCI Events is at-least-once. Sync invocations can overlap and Detached
invocations are independent, so Function arrival order is never used as the
mutation order. Every event is inserted idempotently into a durable queue. One
heartbeated lease owner per binding claims the first non-terminal entry by event
time, received time, priority tie-break, and queue ID. A blocked earlier entry
is not bypassed. Completion advances a binding watermark; a later-delivered
event older than that watermark is blocked for review rather than applied.

TABLE binding is the safe default and coordinates all mappings targeting one
table. MAPPING binding increases concurrency only when mappings own independent
partitions and cannot interact through keys, moves, or delete/create order.
Changing scope or deleting a mapping is blocked while it has non-terminal work.
Event timestamps cannot reveal a producer operation that has not arrived, so
strict cross-file business order still requires a producer manifest or sequence.

A mapping may associate many files with one table, but every active file must
own a disjoint set of business records. Partition exchange makes one file
atomic, not a multi-file publication. To move records between files, remove and
complete the source change before publishing the destination unless a separate
coordinated cutover provides the required consistency.

An abandoned `LOADING` record becomes retryable after `LOAD_LEASE_SECONDS`
(default 120 seconds). A genuinely concurrent load for the same source object
is rejected. Audit records retain the CloudEvent identity and allow errors,
retries, and timing to be correlated.

## Large-file choices

1. Use the UI Data Import workflow for a controlled local/operator import with
   MySQL `LOAD DATA` when event-driven behavior is unnecessary.
2. Use Detached mode for files that cannot reliably complete within the
   300-second Sync limit; OCI permits up to 3,600 seconds.
3. Split larger feeds into smaller, disjoint objects and use a manifest or
   durable sequencer when their order matters.

Detached mode extends the bound; it does not remove it. Earlier 1 GB Sync tests
exceeded 300 seconds; the final controlled run used Detached mode for 1–5 GB.
See the linked final performance report and architecture blog for the test
environment, shapes, worker counts, and storage observations.

## Logging and troubleshooting

When `ENABLE_FUNCTION_LOG=true` and a log group is configured, deployment
creates or reuses the Function invoke log. Use `deploy/showlog.sh` for recent
invocations. Diagnose the flow in order:

1. Confirm bucket object events are enabled and the object name matches exactly
   one live rule.
2. Confirm the rule FAAS action targets the current Function OCID.
3. Look for the raw event and received time in Event TX.
4. Confirm mapping resolution, selected execution mode, and Detached
   self-invocation permission when applicable.
5. Follow transaction state, stage-table creation, row validation, exchange,
   completion time, and cleanup.
6. Correlate an ERROR lifecycle entry with its detailed error record and
   Function invocation log.
7. Check MySQL connectivity, grants, connection limits, CPU, volume throughput,
   and IOPS before increasing workers.

## Validation

```sh
python3.13 -m compileall function ui/myapp
python3.13 -m pytest -q tests ui/tests
bash -n deploy/deploy.sh deploy/deploy_ui.sh deploy/bootstrap.sh
```

No secrets, private keys, OCI tokens, generated runtime environment, or UI
session state should be committed.

## Related documents

- [CSV-to-HeatWave ingestion design](../blog/csv-ingestion-to-heatwave.md)
- [Large-file technical architecture](../blog/technical-architecture-large-csv-heatwave.md)
- [Parallel CSV streaming implementation](csv-stream-parallelization-implementation.md)
- [Diskless parallel CSV streaming implementation](diskless-parallel-csv-streaming-implementation.md)
- [Current VM 6 performance report — MySQL.8 with 1.3 TB storage](../external-reports/performance-test-report-vm6-20260719.md)
- [Prior performance baseline — MySQL.8 with 50 GB storage](../external-reports/performance-test-report-20260719-sync-detached.md)
