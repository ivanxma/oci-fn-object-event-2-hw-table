# CSV to HeatWave performance test

Clean sequential ingestion run using synchronous OCI Function execution through 500 MB, followed by mapping-driven detached execution at 1 GB, 2 GB, and 5 GB.

Run prefix: `performance/retest-20260719T1008Z/perf_t_001/` · Test window: 2026-07-19 10:20–11:12 UTC · VM: testvm04

## Executive result

- **10 / 10** — successful create/import scenarios
- **5 GB** — largest successful object
- **1,408.743 s** — 5 GB detached audit duration
- **82,736,033** — rows validated in the 5 GB case
- **0** — final rows, staging tables, active batches, and live test objects

**Outcome:** synchronous ingestion succeeded through 500 MB. Detached ingestion succeeded at 1 GB, 2 GB, and 5 GB, including exact row-count checks and partition cleanup after every case.

## 1. Architecture and verified configuration

- **Test VM** — Generate deterministic CSV and multipart upload
- **Object Storage** — Fresh isolated object prefix
- **OCI Events** — Bucket and resource-name filter
- **OCI Function** — Sync handler or detached self-invocation
- **HeatWave MySQL** — Stream, stage, validate, exchange partition

| Component | Configuration | Evidence |
| --- | --- | --- |
| OCI Function | `object-storage-heatwave-app5/object-storage-heatwave5` | Live OCI Function lookup |
| Function image | `object-storage-heatwave5:0.1.1` | Live OCI Function lookup |
| Function memory | 2,048 MB | Live OCI Function lookup |
| Timeouts | SYNC 300 seconds; DETACHED 3,600 seconds | Live OCI Function lookup |
| Streaming | 32 MiB Object Storage ranges; no Function-local CSV file | `OBJECT_STORAGE_RANGE_BYTES=33554432` |
| Database writers | 4 workers; 10,000-row batches | Live Function configuration and terminal manifest |
| MySQL shape | MySQL.8; 50 GB, one LUN, approximately 3,750 maximum IOPS | Operator-confirmed environment; the test VM identity could not list DB systems |
| Target | `fntestdb.perf_t_001` | Pre-existing LIST-partitioned table with invisible `batch_num` |
| Control schema | `fndb5` | Mapping, object-event, transaction, error, and batch tables |

## 2. Test method

1. Created a fresh mutually exclusive prefix and aligned the OCI Events rule with the resource mapping.
2. Removed only the dedicated target's two orphan staging tables, recreated the target with its seed partition, and reset mapping-1 batch state.
3. Generated each CSV to at least the nominal binary size using valid columns: `record_id,event_ts,category,payload,amount`.
4. Uploaded one object at a time. The mapping used `SYNC` through 500 MB, then changed dynamically to `DETACHED` for 1 GB, 2 GB, and 5 GB.
5. Waited for a terminal database audit record, verified the exact target row count and absence of staging residue, deleted the object, and waited for successful partition cleanup before advancing.

Function duration below is `object_event.completed_at − object_event.received_at`. Delivery latency is `received_at − event_time`. End-to-end audit latency is `completed_at − event_time`. Upload time is measured independently around the OCI CLI upload.

## 3. Measured create/import results

| Shape | Workers | Case | Mode | Actual bytes | Rows | Generate | Upload | Event delivery | Function / audit duration | End-to-end audit | MiB/s | Rows/s | Seconds / 100 rows | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MySQL.8 | 4 | 10 KB | SYNC | 10,249 | 162 | 0.002 s | 0.782 s | 72.330 s | 0.374 s | 72.704 s | 0.026 | 433 | 0.231137 | SUCCESS |
| MySQL.8 | 4 | 100 KB | SYNC | 102,403 | 1,615 | 0.003 s | 0.797 s | 24.488 s | 0.520 s | 25.008 s | 0.188 | 3,104 | 0.032219 | SUCCESS |
| MySQL.8 | 4 | 1 MB | SYNC | 1,054,003 | 16,384 | 0.016 s | 0.713 s | 7.675 s | 0.653 s | 8.328 s | 1.540 | 25,105 | 0.003983 | SUCCESS |
| MySQL.8 | 4 | 5 MB | SYNC | 5,313,843 | 81,920 | 0.091 s | 0.737 s | 7.341 s | 1.802 s | 9.142 s | 2.812 | 45,462 | 0.002200 | SUCCESS |
| MySQL.8 | 4 | 10 MB | SYNC | 10,537,228 | 162,449 | 0.189 s | 0.999 s | 12.854 s | 2.830 s | 15.684 s | 3.551 | 57,405 | 0.001742 | SUCCESS |
| MySQL.8 | 4 | 100 MB | SYNC | 104,862,793 | 1,616,150 | 1.769 s | 2.144 s | 14.768 s | 23.700 s | 38.467 s | 4.220 | 68,193 | 0.001466 | SUCCESS |
| MySQL.8 | 4 | 500 MB | SYNC | 524,357,793 | 8,080,750 | 13.506 s | 5.874 s | 10.272 s | 117.454 s | 127.727 s | 4.258 | 68,799 | 0.001454 | SUCCESS |
| MySQL.8 | 4 | 1 GB | DETACHED | 1,073,778,638 | 16,547,763 | 27.989 s | 10.336 s | 7.258 s | 254.660 s | 261.918 s | 4.021 | 64,980 | 0.001539 | SUCCESS |
| MySQL.8 | 4 | 2 GB | DETACHED | 2,147,568,233 | 33,095,526 | 49.205 s | 19.646 s | 9.244 s | 547.418 s | 556.663 s | 3.741 | 60,457 | 0.001654 | SUCCESS |
| MySQL.8 | 4 | 5 GB | DETACHED | 5,368,734,188 | 82,736,033 | 112.542 s | 46.277 s | 6.152 s | 1,408.743 s | 1,414.896 s | 3.634 | 58,730 | 0.001703 | SUCCESS |

## 4. Cleanup results

| Case | Mode at execution | Delete Function / audit duration | Outcome |
| --- | --- | --- | --- |
| 10 KB | SYNC | 0.182 s | Partition truncated |
| 100 KB | SYNC | 0.142 s | Partition truncated |
| 1 MB | SYNC | 0.172 s | Partition truncated |
| 5 MB | SYNC | 0.173 s | Partition truncated |
| 10 MB | SYNC | 0.173 s | Partition truncated |
| 100 MB | SYNC | 0.207 s | Partition truncated |
| 500 MB | SYNC | 0.171 s | Partition truncated |
| 1 GB | DETACHED | 2.224 s | Partition truncated |
| 2 GB | DETACHED | 1.657 s | Partition truncated |
| 5 GB | DETACHED | 39.164 s | Partition truncated |

**Final state:** target rows = 0; staging tables = 0; active batches = 0; live objects under the run prefix = 0. Historical audit records were retained.

## 5. Findings and operating guidance

- **Use 500 MB as the demonstrated synchronous ceiling for this configuration.** It completed in 117.454 seconds, leaving 182.546 seconds of the 300-second Function limit. This is measured headroom, not a guarantee under concurrent or degraded I/O conditions.
- **The 1 GB case belongs in detached mode operationally.** It completed in 254.660 seconds in this run, leaving only 45.340 seconds against the synchronous limit. Prior 1 GB synchronous testing exceeded 300 seconds, demonstrating sensitivity to row shape and database load.
- **Detached mode materially extends the envelope.** The 2 GB and 5 GB cases completed in 547.418 and 1,408.743 seconds, respectively, below the 3,600-second detached timeout.
- **Bulk throughput declined as size increased:** 4.258 MiB/s at 500 MB, 4.021 MiB/s at 1 GB, 3.741 MiB/s at 2 GB, and 3.634 MiB/s at 5 GB. This approximately 14.7% decline from 500 MB to 5 GB is consistent with increasing sustained database/I/O pressure.
- **Small files are dominated by event delivery and fixed setup cost.** The first 10 KB event waited 72.330 seconds before receipt, while its Function work required only 0.374 seconds.
- **Streaming avoided Function filesystem capacity failures.** The Function read 32 MiB ranges and sent parsed batches to four database writers; the test VM's local file existed only as an upload source and was deleted immediately after upload.
- **Partition exchange and explicit delete cleanup worked.** Every successful load had one active batch and no staging residue; every delete retired its partition.

**Capacity warning:** linear projection from one sequential sample is not a service limit. Do not infer that objects above 5 GB will complete before 3,600 seconds. Split large sources into ordered, disjoint files when practical, and test concurrency, p95 latency, retries, and degraded storage conditions before setting a production threshold.

## 6. Audit and observability finding

The current transaction query derives execution mode and worker count by joining to the mapping's *current* values. After mapping 1 changed from `SYNC` to `DETACHED`, a later query displayed earlier synchronous transactions as detached. The run manifest captured mode and workers at each terminal event and is therefore the authoritative source for this report.

**Recommended correction:** persist `invocation_mode`, `worker_threads`, Function image/version, Function memory, synchronous timeout, and detached timeout directly on each object-event or transaction row at execution time. UI tables and exports should read the immutable snapshot, not the mutable mapping.

## 7. Limitations

- One measured run per size; median, p95, p99, variance, and cold/warm separation are not available.
- Tests were sequential and isolated. They do not measure concurrent event delivery, connection pressure, or analytics running during ingestion.
- CSV rows used a deterministic narrow payload. Wider payloads, complex quoting, multibyte data, and conversion-heavy schemas may reduce throughput.
- The MySQL.8 shape and 50 GB single-LUN/approximately 3,750-IOPS configuration are operator-confirmed. The VM instance principal received `NotAuthorizedOrNotFound` when listing MySQL DB systems, so the shape could not be independently captured through OCI during this run.
- Object upload duration is separate from Object Storage event time. End-to-end audit latency starts at the event timestamp, not at local file generation.
- Bucket version history may retain deleted object versions even though the live object listing for the test prefix is empty.
Generated from the terminal result manifest and control-schema audit records collected on 2026-07-19. Credentials, tokens, private endpoints, and full OCIDs are intentionally excluded.
