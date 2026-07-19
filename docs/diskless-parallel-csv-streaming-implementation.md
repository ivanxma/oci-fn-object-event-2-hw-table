# Diskless Parallel CSV Streaming for Large Object Storage Imports

Implementation notes for the OCI Function that streams a CSV from Object Storage into MySQL/HeatWave staging tables, parallelizes inserts, and publishes with a partition exchange—without creating a temporary CSV file.

**Verified outcome.** A 500 MB-class update loaded 4,536,966 rows and completed the partition exchange. Function execution was 123.349 seconds; no CSV file was created on the Function filesystem.

## Problem addressed

MySQL `LOAD DATA` cannot directly consume an Object Storage object through this application integration. Downloading the whole object to `/tmp` caused `[Errno 28] No space left on device`. A single, long-lived Object Storage HTTP response also closed at about 120 seconds during slow imports. The implementation removes both dependencies.

## Implementation flow

- **1. Event** — OCI Events invokes the Function.
- **2. Range stream** — HEAD obtains length; GET reads 32 MiB ranges.
- **3. Parser** — One CSV reader validates headers and emits row batches.
- **4. Writers** — Workers insert batches concurrently into staging.
- **5. Publish** — Validate, exchange partition, mark batch active.

## 1. Diskless Object Storage reader

`ObjectStorageRangeStream` implements `io.RawIOBase`. It uses `head_object` for content length and performs sequential `get_object(..., range="bytes=start-end")` calls. `io.BufferedReader` and `io.TextIOWrapper` turn this into one continuous text stream for `csv.DictReader`.

```
HEAD object → Content-Length
for each 32 MiB segment:
    GET object, Range: bytes=start-end
    read bytes into parser buffer
    close this HTTP response

TextIOWrapper(BufferedReader(ObjectStorageRangeStream))
    → csv.DictReader(...)
```

The default `OBJECT_STORAGE_RANGE_BYTES` is `33,554,432` (32 MiB). A range boundary can fall inside a line, quoted field, or UTF-8 character: the buffered text and CSV readers preserve that state. The object is never materialized on disk or as one in-memory byte array.

## 2. Parallelization model

### One parser

CSV parsing remains single-threaded because CSV has state across quoted fields and read boundaries. It validates the header once and produces normalized row tuples.

### Multiple writers

Each writer owns a separate Connector/Python connection. It runs `executemany` for one batch against the per-object staging table. Every row receives the same invisible `batch_num`.

```
with ThreadPoolExecutor(max_workers=WRITER_WORKERS) as executor:
    for rows in csv_batches(csv_stream, columns, BATCH_ROWS):
        pending.add(executor.submit(insert_batch, ..., rows))
        if len(pending) >= WRITER_WORKERS * 2:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            inserted += sum(job.result() for job in done)
    inserted += sum(job.result() for job in pending)
```

## 3. Bounded memory and back-pressure

The parser does not queue the full file. It pauses when `WRITER_WORKERS × 2` batches are pending, then resumes when a writer completes. With four workers and 10,000 rows per batch, no more than eight batches are queued, in addition to normal parser and range buffers.

| Setting | Default | Purpose |
| --- | --- | --- |
| `BATCH_ROWS` | 10,000 | Reduces database commit overhead while retaining bounded row memory. |
| `WRITER_WORKERS` | 4 | Concurrent Connector/Python insert operations. |
| `OBJECT_STORAGE_RANGE_BYTES` | 32 MiB | Maximum bytes for a single HTTP response. |
| `FUNCTION_MEMORY` | 2,048 MB deployed | Capacity for runtime, queued rows, and worker activity—not a disk cache. |

For wide rows, reduce `BATCH_ROWS` before increasing worker count. More workers improve concurrent database writes; they do not make the stateful CSV parser parallel.

## 4. Atomic publication and recovery

Workers write only to a new staging table. Once all futures complete, the Function validates the staged batch number and executes `ALTER TABLE target EXCHANGE PARTITION ... WITH TABLE stage`. The finished file becomes visible in one switch; partial data is never published.

- Successful exchange marks the source batch `ACTIVE`.
- A failure marks it `ERROR`, allowing an update event to retry the same batch.
- Stage-table cleanup is attempted on both success and failure; the UI exposes any exceptional residue for operator cleanup.
- Raw events, source batches, transaction records, and errors remain in the control schema for investigation.

## Operational settings and evidence

- Keep `OBJECT_STORAGE_READ_TIMEOUT_SECONDS` no greater than the Function timeout.
- Use a Function timeout above observed parsing, writing, validation, and exchange time.
- Measure upload completion, invocation start, Function duration, and transaction completion separately; Event Rule delivery latency is not CSV import time.
- The verified 500 MB run used sixteen 32 MiB Object Storage HTTP 206 responses and loaded 4,536,966 rows successfully.

| Object size | Rows | Function duration | Result |
| --- | --- | --- | --- |
| 524,288,100 bytes | 4,536,966 | 123.349 seconds | SUCCESS; partition exchange completed |
