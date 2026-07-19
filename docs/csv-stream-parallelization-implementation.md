# Parallel CSV streaming without temporary files

Implementation report for the OCI Object Storage → OCI Function → MySQL HeatWave partition-loader path. The design imports large CSV objects without copying them to `/tmp`, while database writes proceed concurrently with CSV parsing.

**Implemented outcome.** The Function uses one sequential CSV reader, a bounded queue of row batches, and multiple MySQL writer workers. Object Storage is read through 32 MiB HTTP Range requests, so a single response cannot expire during a long import. The 500 MB validation run loaded 4,536,966 rows successfully and used no temporary CSV file.

## 1. Why a single download fails for large objects

A long-lived Object Storage response is not a durable local file. The loader must also pause while MySQL commits batches. In the measured environment, a single 500 MB response closed at approximately 120 seconds and surfaced as `I/O operation on closed file`. Writing the object first avoids that lifetime, but consumes Function filesystem space and adds a second full I/O pass.

The implementation therefore uses HTTP byte ranges. It performs a small `HEAD` request to obtain `Content-Length`, then opens independent `GET` requests such as `bytes=0-33554431`, `bytes=33554432-67108863`, and so on. Only the active range and the CSV decoder buffer are resident in memory.

## 2. End-to-end flow

- **1. Event Rule** — Object Storage create/update event invokes the Function.
- **2. Control lease** — Resolve mapping and allocate or recover one target batch partition.
- **3. Range reader** — HEAD object, then fetch bounded byte ranges sequentially.
- **4. CSV reader** — Decode rows and validate headers without creating a file.
- **5. Writer pool** — Insert independent row batches into one staging table.
- **6. One switch** — Validate and exchange the staging table with the target partition.

## 3. Implementation components

| Component | Responsibility | Important property |
| --- | --- | --- |
| `ObjectStorageRangeStream` | Implements a readable, non-seekable stream over sequential Object Storage ranges. | At most one range response is open; range size is configurable. |
| `io.BufferedReader` + `io.TextIOWrapper` | Coalesces byte reads and decodes UTF-8 CSV text for `csv.DictReader`. | Decoder state is preserved across range boundaries, including a quoted field split between ranges. |
| `csv_batches` | Validates headers and yields lists of normalized row tuples. | It accepts either a local `Path` for offline tests or a live text stream in the Function. |
| `load_csv_parallel` | Submits row batches to a `ThreadPoolExecutor`. | The reader remains single and ordered; MySQL inserts are parallel. |
| `insert_batch` | Opens a Connector/Python connection and runs one `executemany` transaction against the staging table. | Workers do not share MySQL connections or cursors. |
| `validate_and_exchange` | Checks the invisible `batch_num`, then exchanges the staging table into the target partition. | Users see either the old partition or the complete new batch. |

## 4. Bounded producer/consumer behavior

The main Function thread is the producer. It reads and parses CSV rows, creates a batch of `BATCH_ROWS`, and submits that batch to a worker. The producer never submits without limit: when pending work reaches `WRITER_WORKERS × 2`, it waits for at least one completed future before reading more.

```
pending = set()
for rows in csv_batches(stream, columns, batch_rows):
    pending.add(executor.submit(insert_batch, ..., rows))
    if len(pending) >= workers * 2:
        done, pending = wait(pending, FIRST_COMPLETED)
        inserted += sum(job.result() for job in done)

for job in pending:
    inserted += job.result()
```

This is deliberate back-pressure. It prevents the parser from materializing the entire object as Python lists and bounds memory approximately to the active range, CSV decoder buffer, and at most eight row batches with the default four workers.

## 5. Why the reader is not split across CSV workers

Splitting a CSV at arbitrary byte offsets is unsafe: a range can begin inside a quoted field or a multi-byte UTF-8 character. The safe design has one ordered decoder and parallelizes the expensive database operation. HTTP ranges solve response lifetime, not CSV record framing; the text decoder is intentionally kept above the range layer.

If independent CSV readers are ever required, a separate indexing phase must find record-safe newline boundaries while honoring CSV quoting. That would add complexity and another pass, so it is not used by this implementation.

## 6. Transaction and failure semantics

1. Each worker commits only to the unique staging table for the leased batch.
2. A worker exception propagates through `Future.result()`; the loader marks the source batch `ERROR`.
3. The Function drops the staging table in `finally`, while the UI can expose any residual table for manual cleanup.
4. Only after every worker succeeds does the Function validate and perform one partition exchange.
5. Successful completion marks the source batch `ACTIVE` and writes one success transaction event.

**Important boundary.** Parallel inserts are not one transaction across workers. Atomicity is provided by the final partition exchange. A failed load never exchanges a partial staging table into the target.

## 7. Configuration and tuning

| Setting | Current default | Effect |
| --- | --- | --- |
| `OBJECT_STORAGE_RANGE_BYTES` | 33,554,432 (32 MiB) | Maximum bytes per HTTP Range response. Smaller values reduce response lifetime and memory; larger values reduce request count. |
| `BATCH_ROWS` | 10,000 | Rows per `executemany` call. Larger values reduce commit overhead but increase per-batch memory and rollback work. |
| `WRITER_WORKERS` | 4 | Concurrent MySQL connections. Increase only when HeatWave/MySQL CPU, connection limits, and staging-table write throughput support it. |
| `OBJECT_STORAGE_READ_TIMEOUT_SECONDS` | 300 | Per-range SDK read timeout. It does not replace Range reads; it protects an individual range request. |
| `FUNCTION_MEMORY` | 2,048 MB in the test deployment | Controls decoder, bounded buffers, worker overhead, and SDK memory headroom. No 500 MB file-sized disk allocation is required. |

## 8. Observability and validation

- Record the Object Storage upload start and completion timestamps.
- Record Event Rule delivery and Function start/end timestamps from invocation logs.
- Use `event_tx_log` for action, status, batch number, row count, and error message.
- Use `source_object_batches` to confirm `LOADING → ACTIVE` or `LOADING → ERROR`.
- Check the target partition row count after exchange and verify no unexpected staging tables remain.

Reference implementation: `function/func.py` (`ObjectStorageRangeStream`) and `function/partition_loader.py` (`csv_batches`, `load_csv_parallel`, `insert_batch`).

Generated for the OCI Object Storage event-to-HeatWave implementation. The report describes the deployed diskless range-stream and bounded parallel-writer design.
