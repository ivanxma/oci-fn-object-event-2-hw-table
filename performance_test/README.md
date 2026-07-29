# Streaming performance fixtures

`generate_csv.py` creates deterministic CSV inputs for throughput experiments.
The retired invocation-mode harness is intentionally absent: it depended on
tables and settings that the Container Instance processor no longer creates.

Use the supported deployment verification before measuring performance:

```bash
cd /home/opc/oci-object-event-2-table
./deploy/verify_streaming_deployment.sh
./deploy/verify_durable_capture.sh
./deploy/verify_parallel_flow.sh
```

Generate a larger input with Python 3.13 or later:

```bash
python3.13 performance_test/generate_csv.py \
  --rows 100000 \
  --payload-bytes 128 \
  --output /tmp/perf-100k.csv
```

For repeatable measurements, create a dedicated mapping and target table, use
unique object prefixes per run, and query `stream_message_capture` plus
`stream_event_tx_log` for captured/completed timestamps. Measure FIFO and
Parallel independently; Parallel requires at least two Stream partitions and
does not guarantee global event order.
