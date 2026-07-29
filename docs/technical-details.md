# Streaming processor deployment and operations

## Architecture decision

The active event path is:

```text
Object Storage -> OCI Events rule -> OCI Stream -> Container Instance processor
               -> durable capture/transaction log -> CSV loader -> MySQL target
```

The UI calls a Stream a **Stream Server**. The OCI resource selected by a
mapping and targeted by its Events rule is an OCI **Stream**, not a host
managed by this application. A Stream is an append-only, retained message log
split into one or more partitions. The Stream supplies the durable boundary
between event production and database loading; the processor can be stopped,
replaced, or restarted without requiring Object Storage to reproduce events
that remain within Stream retention.

OCI Events supports both Streaming and Functions actions. This implementation
deliberately uses a Streaming action and does not deploy an OCI Function:

- Direct, independent Function invocations do not provide this application's
  required retained log, partition offset, replay, or one-partition ordering
  boundary. OCI Streaming provides those capabilities directly.
- CSV ingestion can be long-running and has variable Object Storage and MySQL
  latency. A continuously running Container Instance has explicit CPU/memory,
  network placement, polling, retry, and lifecycle control instead of a
  per-invocation execution window.
- The processor must persist the raw event before loading, maintain a cursor,
  recover interrupted attempts, and expose retry/archive state. Keeping these
  responsibilities in one long-running processor avoids recreating a queue and
  checkpoint protocol around Function invocations.
- FIFO and parallel behavior are deployment properties tied to Stream
  partitions. They are not Function `Sync` or `Detached` invocation modes.
  Function execution-mode fields and Function deployment are therefore absent
  from the active UI and runtime contract.

This is an architecture choice, not a statement that OCI Functions cannot
receive OCI Events. A Function design would require an additional durable,
ordered broker plus idempotency/checkpoint handling and would still need to
address execution-time and concurrency behavior. That would duplicate the
Streaming processor implemented here.

## Stream Server and ordering modes

Create or select the Stream in the same compartment used by the mapping:

1. In **Streaming > Stream Server**, select the compartment and create or
   select an ACTIVE Stream.
2. For **FIFO**, use exactly one Stream partition.
3. For **Parallel**, use at least two Stream partitions and assign every
   partition to no more than one active processor.
4. In **Resource Mapping**, select that Stream and the corresponding mode.
5. Create or update the OCI Events rule with a **Streaming** action whose Stream
   OCID is exactly the mapping's Stream OCID.
6. In **Event Processor**, deploy the image with the mapping, database-secret
   OCID, mode, and explicit partition assignment.

OCI Streaming guarantees ordering within a partition and delivers messages at
least once. It does not define a global order across several partitions.

| Mode | Stream | Processor deployment | Ordering |
|---|---|---|---|
| FIFO | Exactly 1 partition | Exactly 1 replica, `PROCESSOR_PARTITIONS=0` | Stream delivery and normal-path processing follow partition offset order. |
| Parallel | At least 2 partitions | 1..N replicas with unique, disjoint assignments | Ordered only within each partition; completion order across partitions is undefined. |

“FIFO” must not be interpreted as exactly-once delivery. The durable
`(stream_id, partition_id, stream_offset)` key makes repeated delivery
idempotent at capture, and source-object batch state makes repeated loader
operations safe where possible.

There is also a current strict-order limitation: a failed capture receives a
future retry time, and the scheduler can claim a later eligible capture while
that retry is delayed. Consequently, FIFO mode preserves one-partition ingress
order and serialized normal-path execution, but does not guarantee database
commit order across failures. For a workflow that requires strict
head-of-line FIFO, stop the processor when a failure is observed, resolve and
retry the earliest failed offset, verify it is `COMPLETED`, and only then
resume. Production automation should treat enforced head-of-line blocking as a
required enhancement before claiming strict FIFO under failure.

## Runtime flow

1. OCI Events matches an Object Storage create, update, or delete event.
2. The rule publishes the event to the mapping's OCI Stream.
3. An OCI Container Instance processor reads only its explicit Stream
   partition assignment.
4. The processor stores the decoded payload in `stream_message_capture` before
   advancing the cursor.
5. It records the attempt in `stream_event_tx_log`, resolves the mapping, and
   invokes the CSV loader.
6. Create/update streams the object into a staging table and atomically
   exchanges the file-owned target partition.
7. Delete drops the matching file-owned partition and marks
   `source_object_batches.lifecycle_state` as `DELETED`.

FIFO requires one Stream partition, one processor replica, and partition
assignment `0`. Parallel requires at least two Stream partitions. Each
processor must receive a unique, disjoint `PROCESSOR_PARTITIONS` assignment;
global FIFO is not guaranteed in Parallel mode. In either mode, a message is
not exactly-once: durable capture and idempotent loader state handle replay.

## Database layout

- The JSON Vault secret supplies `host`, `port`, `user`, `credential`, and
  `database`.
- `control_database` optionally selects the bounded mapping/batch control
  schema.
- `stream_data_database` optionally selects the growing durable capture,
  checkpoint, transaction, retry, and archive schema.
- Target tables live in mapping-selected schemas.
- Target tables use LIST partitioning on the invisible `batch_num` column.
  Every unique key must contain `batch_num`.

The processor reads the external SQL files under `processor/sql/` to initialize
durable objects. The UI reads external SQL under `ui/myapp/sql/` for mapping
objects and removal of obsolete audit objects.

## Partition-exchange loading

Each active source object owns one LIST partition named `p_batch_<batch_num>`
in its mapped target table. The target retains `p_seed` for batch value `0`.
For create and update:

1. The processor reserves or reuses the source object's `batch_num`.
2. It ensures the file-owned target partition exists.
3. It creates a nonpartitioned staging table with `CREATE TABLE ... LIKE` and
   removes the copied partitioning definition.
4. Multiple writer threads load the CSV into the staging table while assigning
   the same `batch_num` to every row.
5. It verifies that the stage contains no other batch value.
6. It publishes the entire file with
   `ALTER TABLE ... EXCHANGE PARTITION ... WITHOUT VALIDATION`.
7. It drops the table holding the previous partition contents.

This design keeps partial CSV loads out of the visible target and makes the
final publication a metadata-oriented partition exchange rather than a
row-by-row replacement. `WITHOUT VALIDATION` avoids MySQL's row-by-row
partition-boundary check; the application assumes responsibility by validating
the single `batch_num` before exchange.

The performance trade-off imposes these schema restrictions:

- The target must be InnoDB and LIST-partitioned by the invisible unsigned
  `batch_num` column.
- Every primary or unique key must include `batch_num`.
- The staging and target definitions, character set, collation, and row format
  must remain exchange-compatible.
- Do not apply an incompatible target DDL change while a load is active.
- A source delete drops its file-owned partition and therefore removes that
  file's target rows.

### Partition capacity

MySQL permits at most **8,192 total partitions per non-NDB table**, including
subpartitions. This implementation always retains `p_seed`, so an otherwise
unmodified target has at most:

```text
8,192 total slots - 1 p_seed slot = 8,191 active file-owned partitions
```

Therefore, 8,192 is the MySQL table limit and 8,191 is this loader's maximum
simultaneously active one-file/one-partition capacity. It is not a lifetime
import limit: a processed Object Storage delete drops the corresponding
partition and releases one slot. Any manually created partition or
subpartition reduces the available file-owned capacity. Each target table has
its own independent MySQL limit.

Do not operate near the hard ceiling. MySQL notes that even hundreds of
partitions can be inadvisable for some workloads. Establish a lower operational
warning threshold and monitor both the physical partition count and active
source ownership:

```sql
SELECT
    COUNT(*) AS physical_partitions,
    8192 - COUNT(*) AS remaining_mysql_slots
FROM information_schema.partitions
WHERE table_schema = '<target_database>'
  AND table_name = '<target_table>'
  AND partition_name IS NOT NULL;

SELECT lifecycle_state, COUNT(*) AS source_objects
FROM <control_database>.source_object_batches
WHERE target_database = '<target_database>'
  AND target_table = '<target_table>'
GROUP BY lifecycle_state;
```

### Reducing the partition count safely

A database administrator can manually reorganize several immutable
`p_batch_*` LIST partitions into one archive partition containing all their
batch values, or copy historical rows to a separate archive table and then
drop their original partitions. Consolidation frees physical partition slots,
but it deliberately breaks the normal one-file/one-partition management
boundary:

- A later update tries to create or exchange `p_batch_<batch_num>`, which is no
  longer the owning partition and can conflict with a batch value held by the
  merged partition.
- A later delete tries to drop `p_batch_<batch_num>`, which no longer exists.
- The corresponding `source_object_batches` records do not automatically
  become archive metadata.

Before deleting, relocating, or consolidating matched source objects:

1. Disable the mapping's OCI Events rule. Otherwise Object Storage deletions
   are published to the Stream and intentionally remove target data.
2. Stop or pause the assigned processor after all already-published messages
   have drained. Confirm no relevant capture is `CAPTURED`, `PROCESSING`, or
   retryable `FAILED`.
3. Back up or archive the target data and record the affected mapping IDs,
   object names, batch numbers, partitions, and row counts.
4. Perform and validate the manual partition reorganization or archive-table
   move.
5. Exclude the consolidated objects from the rule/mapping permanently before
   re-enabling processing. Do not allow later create, update, or delete events
   for those objects to enter the standard loader.

If files must remain individually mutable, use multiple mapped target tables
instead of merging their partitions.

## Processor deployment contract

Required OCI and image values are defined in the ignored `deploy/env.sh`:

| Area | Variables |
|---|---|
| OCI | `COMPARTMENT_ID`, `REGION`, `REGION_KEY`, `SUBNET_ID`, `CONTAINER_AVAILABILITY_DOMAIN` |
| Shape | `PROCESSOR_SHAPE`, `PROCESSOR_OCPUS`, `PROCESSOR_MEMORY_GBS` |
| Image | `REPOSITORY_PREFIX`, `PROCESSOR_IMAGE_NAME`, `PROCESSOR_IMAGE_TAG`, `PROCESSOR_IMAGE_URL` |
| Mapping | `PROCESSOR_MAPPING_ID`, `OCI_STREAM_ID`, `PROCESSING_MODE` |
| Assignment | `EXPECTED_PARTITION_COUNT`, `PROCESSOR_REPLICA_COUNT`, `PROCESSOR_PARTITIONS` |
| Database | `DB_SECRET_OCID` |
| Loader | `WRITER_WORKERS`, `BATCH_ROWS`, `OBJECT_STORAGE_RANGE_BYTES`, `OBJECT_STORAGE_READ_TIMEOUT_SECONDS` |

Only these seven values are passed to a processor container:

```text
OCI_STREAM_ID
PROCESSING_MODE
EXPECTED_PARTITION_COUNT
PROCESSOR_REPLICA_COUNT
PROCESSOR_PARTITIONS
DB_SECRET_OCID
WRITER_WORKERS
```

The Vault secret must be a complete JSON bundle. Password-only secrets and raw
database connection environment variables are not supported by the processor.
OCI Container Instances use resource-principal authentication. The build VM
pushes to OCIR using its instance-principal credential helper; no registry
authentication token is used.

## IAM baseline

Replace placeholders with your own generic dynamic group and compartment:

```text
Allow dynamic-group <dynamic-group> to manage compute-container-family in compartment <compartment>
Allow dynamic-group <dynamic-group> to use virtual-network-family in compartment <compartment>
Allow dynamic-group <dynamic-group> to manage streams in compartment <compartment>
Allow dynamic-group <dynamic-group> to manage cloudevents-rules in compartment <compartment>
Allow dynamic-group <dynamic-group> to manage repos in compartment <compartment>
Allow dynamic-group <dynamic-group> to read vaults in compartment <compartment>
Allow dynamic-group <dynamic-group> to read secrets in compartment <compartment>
Allow dynamic-group <dynamic-group> to read secret-bundles in compartment <compartment>
Allow dynamic-group <dynamic-group> to manage secrets in compartment <compartment>
Allow dynamic-group <dynamic-group> to use secret-family in compartment <compartment>
Allow dynamic-group <dynamic-group> to read keys in compartment <compartment>
Allow dynamic-group <dynamic-group> to use keys in compartment <compartment>
Allow dynamic-group <dynamic-group> to manage objects-family in compartment <compartment>
Allow dynamic-group <dynamic-group> to read objectstorage-namespaces in compartment <compartment>
```

Apply only the required subset to each principal. The processor principal needs
Stream consumption, source-object reads, and Vault bundle reads. The UI/build
VM principal additionally needs Container Instance, Event Rule, Stream
administration, repository, Vault metadata/secret creation, key use, and
test-object permissions.

## Deployment

On the Oracle Linux validation VM:

```bash
cd /home/opc/oci-object-event-2-table
./deploy/bootstrap_streaming.sh
./deploy/build_processor_image.sh
./deploy/verify_streaming_deployment.sh
./deploy/verify_durable_capture.sh
./deploy/deploy_ui.sh
./deploy/deploy_processor.sh
```

`deploy_ui.sh` generates a self-signed certificate only when
`GENERATE_SELF_SIGNED_CERT=true`; production should supply a trusted certificate.

## Verification gates

Before a production deployment:

1. Run processor, loader, and UI tests.
2. Run the redacted deployment preflight.
3. Run durable idempotency, retry, completion, and interrupted-processing
   recovery verification.
4. Run FIFO create/delete and confirm that delete removes the owned partition.
5. Run `deploy/verify_parallel_flow.sh`; it creates disposable two-partition
   resources, verifies 10 creates, deletes five, deletes the remaining five,
   checks durable/transaction state, and cleans its exact resources.
6. Authenticate over HTTPS and load Flow, Streaming, Resource Mapping, Event
   Processor, Durable Messages, and Event TX.
7. Confirm exactly the intended managed processors are ACTIVE and that the
   image/tag, mapping tag, partition assignments, and resource principal match.

The permanent seed partition is expected. An empty file-owned partition is
valid only for an active header-only object. A deleted object must have a
`DELETED` batch record and no corresponding target partition.

## Authoritative service references

- [OCI Events: add a Streaming or Functions action](https://docs.oracle.com/en-us/iaas/Content/Events/Task/create-action-events-rule.htm)
- [OCI Streaming overview, retention, replay, and ordering](https://docs.oracle.com/en-us/iaas/Content/Streaming/Concepts/streamingoverview.htm)
- [OCI Functions invocation modes and execution timeouts](https://docs.oracle.com/en-us/iaas/Content/Functions/Tasks/functionsinvokingfunctions.htm)
- [MySQL 9.7 partition exchange](https://dev.mysql.com/doc/refman/9.7/en/partitioning-management-exchange.html)
- [MySQL 9.7 partitioning restrictions and 8,192-partition limit](https://dev.mysql.com/doc/refman/9.7/en/partitioning-limitations.html)
