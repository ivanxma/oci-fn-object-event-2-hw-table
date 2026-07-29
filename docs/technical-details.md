# Streaming processor deployment and operations

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
global FIFO is not guaranteed in Parallel mode.

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
