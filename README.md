# OCI Object Event to MySQL Table

This application turns OCI Object Storage CSV lifecycle events into controlled,
auditable MySQL table updates. OCI Events routes object create, update, and
delete events to OCI Streaming. A long-running OCI Container Instance processor
captures each stream message durably in MySQL before it invokes the existing
CSV loader. A mapping selects the target table, Stream, and FIFO or parallel
processing mode.

The Flask operations UI provides Stream Server and Stream Content pages,
Object Storage mapping maintenance, live OCI Events rule management, an Event
Processor page for the processor deployment contract, and a separate Durable
Messages page for capture, retry, archive, and retention operations.

## Application components

```mermaid
flowchart LR
    Operator[Operator] --> UI[Flask operations UI]
    UI --> Control[(MySQL control schema)]
    UI --> OCIAPI[OCI APIs]
    UI --> Target[(Mapped MySQL tables)]
    Publisher[CSV publisher] --> Bucket[OCI Object Storage]
    Bucket --> Events[OCI Events rule]
    Events --> Stream[OCI Streaming]
    Stream --> Processor[Container Instance processor]
    Processor --> Capture[(Durable message capture)]
    Capture --> Loader[Streaming CSV loader]
    Loader --> Bucket
    Loader --> Stage[(Parallel staging tables)]
    Stage -->|Atomic partition exchange| Target
    Loader --> Control
```

## What it does

- Maps a compartment, bucket, and object-name pattern to a pre-approved target
  table.
- Handles create/update by streaming bounded Object Storage ranges into
  parallel database writers without creating a full temporary CSV file.
- Publishes one file atomically with MySQL partition exchange. A matching
  delete event drops that file's owned target-table partition; it does not
  truncate and retain an empty partition.
- Chooses FIFO (one Stream partition/one processor) or parallel partition
  assignments from each mapping.
- Captures raw Stream events before processing, so failed records can be
  retried independently of Stream cursor retention.
- Provides operational UI workflows for Streams, mappings, live OCI Rules,
  processor orchestration, Object Storage testing, registered-table data, and
  captured-message retry.

## Deployment and configuration

The supported runtime is Python 3.13 or later. The UI uses Flask and Oracle
MySQL Connector/Python `>=9.7,<10`.

On an Oracle Linux deployment host configured with an OCI instance principal,
run production deployment scripts from `deploy/` and validation harnesses from
`tests/integration/`:

```sh
./deploy/bootstrap_streaming.sh
# Recommended on the UI/deployment VM: discover compartment/region and choose
# AD, VCN, subnet, Vault, enabled AES key, and database secret interactively.
./deploy/setup_env.sh
# Or copy env.sh.example and fill only its mandatory settings.
./deploy/build_processor_image.sh
./tests/integration/verify_streaming_deployment.sh
# Optional bounded integration check against the configured durable database.
# On OL9 it uses the project-local Python 3.12 verifier environment.
./tests/integration/verify_durable_capture.sh
# Disposable two-partition create/delete verification. It creates uniquely
# named OCI/DB resources and removes only those exact resources afterward.
./tests/integration/verify_fifo_flow.sh
./tests/integration/verify_parallel_flow.sh
./deploy/deploy_ui.sh
# Only after the documented full verification gate:
./deploy/deploy_processor.sh
```

`build_processor_image.sh` builds and pushes the non-root processor image using
the deployment host's instance-principal OCIR credential helper; it never uses
a static registry credential. `deploy_processor.sh` creates
one Container Instance for one explicit partition assignment. `deploy_ui.sh`
deploys the Flask container behind nginx HTTPS. Keep `deploy/env.sh`, database
credentials, Vault values, TLS private keys, and Flask secrets out of Git.
The selected Vault secret must be a JSON object containing `host`, `port`,
`user`, `credential`, and `database`; optional `control_database` and
`stream_data_database` fields keep control and growing durable data separate.
Plain-text password-only Vault secrets are not accepted.
The selected Vault and key become the default choices in the Database Secret
tab; they are OCIDs, not secret material.
All `stream_db` and `stream_data` initialization DDL is stored under
`loader_core/sql/`, `processor/sql/`, or `ui/myapp/sql/`; see the external
database schema inventory in `docs/technical-details.md`.
Unit tests, integration harnesses, and disposable SQL fixtures live under
`tests/`; production processor images do not copy them.

For a fresh OL9 validation VM, create a mode-`0600` config containing only the
existing Vault secret OCID, processor subnet OCID, and isolated control/durable
database names, then run:

```sh
./deploy/install_validation_vm.sh --config /path/to/validation-install.env
```

This non-interactive path bootstraps packages, derives instance/region/AD/VCN
and Vault/key metadata, generates ignored `env.sh`, builds and pushes the
processor with instance-principal authentication, initializes the external SQL
schemas, runs preflight/durable verification, and deploys the HTTPS UI.

Before use, confirm:

- Object events are enabled on each source bucket.
- The Events rule covers create, update, and delete and its bucket/object filter
  matches exactly one mapping.
- The processor resource principal can read source objects, consume the Stream,
  and read the DB secret from Vault.
- The deployment/UI instance principal has scoped Streaming, Events,
  Container Instance, repository, and test-object permissions.
- The processor subnet can reach MySQL and the database account can use the
  control schema plus approved target/staging objects.

### IAM policy baseline

Use the dynamic group that contains the UI/deployment VM instance principal.
The Container Instance policy resource family is `compute-container-family`
(not `container-instances` or `container-instances-family`). Replace
`<dynamic-group>`, `<compartment>`, and `<tag-namespace>` below with your own
OCI names. A deployment requires a policy bundle; it is not sufficient to
grant only Container Instance access:

```text
Allow dynamic-group <dynamic-group> to manage compute-container-family in compartment <compartment>
Allow dynamic-group <dynamic-group> to use virtual-network-family in compartment <compartment>
Allow dynamic-group <dynamic-group> to manage streams in compartment <compartment>
Allow dynamic-group <dynamic-group> to read secrets in compartment <compartment>
Allow dynamic-group <dynamic-group> to read secret-bundles in compartment <compartment>
Allow dynamic-group <dynamic-group> to manage repos in compartment <compartment>
```

`compute-container-family` permits Event Processor list/create/manage
operations. `virtual-network-family` permits VNIC/subnet attachment. Streams
cover the UI Stream Server/Content operations and processor assignment. `read
secrets` lists metadata for the UI selector; `read secret-bundles` retrieves
the selected value at runtime. Repositories permit the approved OCIR image
workflow through the instance principal.

To enable the Event Processor **Database Secret** tab to create a new JSON
database-connectivity secret, grant the UI/deployment principal the following
additional least-privilege permissions in the compartment containing the
selected Vault and key:

```
Allow dynamic-group <dynamic-group> to manage secrets in compartment <compartment>
Allow dynamic-group <dynamic-group> to use secret-family in compartment <compartment>
Allow dynamic-group <dynamic-group> to read vaults in compartment <compartment>
Allow dynamic-group <dynamic-group> to read keys in compartment <compartment>
Allow dynamic-group <dynamic-group> to use keys in compartment <compartment>
```

The create form requires an existing Vault OCID and a symmetric encryption-key
OCID from that Vault. It sends the submitted connection JSON directly to OCI
Vault and returns only the resulting secret OCID; it does not store or display
the credential. `use secret-family` is required for the `CreateSecret`
operation in addition to `manage secrets`. The processor resource principal still needs only `read
secret-bundles` to resolve that OCID at runtime.

If the deployment applies **defined tags** (freeform tags do not need this),
also grant use of the approved tag namespace at tenancy scope:

```text
Allow dynamic-group <dynamic-group> to use tag-namespaces in tenancy
```

Apply least privilege to the actual processor resource-principal dynamic group
as well. It must be able to consume the assigned Stream, read the Vault bundle,
read source Object Storage objects, and reach MySQL. Verify each permission
with read-only preflight checks before enabling Container Instance creation.

See [Deployment, configuration, IAM, and implementation details](docs/technical-details.md)
for environment variables, policies, runtime flow, UI behavior, logging,
troubleshooting, and validation commands.

## Assumptions and limitations

- One CSV file is one complete logical partition of a mapped table. Many files
  may map to one table, but active files must not contain overlapping business
  records.
- FIFO requires one Stream partition and one processor; parallel mode allows
  independent partition processing and does not guarantee global FIFO order.
- Move records between files by completing removal from the source file before
  adding them to the destination, or use an external sequenced publication
  workflow.
- Target tables must already satisfy the loader contract: compatible columns,
  LIST partitioning by `batch_num`, and `batch_num` in every unique key.
- The seed partition remains by design. A header-only active CSV may also own
  an empty partition; empty partitions for deleted source objects indicate
  legacy or interrupted processing and can be reconciled against
  `source_object_batches`.
- MySQL limits a non-NDB table to 8,192 partitions in total, including
  subpartitions. Because this loader retains one seed partition and assigns one
  partition to each active source file, a target table can hold at most 8,191
  active file-owned partitions. This is a concurrent active-file limit, not a
  lifetime import count: a successfully deleted file has its partition dropped
  and releases that capacity. Set an operational alert below the hard limit,
  because very large partition counts can become impractical before 8,192.
  See the [MySQL 9.7 partitioning restrictions and
  limitations](https://dev.mysql.com/doc/refman/9.7/en/partitioning-limitations.html).
- Do not delete Object Storage files merely to reduce the partition count while
  their mapping rule is enabled. The resulting delete events are published to
  Streaming and intentionally drop the corresponding target-table partitions,
  removing those rows. First disable the mapping's OCI Events rule (or replace
  its condition so the archived objects cannot match), confirm that processing
  has drained, and then perform the approved archival or consolidation work.
- To retain data while reducing the count, an administrator can manually
  consolidate several immutable `p_batch_*` LIST partitions into one partition,
  or move historical rows to a dedicated archive table and then remove the
  original partitions. A merged partition no longer has the normal
  one-file/one-partition boundary: later create, update, or delete events for
  any merged source file are incompatible with the standard loader path.
  Keep those objects excluded from the rule before re-enabling it, record the
  consolidation in operational metadata, and test retry/delete behavior.
  Sharding independent datasets across multiple target tables is the safer
  option when source files must remain individually mutable.
- The processor is long-running; each Stream message is captured before loading
  so a failed loader invocation remains retryable after a processor restart.
- OCI Events is at-least-once and may retry or deliver conflicting operations
  out of order. Publishers must avoid simultaneous updates to the same logical
  data set.
- A timeout can leave a staging table behind. The UI exposes confirmed cleanup,
  while protecting a target that still has an active loading lease.
- More worker threads help only while MySQL CPU, connection capacity, storage
  throughput, and IOPS have headroom.

## More information

- [Technical deployment and operations guide](docs/technical-details.md)
