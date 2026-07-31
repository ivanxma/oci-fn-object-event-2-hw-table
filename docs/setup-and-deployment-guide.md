# Setup and deployment guide

This guide describes what must exist before installation, what the deployment
scripts discover or create, how to publish and activate releases, and how to
connect the UI or the complete deployment to a different MySQL deployment.

## Deployment model

The deployment has two database connection planes. They must point to compatible
schemas when the UI and Processor operate as one deployment.

| Plane | Connection source | Credential lifetime | Used by |
|---|---|---|---|
| UI authenticated session | A UI Connection Profile plus the username and password entered at sign-in | Held only in server-side process memory for the session | Settings, Resource Mappings, Event TX, Durable Messages, import, and other UI database operations |
| Processor runtime | OCI Vault secret OCID passed as `DB_SECRET_OCID` | Secret bundle is read by the Container Instance resource principal at startup | Durable capture, checkpoints, transaction logging, Object Storage loading, staging, and target-table updates |

A Connection Profile does **not** modify a running Processor. Likewise, changing
the Processor Vault secret does not change the UI profile selected at sign-in.

The schemas have distinct roles:

| Schema | Default | Purpose |
|---|---|---|
| Control | `stream_db` | Resource mappings, source-object ownership, batch sequencing, and deployment history |
| Durable data | `stream_data` | Captured Stream messages, checkpoints, Event TX records, retries, and archives |
| Staging | `stream_staging` | Transient loader tables used before partition exchange; keeps internal working tables out of user databases |
| Target | `testdb` or the database selected by each mapping | User-visible mapped tables only; there is no global loader database |

## What must be prepared

### OCI resources

Prepare these resources in the deployment compartment before setup:

- An Oracle Linux 9 UI/deployment VM with an OCI instance principal. The VM can
  be in a public subnet when HTTPS access is required.
- A VCN containing a private subnet for Processor Container Instances. It must
  prohibit public IP assignment and have network reachability to MySQL, OCI
  Streaming, OCI Vault, OCIR, and Object Storage as required by the VCN design.
- An Object Storage bucket with object event emission enabled.
- One deployment-owned OCI Container Registry repository. Both UI and Processor
  images use this repository with separate immutable tags.
- An active OCI Vault and an enabled symmetric AES key.
- A dynamic group containing the UI/deployment VM. A separate dynamic group can
  be used for Processor Container Instance resource principals.
- An NSG/security-list and host-firewall path allowing inbound TCP/443 to the UI.

The setup VM and Processor must use the same VCN. The setup script derives the
VM compartment, region, availability domain, VCN, Object Storage namespace, and
OCIR region key from instance metadata and OCI APIs. It selects a compatible
private Processor subnet rather than placing Processor containers in the public
UI subnet.

### IAM baseline

Replace the placeholders with deployment-specific names and scope each policy
to the appropriate compartment:

```text
Allow dynamic-group <ui-deployment-dynamic-group> to manage compute-container-family in compartment <compartment>
Allow dynamic-group <ui-deployment-dynamic-group> to use virtual-network-family in compartment <compartment>
Allow dynamic-group <ui-deployment-dynamic-group> to manage streams in compartment <compartment>
Allow dynamic-group <ui-deployment-dynamic-group> to manage cloudevents-rules in compartment <compartment>
Allow dynamic-group <ui-deployment-dynamic-group> to manage repos in compartment <compartment>
Allow dynamic-group <ui-deployment-dynamic-group> to read vaults in compartment <compartment>
Allow dynamic-group <ui-deployment-dynamic-group> to read keys in compartment <compartment>
Allow dynamic-group <ui-deployment-dynamic-group> to use keys in compartment <compartment>
Allow dynamic-group <ui-deployment-dynamic-group> to manage secrets in compartment <compartment>
Allow dynamic-group <ui-deployment-dynamic-group> to use secret-family in compartment <compartment>
Allow dynamic-group <ui-deployment-dynamic-group> to read secret-bundles in compartment <compartment>
Allow dynamic-group <ui-deployment-dynamic-group> to read objectstorage-namespaces in compartment <compartment>
Allow dynamic-group <ui-deployment-dynamic-group> to manage objects-family in compartment <compartment>
```

The Container Instance family name is `compute-container-family`. If defined
tags are applied, add `use tag-namespaces` for the approved namespace at the
required scope.

Processor resource principals need the smaller runtime subset: consume the
assigned Stream, read the selected secret bundle, read source objects, and use
the configured network. Do not configure registry usernames or authentication
tokens; image build and push use the deployment VM instance principal.

### MySQL preparation

A DBA must create the application schemas, every approved target schema, and a
streaming account before deployment. Use site-approved TLS, host restrictions,
password policy, and credential rotation.

```sql
CREATE DATABASE IF NOT EXISTS stream_db;
CREATE DATABASE IF NOT EXISTS stream_data;
CREATE DATABASE IF NOT EXISTS stream_staging;
CREATE DATABASE IF NOT EXISTS target_db;

CREATE USER IF NOT EXISTS 'stream_user'@'%' IDENTIFIED BY '<secure-password>';
GRANT ALL ON stream_db.* TO 'stream_user'@'%';
GRANT ALL ON stream_data.* TO 'stream_user'@'%';
GRANT ALL ON stream_staging.* TO 'stream_user'@'%';
GRANT ALL ON target_db.* TO 'stream_user'@'%';
```

Repeat the target grant for every database used by a Resource Mapping. The UI
Settings workflow can create or rotate the stream user and apply these grants,
but that action requires a signed-in administrative MySQL account and at least
one existing Resource Mapping.

The MySQL endpoint must be reachable from both the UI VM and the private
Processor subnet. Test routing, TCP/3306, DNS when used, and MySQL account host
rules before creating a Processor.

### HeatWave DB System requirements

The deployment scripts initialize schemas and application objects; they do not
create or resize the HeatWave/MySQL DB System itself. Provision and validate the
DB System before application setup.

| HeatWave item | Required preparation | Application use |
|---|---|---|
| Endpoint | Private IP or resolvable private hostname and TCP port, normally 3306 | Used in the UI Connection Profile and Vault JSON secret |
| MySQL account | Streaming user plus a separate administrative user when the UI must create users/schemas | Processor uses the streaming user; UI login uses credentials entered for the selected profile |
| Control schema | Create/grant `stream_db` or the selected override | Mappings, batch ownership, sequence and deployment history |
| Durable schema | Create/grant `stream_data` or the selected override | Capture, checkpoint, Event TX, retry and archive state |
| Staging schema | Create/grant `stream_staging` or the selected override | Transient working tables and partition exchange preparation |
| Target schemas | Create and grant every schema approved for Resource Mappings | User-visible target tables; never used for application staging |
| TLS | Configure the account and network for the site-required MySQL TLS mode | Connector/Python enables encrypted TCP by default unless explicitly disabled for an approved environment |
| Shape and storage | Size for expected concurrent Processor writers, redo/commit activity and table/partition growth | Not controlled by `env.sh`; observe CPU, connections, statements, write latency/operations and storage headroom |
| Backup and recovery | Configure DB System backups and take a logical/physical recovery point before incompatible reinitialization or upgrade | Required for rollback when a schema migration cannot be reversed safely |

Shape, storage allocation and IOPS characteristics are part of HeatWave capacity
planning, not image deployment. Keep their observed values with performance and
release records so a software upgrade is not mistaken for a database-capacity
change.

### Deployment host and repository

Clone the `main-with-stream` branch on the OL9 VM. A new image needs Git before
the repository bootstrap can install the remaining packages:

```sh
sudo dnf install -y git
git clone --branch main-with-stream <repository-url> oci-object-event-2-table
cd oci-object-event-2-table
./deploy/bootstrap_streaming.sh
```

`bootstrap_streaming.sh` installs the supported OCI, container, Python, MySQL
Connector, and deployment dependencies. Generated runtime files, credentials,
TLS keys, reports, profiles, and caches are ignored by Git.

## What setup discovers, creates, and preserves

| Item | Responsibility |
|---|---|
| Compartment, region, AD, VCN and namespace | Discovered from the setup VM and OCI using its instance principal |
| Private Processor subnet | Selected from available private subnets in the setup VM VCN; explicit override is supported |
| OCIR repository name | Resolved and validated from mandatory `OCI_REGISTRY_REPOSITORY_ID` |
| Vault and AES key | Selected interactively, or auto-selected only when an unattended choice is unambiguous |
| Database secret | Created or updated as `stream_hw_secret_key`, unless an existing `DB_SECRET_OCID` is supplied |
| `deploy/env.sh` | Generated mode `0600`; contains resource references and non-secret configuration, never the DB password |
| Flask signing key | Randomly generated when not supplied |
| TLS certificate | A self-signed certificate can be generated for validation; production should provide a trusted certificate |
| Database structures | Created/migrated from repository-owned external SQL files |
| Images | Published as immutable `processor-<version>` and `ui-<version>` tags in the same repository |
| UI service | Deployed as a systemd-managed Podman container behind nginx HTTPS |
| Processor instances | Created separately per mapping/partition from Event Processor or `deploy_processor.sh` |

## Complete deployment configuration inventory

Use this matrix as the readiness checklist for a fresh deployment.

| Area | Required or selected | Derived/defaulted | Created or deployed |
|---|---|---|---|
| HeatWave/MySQL | Endpoint, streaming user credential, grants, target schemas | Port 3306; control `stream_db`; durable `stream_data`; staging `stream_staging`; target `testdb` | Application tables/migrations are applied from external SQL |
| OCI placement | Running setup VM and instance-principal dynamic group | Compartment, region, AD, VCN, namespace and region key from VM/OCI | None; existing OCI placement is reused |
| Networking | VCN routes/security and private Processor subnet | A unique compatible private subnet can be auto-selected | Processor VNICs are created in the selected private subnet |
| Object Storage | Existing bucket with events enabled | Bucket becomes the default for mappings | Test objects only when a verification harness is run |
| OCI Events | IAM permission and an enabled source bucket | Rule naming/filter values come from each Resource Mapping | Mapping workflow creates Streaming-action rules |
| OCI Streaming | IAM permission; an existing Stream can be selected | FIFO means one partition; Parallel means multiple partitions | UI can create Streams in the deployment compartment |
| Vault/KMS | Existing active Vault and enabled AES key | A unique eligible Vault/key can be selected automatically in unattended setup | `stream_hw_secret_key` is created/updated unless an existing secret is supplied |
| OCIR | Existing repository OCID in the deployment compartment | Repository name and registry endpoint are resolved | Immutable Processor and UI tags are pushed to the same repository |
| UI container | VM, TCP/443 ingress, certificate decision | One Gunicorn process, eight threads, loopback port 8080, nginx HTTPS | Systemd-managed Podman UI container and nginx configuration |
| Processor container | Mapping, Stream, secret, private subnet and image tag | `CI.Standard.E4.Flex`, 1 OCPU, 16 GiB and four writer workers | One or more OCI Container Instances with resource principal enabled |
| Version provenance | Release version or current Git SHA | Git SHA, branch, build UTC and configuration schema version | OCI tags, image build metadata, freeform tags and DB deployment-history rows |

### Minimal `deploy/env.sh`

The generated file is mode `0600` and secret-free. The minimum operator-owned
references are the bucket, OCIR repository OCID and resulting Vault secret OCID.
Schema names and image tags have defaults. OCI placement is derived from the VM.

```sh
export OBJECT_STORAGE_BUCKET_NAME='existing-bucket'
export OCI_REGISTRY_REPOSITORY_ID='ocid1.containerrepo...'
export DB_SECRET_OCID='ocid1.vaultsecret...'

export CONTROL_DATABASE='stream_db'
export STREAM_DATA_DB_NAME='stream_data'
export STAGING_DATABASE='stream_staging'
export PROCESSOR_IMAGE_TAG='<version>'
export UI_IMAGE_TAG='<version>'
export FLASK_SECRET_KEY='<generated-random-value>'
```

Do not add a database password, OCIR username/token, SSH private key, or Vault
secret content. `setup_env.sh` generates this file; manual construction is for
diagnosis or controlled migration only.

### Container runtime requirements

| Component | Placement and runtime | Image/runtime contract |
|---|---|---|
| UI | Podman on the UI VM, systemd service, host networking, bound to `127.0.0.1:8080`, nginx on TCP/443 | Python 3.13 non-root image; one Gunicorn worker and eight threads preserve the in-memory authenticated session store; `/app/instance` is persisted from `ui/instance` |
| Processor | OCI Container Instance on the private subnet, resource principal enabled | Python 3.13 non-root image; default `CI.Standard.E4.Flex`, 1 OCPU, 16 GiB; `WRITER_WORKERS` is 1–32 |

Only these mapping/runtime values are passed to a Processor container:

```text
OCI_STREAM_ID
PROCESSING_MODE
EXPECTED_PARTITION_COUNT
PROCESSOR_REPLICA_COUNT
PROCESSOR_PARTITIONS
DB_SECRET_OCID
WRITER_WORKERS
```

FIFO requires one partition, one replica and assignment `0`. Parallel mode
requires at least two Stream partitions; replicas cannot exceed partitions.
Container Instance environment variables and image references cannot be edited
in place, so configuration and image changes use a replacement deployment.

## Database Vault secret contract

The Processor secret is base64-encoded JSON stored in OCI Vault. Its decoded
content has this contract:

```json
{
  "host": "mysql-private-host",
  "port": 3306,
  "user": "stream_user",
  "credential": "<database-password>",
  "database": "target_db",
  "control_database": "stream_db",
  "stream_data_database": "stream_data",
  "staging_database": "stream_staging"
}
```

The `database` value is the default target presented during setup. Each Resource
Mapping stores its own target database and table. The other three schema names
are application runtime contracts and must remain mutually distinct.

`setup_env.sh` creates JSON, base64-encodes it, sends it to Vault, and writes
only the returned secret OCID to `deploy/env.sh`. For unattended installation,
the password file must be a regular mode-`0600` file. It is removed after the
secret OCID is written successfully.

## Interactive setup

Use this path on the UI/deployment VM when an operator can choose resources:

```sh
cd /home/opc/oci-object-event-2-table
./deploy/bootstrap_streaming.sh
./deploy/setup_env.sh
```

The prompts select the repository, AD, private subnet, Vault and AES key, then
collect the database endpoint, user, schema names, Object Storage bucket, image
version and UI server name. Review the generated `deploy/env.sh`; do not add a
password or registry token.

## Straight-through unattended installation

Create a password file containing only the database password and a separate
installer configuration. Both files must be mode `0600`.

```sh
install -m 600 /dev/null /secure/path/db-password
# Write the password using an approved secret-delivery mechanism.

install -m 600 /dev/null /secure/path/validation-install.env
```

The installer configuration requires only values that cannot be safely
discovered or inferred:

```sh
export OBJECT_STORAGE_BUCKET_NAME='existing-bucket'
export OCI_REGISTRY_REPOSITORY_ID='ocid1.containerrepo...'
export DB_HOST='mysql-private-host'
export DB_PORT='3306'
export DB_USER='stream_user'
export DB_PASSWORD_FILE='/secure/path/db-password'

# Optional when defaults are unsuitable.
export DB_NAME='target_db'
export CONTROL_DATABASE='stream_db'
export STREAM_DATA_DB_NAME='stream_data'
export STAGING_DATABASE='stream_staging'

# Required only when multiple eligible Vaults or keys prevent auto-selection.
export VAULT_ID='ocid1.vault...'
export VAULT_KEY_ID='ocid1.key...'
```

Run the complete installation without an interactive continuation prompt:

```sh
./deploy/install_validation_vm.sh --config /secure/path/validation-install.env
```

The installer bootstraps packages, creates or updates the Vault secret,
deletes the consumed password file, generates `deploy/env.sh`, initializes the
schemas, runs deployment verification, publishes both images, and activates
the HTTPS UI.

## Publish and deploy a release

Use one version for both components:

```sh
cd /home/opc/oci-object-event-2-table
git pull --ff-only origin main-with-stream
VERSION="$(git rev-parse --short HEAD)"
./deploy/publish_release.sh --version "$VERSION"
```

This applies database migrations, publishes `processor-$VERSION` and
`ui-$VERSION`, deploys the UI image, and records secret-free release history.
Use `--resume` only to continue the same version after a stopped partial
release. Tags are immutable and normal releases must use an increased version.

Publishing an image does not replace a running Processor. In **Event Processor
→ Deployment**, select the mapping, the new `processor-*` image tag, the current
database secret, shape, partition assignment, and worker count. Create the
replacement, verify it is ACTIVE and processing, then delete the retired
instance. FIFO uses one Stream partition, one Processor replica, and partition
assignment `0`. Parallel mode uses multiple partitions and has no global FIFO
guarantee.

## Version model

One logical version produces two component tags in the single configured OCIR
repository:

```text
<repository>:processor-<version>
<repository>:ui-<version>
```

Use a Git short SHA for exact source traceability or a controlled release value
such as `1.4.0`. The accepted value contains letters, digits, dots, underscores
and hyphens. Do not include the `processor-` or `ui-` prefix when invoking
`publish_release.sh`; the script adds the component prefixes.

Published tags are immutable. `build_processor_image.sh` refuses to overwrite
an existing Processor tag. `--resume` is the only supported reuse path and is
intended for continuing the same partially completed release, not rebuilding a
different image under an old version.

Every image contains these secret-free stamps:

- logical release version;
- Git revision and source branch;
- UTC build time;
- image name and component tag;
- configuration schema version.

The UI displays its build stamp and deployment history in Settings. Processor
Container Instances carry release, Git and configuration-schema freeform tags;
durable/Event TX records retain the Processor release that handled the message.

## Upgrade to a new version

The normal upgrade is a forward, immutable, replacement deployment.

### 1. Prepare and assess

1. Confirm the current UI and Processor releases in **Settings → Release and
   deployment history** and **Event Processor → Managed instances**.
2. Confirm the repository worktree contains the intended commit and no
   unreviewed deployment changes.
3. Review application SQL migrations under `loader_core/sql/`, `processor/sql/`
   and `ui/myapp/sql/`. Back up control and durable schemas before a migration
   that is not demonstrably backward compatible.
4. Confirm the existing Vault secret bundle, database grants, subnet, IAM,
   repository and bucket remain valid.
5. Run unit and integration tests appropriate to the release before changing
   active resources.

### 2. Publish and activate the UI

```sh
cd /home/opc/oci-object-event-2-table
git fetch origin
git switch main-with-stream
git pull --ff-only origin main-with-stream

VERSION="$(git rev-parse --short HEAD)"
./deploy/publish_release.sh --version "$VERSION"
```

The command performs these operations in order:

1. Loads ignored `deploy/env.sh`.
2. Runs `deploy/initialize_databases.py` against the configured Vault bundle to
   apply idempotent schema creation and migrations.
3. Builds and pushes `processor-$VERSION` using the VM instance principal.
4. Builds or pulls `ui-$VERSION`, writes the UI runtime environment, restarts
   the systemd UI service, and verifies service status.
5. Records the activated UI release in the control database when the deployment
   Python runtime is available.

If execution stops after the Processor image was pushed but before UI activation,
continue the exact same source/version with:

```sh
./deploy/publish_release.sh --version "$VERSION" --resume
```

Do not use `--resume` after source changes. Choose a new version instead.

### 3. Replace Processor deployments

The UI upgrade does not automatically replace Processor Container Instances.
For every active mapping:

1. In **Event Processor → Deployment**, select the mapping and
   `processor-$VERSION` image.
2. Select the current database Vault secret and retain or deliberately revise
   mode, partition assignment, shape, OCPU, memory and worker count.
3. Create the replacement and wait for lifecycle state ACTIVE.
4. Confirm its image, mapping ID, resource principal, Stream assignment and
   secret reference in Managed Instance details.
5. Verify durable capture and Event TX activity for a unique test object.
6. Delete the retired instance only after the replacement is healthy.

For strict FIFO, avoid running old and new consumers against the same partition
as an uncontrolled blue/green pair. Quiesce the Event rule, allow the old
Processor to drain, replace it, verify the checkpoint state, then re-enable the
rule.

### 4. Verify the upgraded release

```sh
sudo systemctl is-active object-storage-heatwave-ui
sudo systemctl is-active nginx
sudo podman ps --format '{{.Names}} {{.Image}}'
curl -kI https://127.0.0.1/
./tests/integration/verify_streaming_deployment.sh
./tests/integration/verify_durable_capture.sh
```

Also verify the UI release stamp, deployment-history rows, active Processor
image tags, Flow resolution, a create/update/delete message lifecycle, exact
target rows/partitions, and absence of orphan staging tables.

### 5. Rollback

Never overwrite or retag an existing version. A rollback selects an already
published immutable image:

```sh
OLD_VERSION='<previous-version>'
RELEASE_VERSION="$OLD_VERSION" \
UI_IMAGE_TAG_OVERRIDE="$OLD_VERSION" \
./deploy/deploy_ui.sh
```

Then create replacement Processors using `processor-$OLD_VERSION`, verify them,
and retire the failed-version instances. A software rollback does not reverse
database migrations. If the older code is not compatible with the upgraded
schema, restore the pre-upgrade database recovery point or deploy a tested
forward-fix version. Preserve durable captures and checkpoints unless a tested
recovery procedure explicitly requires their restoration.

### Version-only and Processor-only cases

- Use `./deploy/publish_release.sh --version "$VERSION" --skip-ui` to publish a
  Processor image without changing the UI.
- A UI-only redeployment can use `UI_IMAGE_TAG_OVERRIDE`, but normal releases
  should still publish matched UI and Processor tags for traceability.
- Changing only a Vault secret version does not require rebuilding either image,
  but affected Processors must be restarted or replaced so they reload the
  secret. The UI login profile must be changed separately when the endpoint also
  changes.
- Changing shape, OCPU, memory, workers, processing mode or partition assignment
  does not require an image rebuild; it requires a replacement Processor
  deployment using the desired immutable image tag.

## First UI sign-in and schema initialization

1. Open `https://<ui-host>/` and accept the self-signed certificate only for an
   approved non-production deployment.
2. Select or create a Connection Profile for the MySQL endpoint.
3. Enter a MySQL username and password. Credentials are retained only in the
   server-side authenticated session.
4. If the selected deployment has no application tables, the UI redirects to
   **Settings** and shows **Install schemas now**.
5. Install/migrate structures. Use **Re-initialize DB structure** only for an
   incompatible legacy schema after backup; it deletes application-owned
   control and durable state but does not drop mapped target tables.
6. Create or verify a Stream, Resource Mapping, OCI Event rule, Vault database
   secret, and Processor deployment.

Database objects are initialized from external SQL under:

- `loader_core/sql/` for control and staging objects;
- `processor/sql/` for durable capture, checkpoint, transaction and migration
  objects;
- `ui/myapp/sql/` for UI mapping and archive registry objects.

## Connection Profiles

Profiles store only non-secret destinations:

- profile name;
- direct MySQL or SSH-tunnel mode;
- MySQL host and port;
- optional default database;
- for SSH mode, jump host, port, user and a server-side private-key reference.

The MySQL username and password are entered at login and are not written to the
profile. Uploaded SSH private keys are stored under the UI instance directory
with restricted permissions and are never displayed.

Authenticated users can always use **Connection Profiles → Create profile**.
The **Login-screen profile creation** setting controls only whether an
unauthenticated user sees the creation link. Disabling public creation does not
disable authenticated profile administration.

After editing a profile, sign out and sign back in. Existing authenticated
sessions keep their original server-side connection state until logout or
connection failure.

## Connect the UI to a different MySQL deployment

Use this procedure when only the UI session needs to inspect or administer a
different database deployment:

1. Sign in to the current deployment.
2. Open **Connection Profiles** and create a profile with the other MySQL host,
   port and optional default database. Use SSH mode only when a server-side
   tunnel is required.
3. Sign out.
4. Select the new profile and sign in with a user authorized on that MySQL
   deployment.
5. Open **Settings** and set the control and stream-data schema names used on
   that server.
6. If application objects are missing, choose **Install schemas now** or
   **Initialize / migrate DB structure**.

Important constraints:

- The saved Settings values are UI-instance configuration, not values isolated
  per Connection Profile. Changing control or durable schema names affects
  subsequent requests on that UI instance.
- The staging schema comes from the deployed UI runtime
  `STAGING_DATABASE`. It is not currently a Connection Profile field. If the
  other deployment uses a different staging schema name, update ignored
  `deploy/env.sh` and redeploy the UI before initializing it.
- A UI-only profile change does not redirect existing Processor instances.
- Target databases are selected per Resource Mapping; there is no global
  target-database or loader-database setting.

For occasional administration of multiple deployments with identical control,
durable and staging schema names, separate profiles are sufficient. For
long-lived deployments with different application schema names, use a separate
UI instance per deployment to avoid global Settings conflicts.

## Move the complete deployment to a different MySQL endpoint

Use this coordinated sequence when the UI and Processors must move together:

1. **Quiesce input.** Disable the relevant OCI Event rules and wait for current
   durable messages to reach a terminal state.
2. **Prepare MySQL.** Create the control, durable, staging and target schemas on
   the new endpoint and grant the streaming user access.
3. **Create or update the Vault secret.** In **Event Processor → Database
   Secret**, select the Vault and AES key and create/update the complete JSON
   bundle for the new endpoint. The recommended default name is
   `stream_hw_secret_key`.
4. **Update deployment defaults.** Set the new secret OCID and matching schema
   names in ignored `deploy/env.sh`. Never add the database password.
5. **Initialize the new application schemas.** From the deployment VM run:

   ```sh
   set -a
   source deploy/env.sh
   set +a
   OCI_AUTH_MODE=instance_principal \
     ./.venv-verification-py312/bin/python deploy/initialize_databases.py
   ```

6. **Create the UI profile.** Add a profile for the new MySQL endpoint, sign out,
   and sign in using that profile. Confirm Settings shows the matching control
   and durable schemas. Redeploy the UI if `STAGING_DATABASE` changed.
7. **Recreate Processors.** Container Instance environment variables cannot be
   edited in place. Create a replacement Processor for every mapping with the
   new secret OCID, verify it is ACTIVE and can connect, then retire the old
   Processor. Updating the content of an existing Vault secret keeps its OCID,
   but running Processors must still be restarted or replaced to load the new
   secret version.
8. **Recreate or migrate mappings deliberately.** Mappings and transaction state
   live in the control/durable schemas. Confirm target database/table values,
   Stream OCIDs, Event rule state, and partition assignments on the new
   deployment.
9. **Verify before resuming events.** Upload a uniquely named test object, check
   the durable capture and Event TX drill-down, verify the exact target row and
   partition counts, then delete it and verify owned data removal.
10. **Enable the intended Event rules** only after the new end-to-end flow
    passes. Keep the old database available until reconciliation and rollback
    requirements are satisfied.

Do not point a new Processor at an empty control schema while the old event rule
continues publishing without planning state migration. OCI Events and Streaming
are at-least-once; preserve durable stream partition/offset history when replay
protection is required.

## Verification checklist

Run the automated deployment checks:

```sh
./tests/integration/verify_streaming_deployment.sh
./tests/integration/verify_durable_capture.sh
```

For disposable end-to-end validation, use the FIFO and Parallel harnesses only
with their documented isolated test resources:

```sh
./tests/integration/verify_fifo_flow.sh
./tests/integration/verify_parallel_flow.sh
```

Then verify:

- `systemctl` reports the UI service and nginx ACTIVE;
- HTTPS responds and the active Podman image has the intended immutable UI tag;
- Flow resolves the Event rule, Stream, Processor, Vault reference, database
  endpoint, and target table;
- the Processor is ACTIVE with resource principal enabled and the intended
  mapping/partition assignment;
- Event TX shows known event type, FIFO or Parallel mode, lifecycle progression,
  target database/table, row counts, and timing metrics;
- Durable Messages shows capture content, attempts, retry/archive behavior and
  transaction linkage;
- create/update loads the expected object-owned target partition and delete
  removes that partition;
- no unexpected staging tables remain after completed transactions;
- release history records the UI and Processor version without secret content.

## Operational cautions

- Disable the Event rule before intentionally deleting source objects when the
  corresponding target data must be preserved. A delivered delete event removes
  the source object's owned target partition.
- OCI Object Storage folder moves/renames do not provide an atomic move event.
- FIFO ordering is guaranteed only within the one assigned Stream partition.
- OCI Events and Streaming are at-least-once; durable capture and offset-based
  idempotency are part of the correctness design.
- One active object uses one target partition. MySQL permits at most 8,192
  partitions per table, leaving at most 8,191 file-owned partitions when the
  permanent seed partition is present. Archive, delete, or manually consolidate
  partitions before reaching the limit.
- A self-signed certificate is for validation only. Use a trusted certificate,
  restricted ingress, private MySQL connectivity, scoped IAM and approved secret
  rotation for production.

## Related documentation

- [Technical implementation and operations details](technical-details.md)
- [Project overview and deployment commands](../README.md)
