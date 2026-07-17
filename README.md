# OCI Object Event to MySQL Table

This project turns OCI Object Storage create, update, and delete events into
partition-exchanged MySQL table updates. It combines the tested prototype's
per-target-table batch allocation, parallel CSV inserts, and partition exchange
with an OCI Function deployment and the Flask management UI.

## Layout

```text
function/  OCI Function source and partition loader
ui/        Separate Flask UI application
deploy/    Oracle Linux bootstrap and idempotent OCI deployment scripts
tests/     Function-focused tests and fixtures
```

## Event flow

1. OCI Events invokes `function/func.py` for an Object Storage event.
2. The Function records the raw event in `fndb.object_event`, then persists its
   `object_event_id` on the corresponding transaction audit entry. This makes
   the UI's **Event TX → Object Storage Event** status and error drill-down
   deterministic for newly received events.
3. It resolves `fndb.object_storage_mappings` by compartment, bucket, and
   resource-name pattern.
4. For create/update, it assigns a batch number scoped to the mapped target
   database/table, downloads the object with a resource principal, loads it
   into a staging table in parallel, and atomically exchanges the corresponding
   `batch_num` partition into the target table.
5. For delete, it finds the mapped batch and truncates that partition.
6. It records success and failure rows in `fndb.event_tx_log` and
   `fndb.event_errors`.

If a load fails after its batch is allocated, its batch record changes from
`LOADING` to `ERROR`, allowing a later update event to retry it. A configurable
`LOAD_LEASE_SECONDS` (default `120`) also treats an abandoned `LOADING` record
as retryable while still rejecting genuinely concurrent loads for the same
source object.

The target table must already exist, use `LIST` partitioning by an invisible
`batch_num` column, and include `batch_num` in every unique key. The Function
does not create arbitrary business tables.

### Staging-table lifecycle

Each create or update uses a short-lived staging table with a UUID suffix, for
example `employees_stage_a1b2c3d4e5f6`. This prevents concurrent events and
retries from sharing a staging name. The table is removed after a successful
partition exchange and is also cleaned up after a failed load. Audit and error
records remain in the control database after the temporary table is gone.

## UI

The Flask UI is deliberately separate from the Function deployment:

```sh
cd ui
python3.13 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
export FLASK_SECRET_KEY='set-a-local-random-value'
flask --app myapp.app run --host 127.0.0.1 --port 8080
```

Use **Resource Mappings** to register a target. Use **Event TX** to review
per-table transactions and raw Object Storage events. Database passwords remain
only in the UI's server-side active connection state.

### Event TX views

- **Object Storage Event** adds a lifecycle column to raw OCI events:
  `SUCCESS` is green, `ERROR` is red, and `RECEIVED` is blue. A red
  **ERROR · View log** link opens and highlights the corresponding
  `event_errors` record. Historical entries retain a bucket/resource/action
  fallback until their next event is processed with the direct link.
- **Registered Table** server-pages transaction history (10 rows by default;
  change **Show** and select **Refresh**). Its toolbar keeps Refresh on the
  left and a single CSV-download icon on the right.
- The ▤ button beside **Target table** opens a paged dialog of the selected
  target table's visible rows. The dialog fetches only the requested page, so
  large target tables are not loaded into the browser or application at once.

### Deploy the UI with HTTPS

On Oracle Linux, `deploy/deploy_ui.sh` builds the UI container, creates a
systemd service, binds the container only to `127.0.0.1`, and creates an nginx
HTTPS reverse proxy on port 443. It does not expose the Flask port directly.

```sh
cd deploy
# env.sh must be mode 0600 and include FLASK_SECRET_KEY plus TLS settings.
./deploy_ui.sh
```

Set `TLS_CERT_FILE` and `TLS_KEY_FILE` in `env.sh` to existing CA-signed
material. For a non-production test only, set `GENERATE_SELF_SIGNED_CERT=true`.
The script enables the host firewall's HTTPS service; also allow inbound
TCP/443 in the Compute instance's OCI NSG or security list. The generated UI
runtime environment and any generated TLS files remain untracked.

## Deploy the OCI Function

On an Oracle Linux deployment host with an OCI instance principal:

```sh
cd deploy
./bootstrap.sh
cp env.sh.example env.sh
chmod 600 env.sh
# Edit env.sh locally. Do not commit it.
./deploy.sh
```

`deploy.sh` creates or updates the Function application, deploys the container,
sets its protected Function configuration, and creates or updates the Object
Storage Events rule. It expects the Functions application subnet to reach the
MySQL endpoint and the Object Storage service.

The required `env.sh` variables are the OCI compartment/subnet/application
details, OCIR credentials, and `DB_HOST`, `DB_USER`, and `DB_PASSWORD`. Optional
`BATCH_ROWS` and `WRITER_WORKERS` control the loader. Set
`OBJECT_STORAGE_BUCKET_NAME` to limit the Events rule to a bucket.
Set `OBJECT_STORAGE_OBJECT_NAME_PATTERN='myfolder/*.csv'` to additionally
limit the rule to matching `data.resourceName` values under that virtual folder.
OCI Events supports `*` in filter values; leave the setting blank to process
all object names in the selected bucket.

### Function invocation logs

Invocation logging is enabled by default when `ENABLE_FUNCTION_LOG='true'` and
`FUNCTION_LOG_GROUP_ID` is set. During deployment, `deploy.sh` creates or
reuses the application's OCI Functions `invoke` service log in that group.
Set `FUNCTION_LOG_NAME` only when a custom log name is required. The log group
and log identifiers belong in the protected `deploy/env.sh`, never in source
control. On first creation, `deploy.sh` discovers the created log and writes
`FUNCTION_LOG_ID` back to that protected file. Use `deploy/showlog.sh` to view
recent invocations.

## OCI IAM policies: Function, Events, and Object Storage

This deployment has three separate OCI principals. Do not give the Function the
deployment VM's broad permissions, and do not use a human user's API key inside
the Function.

| Principal | Used for | Minimum scope |
| --- | --- | --- |
| Deployment Compute instance principal | Builds/pushes the image and creates the Function, Event Rule, and Function log | Function/repository/Event/Logging resources in the deployment compartment |
| OCI Function resource principal | Downloads the CSV that triggered the event | `read objects` for the specific source bucket |
| OCI Events service | Delivers an already-matched event to the Function | No extra policy in the normal same-tenancy case |

Replace every `<...>` value; policy examples deliberately use names/OCIDs rather
than the protected values in `deploy/env.sh`.

### 1. Deployment Compute instance principal

Put the deployment VM in a dynamic group. Prefer an explicit instance OCID for
a single deployment host; use a compartment rule only when every instance in
that compartment is trusted to deploy this application.

```text
# Option A: one deployment VM
instance.id = '<deployment-instance-ocid>'

# Option B: deployment VMs in one compartment
ALL {resource.type = 'instance', resource.compartment.id = '<deployment-compartment-ocid>'}
```

For that dynamic group, grant the deployment capabilities used by
`deploy/deploy.sh` and `deploy/showlog.sh`:

```text
Allow dynamic-group <deployment-instance-dg> to read objectstorage-namespaces in tenancy
Allow dynamic-group <deployment-instance-dg> to inspect repos in tenancy
Allow dynamic-group <deployment-instance-dg> to manage repos in compartment <registry-compartment>
Allow dynamic-group <deployment-instance-dg> to manage functions-family in compartment <function-compartment>
Allow dynamic-group <deployment-instance-dg> to use virtual-network-family in compartment <network-compartment>
Allow dynamic-group <deployment-instance-dg> to manage cloudevents-rules in compartment <rule-compartment>
Allow dynamic-group <deployment-instance-dg> to manage logging-family in compartment <logging-compartment>
Allow dynamic-group <deployment-instance-dg> to read log-content in compartment <logging-compartment>
```

Use the same compartment for the placeholders when the repository, Function
application, Event Rule, subnet, and log group are co-located. `manage repos`
can be restricted further by repository name if the deployment identity also
has the required `inspect repos` permission. If Function invocation logging is
disabled, omit the final two Logging statements.

### 2. OCI Function resource principal: read the CSV bucket

The Function uses `get_resource_principals_signer()` to call Object Storage.
Create a separate dynamic group for it. Scope it to the exact Function when
possible; a compartment-wide rule is suitable only for a dedicated Functions
compartment.

```text
# Preferred: one Function
resource.id = '<function-ocid>'

# Alternative: all Functions in a compartment
ALL {resource.type = 'fnfunc', resource.compartment.id = '<function-compartment-ocid>'}
```

Grant read-only access to the source bucket. The loader only calls `GetObject`.

```text
Allow dynamic-group <object-loader-function-dg> to read objects in compartment <bucket-compartment> where all {target.bucket.name='<bucket-name>'}
```

If CSV files are received from multiple buckets, add one statement per bucket.
Use `manage objects` only if a future Function revision must create, overwrite,
or remove objects. OCI resource-principal policy and dynamic-group changes can
take up to 15 minutes to be reflected in a running Function.

### 3. Object Storage event emission and Event Rule

The source bucket must have object events enabled; enabling a rule alone is not
enough. A bucket administrator can enable it with:

```sh
oci os bucket update --name '<bucket-name>' --object-events-enabled true
```

`deploy/deploy.sh` creates an OCI Events rule with Function action and filters
it using `OBJECT_STORAGE_BUCKET_NAME` and, optionally,
`OBJECT_STORAGE_OBJECT_NAME_PATTERN` (for example `myfolder/*.csv`). Configure
the rule for all required event types:

```text
com.oraclecloud.objectstorage.createobject
com.oraclecloud.objectstorage.updateobject
com.oraclecloud.objectstorage.deleteobject
```

In the usual same-tenancy deployment, OCI Events can deliver a matched rule to
the selected Function without a separate `eventrule` invocation policy. For a
cross-tenancy action, create the required paired `endorse`/`admit` policy using
the `FN_INVOCATION` permission; do not reuse the same-tenancy template.

### 4. MySQL network and database access

IAM does not replace database or network authorization. The Function subnet
must be able to reach the MySQL host and port (typically TCP/3306), and the
MySQL account needs access to the control database plus only the mapped target
schemas. At minimum the loader needs `SELECT`, `INSERT`, `UPDATE`, `CREATE`,
`ALTER`, and `DROP` where its staging/partition workflow requires them. Restrict
the account by schema and network path; keep its password only in protected
Function configuration (`deploy/env.sh`), never in Git or the UI.

For OCI reference, see [Functions resource-principal access](https://docs.oracle.com/en-us/iaas/Content/Functions/Tasks/functionsaccessingociresources.htm), [Functions deployment policies](https://docs.oracle.com/en-us/iaas/Content/Functions/Tasks/functionscreatingpolicies.htm), [Events IAM policies](https://docs.oracle.com/en-us/iaas/Content/Events/Concepts/eventspolicy.htm), and [enabling bucket object events](https://docs.oracle.com/iaas/Content/Object/Tasks/managingbuckets_topic-To_enable_or_disable_emitting_events_for_object_state_changes.htm).

## Local checks

```sh
python3.13 -m compileall function ui/myapp
bash -n deploy/deploy.sh deploy/bootstrap.sh
```

No secrets, private keys, OCI auth tokens, generated reports, or UI runtime
state are tracked by Git.
