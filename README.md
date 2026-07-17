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

## Required access

The deployment instance principal needs Functions, Events, repository, namespace,
subnet, and Logging management permissions in the deployment compartment when
Function logging is enabled. The deployed Function's resource principal needs
read access to objects in the relevant bucket(s).

For the dynamic group that contains the deployment Compute instances, add these
OCI IAM statements (replace placeholders with your dynamic group and target
compartment):

```text
Allow dynamic-group <deployment-instance-dynamic-group> to manage log-groups in compartment <deployment-compartment>
Allow dynamic-group <deployment-instance-dynamic-group> to read log-content in compartment <deployment-compartment>
```

`manage log-groups` permits the deployment script to create the Functions
`invoke` service log; `read log-content` permits `deploy/showlog.sh` to search
that log. Without the first statement, OCI returns `CreateLog`
`NotAuthorizedOrNotFound` even though the instance principal can list log
groups.

The MySQL account needs access to `fndb` plus `SELECT`, `INSERT`, `UPDATE`,
`CREATE`, `ALTER`, and `DROP` on mapped target schemas. Restrict this account to
the needed schemas and Function subnet/network path.

## Local checks

```sh
python3.13 -m compileall function ui/myapp
bash -n deploy/deploy.sh deploy/bootstrap.sh
```

No secrets, private keys, OCI auth tokens, generated reports, or UI runtime
state are tracked by Git.
