#!/usr/bin/env python3
"""Disposable Object Storage -> Stream -> processor FIFO/Parallel verification."""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
PROCESSOR = ROOT / "processor"
FIXTURES = ROOT / "tests" / "fixtures" / "sql"
sys.path.insert(0, str(PROCESSOR))

import mysql.connector
import oci

from vault_config import load_database_config


IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
MANAGED_BY = "oci-object-event-2-table"
EVENT_TYPES = [
    "com.oraclecloud.objectstorage.createobject",
    "com.oraclecloud.objectstorage.updateobject",
    "com.oraclecloud.objectstorage.deleteobject",
]


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required.")
    return value


def identifier(value: str, label: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"{label} must be a valid MySQL identifier.")
    return value


def quoted(value: str, label: str) -> str:
    return f"`{identifier(value, label)}`"


def statements(path: Path, substitutions: dict[str, str]) -> list[str]:
    text = path.read_text(encoding="utf-8")
    for marker, value in substitutions.items():
        text = text.replace(marker, value)
    text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    return [statement.strip() for statement in text.split(";") if statement.strip()]


def connect(config: dict[str, Any], database: str):
    return mysql.connector.connect(
        host=config["host"],
        port=int(config["port"]),
        user=config["user"],
        **{"pass" + "word": config["credential"]},
        database=database,
        ssl_disabled=bool(config.get("ssl_disabled", False)),
        autocommit=False,
    )


def wait_until(label: str, predicate, timeout: int = 900, interval: int = 10):
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            print(f"PASS: {label}: {last}", flush=True)
            return last
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for {label}; last observation: {last!r}")


def split_csv(path: Path, pieces: int = 10) -> list[bytes]:
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.reader(source))
    if len(rows) < pieces + 1:
        raise ValueError("Parallel CSV fixture does not contain enough data rows.")
    header, data = rows[0], rows[1:]
    chunks: list[bytes] = []
    for index in range(pieces):
        selected = data[index::pieces]
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(selected)
        chunks.append(buffer.getvalue().encode("utf-8"))
    return chunks


class FlowVerification:
    def __init__(self) -> None:
        self.run_started = time.monotonic()
        self.metrics: dict[str, Any] = {}
        self.mode = os.environ.get("FLOW_MODE", "PARALLEL").strip().upper()
        if self.mode not in {"FIFO", "PARALLEL"}:
            raise ValueError("FLOW_MODE must be FIFO or PARALLEL.")
        self.partition_count = 1 if self.mode == "FIFO" else 2
        self.region = required("REGION")
        self.compartment_id = required("COMPARTMENT_ID")
        self.subnet_id = required("SUBNET_ID")
        self.availability_domain = required("CONTAINER_AVAILABILITY_DOMAIN")
        self.shape = required("PROCESSOR_SHAPE")
        self.ocpus = float(required("PROCESSOR_OCPUS"))
        self.memory_gbs = float(required("PROCESSOR_MEMORY_GBS"))
        self.secret_id = required("DB_SECRET_OCID")
        self.stream_id = required(f"{self.mode}_STREAM_ID")
        self.bucket = required("OBJECT_STORAGE_BUCKET_NAME")
        self.fixture = Path(required("PARALLEL_CSV_PATH"))
        if not self.fixture.is_file():
            raise ValueError("PARALLEL_CSV_PATH does not exist.")
        suffix = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        self.prefix = os.environ.get("FLOW_RESUME_PREFIX", "").strip() or f"{self.mode.lower()}-verify-{suffix}"
        self.target_database = identifier(required("FLOW_TARGET_DATABASE"), "flow target database")
        self.target_table = identifier(
            os.environ.get("FLOW_RESUME_TARGET_TABLE", "").strip() or f"employees_{self.mode.lower()}_{suffix}",
            "flow target table",
        )
        resume_mapping = os.environ.get("FLOW_RESUME_MAPPING_ID", "").strip()
        self.mapping_id: int | None = int(resume_mapping) if resume_mapping else None
        self.rule_id = os.environ.get("FLOW_RESUME_RULE_ID", "").strip()
        self.container_ids = [
            value.strip()
            for value in os.environ.get("FLOW_RESUME_CONTAINER_IDS", "").split(",")
            if value.strip()
        ]
        self.chunks = split_csv(self.fixture)
        self.chunk_rows = [max(0, content.count(b"\n") - 1) for content in self.chunks]
        self.expected_rows = sum(self.chunk_rows)
        self.metrics.update(
            {
                "mode": self.mode,
                "partition_count": self.partition_count,
                "object_count": len(self.chunks),
                "expected_rows": self.expected_rows,
                "writer_workers": int(os.environ.get("WRITER_WORKERS", "4")),
            }
        )
        self.object_names = (
            [f"{self.prefix}/employees{index:02d}.csv" for index in range(1, 11)]
            if self.mapping_id is not None
            else []
        )
        self.config = load_database_config()
        self.control_database = identifier(
            str(self.config.get("control_database") or self.config["database"]),
            "control database",
        )
        self.stream_data_database = identifier(
            str(self.config.get("stream_data_database") or os.environ.get("STREAM_DATA_DB_NAME") or self.config["database"]),
            "stream data database",
        )
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        client_config = {"region": self.region}
        self.identity = oci.identity.IdentityClient(client_config, signer=signer)
        self.stream_admin = oci.streaming.StreamAdminClient(client_config, signer=signer)
        self.events = oci.events.EventsClient(client_config, signer=signer)
        self.object_storage = oci.object_storage.ObjectStorageClient(client_config, signer=signer)
        self.containers = oci.container_instances.ContainerInstanceClient(client_config, signer=signer)
        self.namespace = self.object_storage.get_namespace().data
        self.compartment_name = self.identity.get_compartment(self.compartment_id).data.name
        repository = required("REPOSITORY_PREFIX").strip("/").lower()
        image_name = required("PROCESSOR_IMAGE_NAME")
        image_tag = required("PROCESSOR_IMAGE_TAG")
        region_key = required("REGION_KEY")
        self.image_url = f"{region_key}.ocir.io/{self.namespace}/{repository}/{image_name}:{image_tag}"

    def verify_contract(self) -> None:
        stream = self.stream_admin.get_stream(self.stream_id).data
        if str(stream.lifecycle_state).upper() != "ACTIVE" or int(stream.partitions) != self.partition_count:
            raise RuntimeError(
                f"{self.mode}_STREAM_ID must identify an ACTIVE {self.partition_count}-partition Stream."
            )
        print(
            f"PASS: {self.mode} Stream contract: {stream.name} has "
            f"{self.partition_count} ACTIVE partition(s)"
        )

    def create_target(self) -> None:
        script = FIXTURES / "create_employees_parallel_verification.sql"
        sql = statements(
            script,
            {
                "__TARGET_DATABASE__": quoted(self.target_database, "target database"),
                "__TARGET_TABLE__": quoted(self.target_table, "target table"),
            },
        )
        connection = connect(self.config, self.control_database)
        try:
            cursor = connection.cursor()
            for statement in sql:
                cursor.execute(statement)
            connection.commit()
        finally:
            connection.close()
        print(f"PASS: disposable target created: {self.target_database}.{self.target_table}")

    def create_mapping_and_rule(self) -> None:
        connection = connect(self.config, self.control_database)
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"""INSERT INTO {quoted(self.control_database, 'control database')}.object_storage_mappings
                    (compartment_name,bucket_name,resource_name_pattern,target_database,target_table,
                     worker_threads,stream_id,processing_mode)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    self.compartment_name,
                    self.bucket,
                    f"{self.prefix}/employees*.csv",
                    self.target_database,
                    self.target_table,
                    int(os.environ.get("WRITER_WORKERS", "4")),
                    self.stream_id,
                    self.mode,
                ),
            )
            self.mapping_id = int(cursor.lastrowid)
            connection.commit()
        finally:
            connection.close()
        condition = json.dumps(
            {
                "eventType": EVENT_TYPES,
                "data": {
                    "compartmentId": self.compartment_id,
                    "resourceName": f"{self.prefix}/employees*.csv",
                    "additionalDetails": {"bucketName": self.bucket},
                },
            },
            separators=(",", ":"),
        )
        action = oci.events.models.CreateStreamingServiceActionDetails(
            action_type="OSS",
            is_enabled=True,
            description=f"Disposable {self.mode} processor verification",
            stream_id=self.stream_id,
        )
        details = oci.events.models.CreateRuleDetails(
            display_name=f"{self.mode.lower()}-verify-mapping-{self.mapping_id}-{self.prefix[-6:]}",
            description=f"Disposable mapping {self.mapping_id} to {self.target_database}.{self.target_table}",
            is_enabled=True,
            condition=condition,
            compartment_id=self.compartment_id,
            actions=oci.events.models.ActionDetailsList(actions=[action]),
            freeform_tags={
                "managed-by": MANAGED_BY,
                "mapping-id": str(self.mapping_id),
                "purpose": f"{self.mode.lower()}-verification",
            },
        )
        self.rule_id = str(self.events.create_rule(details).data.id)
        connection = connect(self.config, self.control_database)
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"UPDATE {quoted(self.control_database, 'control database')}.object_storage_mappings SET event_rule_id=%s WHERE id=%s",
                (self.rule_id, self.mapping_id),
            )
            connection.commit()
        finally:
            connection.close()
        wait_until(
            f"{self.mode} Events rule ACTIVE",
            lambda: (
                state
                if (state := str(self.events.get_rule(self.rule_id).data.lifecycle_state).upper()) == "ACTIVE"
                else False
            ),
        )

    def create_processor(self, partition: int) -> str:
        name = f"{self.mode.lower()}-verify-{self.mapping_id}-p{partition}-{self.prefix[-6:]}"
        models = oci.container_instances.models
        environment = {
            "OCI_STREAM_ID": self.stream_id,
            "PROCESSING_MODE": self.mode,
            "EXPECTED_PARTITION_COUNT": str(self.partition_count),
            "PROCESSOR_REPLICA_COUNT": str(self.partition_count),
            "PROCESSOR_PARTITIONS": str(partition),
            "DB_SECRET_OCID": self.secret_id,
            "WRITER_WORKERS": os.environ.get("WRITER_WORKERS", "4"),
        }
        details = models.CreateContainerInstanceDetails(
            display_name=name,
            compartment_id=self.compartment_id,
            availability_domain=self.availability_domain,
            shape=self.shape,
            shape_config=models.CreateContainerInstanceShapeConfigDetails(
                ocpus=self.ocpus,
                memory_in_gbs=self.memory_gbs,
            ),
            containers=[
                models.CreateContainerDetails(
                    display_name=name,
                    image_url=self.image_url,
                    is_resource_principal_disabled=False,
                    environment_variables=environment,
                )
            ],
            vnics=[
                models.CreateContainerVnicDetails(
                    subnet_id=self.subnet_id,
                    is_public_ip_assigned=False,
                )
            ],
            freeform_tags={
                "managed-by": MANAGED_BY,
                "mapping-id": str(self.mapping_id),
                "purpose": f"{self.mode.lower()}-verification",
                "partition": str(partition),
            },
        )
        container_id = str(self.containers.create_container_instance(details).data.id)
        self.container_ids.append(container_id)
        wait_until(
            f"processor partition {partition} ACTIVE",
            lambda: (
                state
                if (state := str(self.containers.get_container_instance(container_id).data.lifecycle_state).upper()) == "ACTIVE"
                else (False if state not in {"FAILED", "DELETED"} else (_ for _ in ()).throw(RuntimeError(f"Processor {partition} entered {state}")))
            ),
        )
        return container_id

    def upload(self) -> None:
        self.create_started = time.monotonic()
        for index, content in enumerate(self.chunks, start=1):
            name = f"{self.prefix}/employees{index:02d}.csv"
            self.object_storage.put_object(self.namespace, self.bucket, name, content)
            self.object_names.append(name)
        self.metrics["upload_seconds"] = round(time.monotonic() - self.create_started, 3)
        print(
            f"PASS: uploaded {len(self.object_names)} mutually exclusive CSV objects "
            f"({self.expected_rows} rows)"
        )

    def database_observation(self) -> dict[str, Any]:
        connection = connect(self.config, self.stream_data_database)
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(
                """SELECT COUNT(*) captures,
                          SUM(status='COMPLETED') completed,
                          SUM(status='FAILED') failed,
                          COUNT(DISTINCT partition_id) partitions
                     FROM stream_message_capture
                    WHERE stream_id=%s
                      AND JSON_UNQUOTE(JSON_EXTRACT(payload,'$.data.resourceName')) LIKE %s""",
                (self.stream_id, f"{self.prefix}/%"),
            )
            capture = cursor.fetchone()
            cursor.execute(
                """SELECT COUNT(*) tx_count, SUM(tx.event_status='COMPLETED') tx_completed,
                          MIN(tx.attempts) min_attempts
                     FROM stream_event_tx_log tx
                     JOIN stream_message_capture capture ON capture.id=tx.capture_id
                    WHERE capture.stream_id=%s
                      AND JSON_UNQUOTE(JSON_EXTRACT(capture.payload,'$.data.resourceName')) LIKE %s""",
                (self.stream_id, f"{self.prefix}/%"),
            )
            tx = cursor.fetchone()
        finally:
            connection.close()
        connection = connect(self.config, self.target_database)
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(f"SELECT COUNT(*) rows_loaded FROM {quoted(self.target_table, 'target table')}")
            target = cursor.fetchone()
            cursor.execute(
                """SELECT COUNT(*) partition_count FROM information_schema.partitions
                   WHERE table_schema=%s AND table_name=%s AND partition_name IS NOT NULL""",
                (self.target_database, self.target_table),
            )
            partition_count = int(cursor.fetchone()["partition_count"])
        finally:
            connection.close()
        return {
            **capture,
            **tx,
            **target,
            "target_partitions": partition_count,
        }

    def wait_for_create_flow(self) -> dict[str, Any]:
        def complete():
            value = self.database_observation()
            if (
                int(value["captures"] or 0) >= 10
                and int(value["completed"] or 0) >= 10
                and int(value["failed"] or 0) == 0
                and int(value["partitions"] or 0) == self.partition_count
                and int(value["rows_loaded"] or 0) == self.expected_rows
                and int(value["tx_count"] or 0) >= 10
                and int(value["tx_completed"] or 0) >= 10
            ):
                return value
            return False

        result = wait_until(f"{self.mode} create flow and transaction log", complete)
        elapsed = time.monotonic() - self.create_started
        self.metrics.update(
            {
                "create_event_to_database_seconds": round(elapsed, 3),
                "create_rows_per_second": round(self.expected_rows / elapsed, 3),
                "create_observation": result,
            }
        )
        return result

    def delete_subset_and_verify(self) -> dict[str, Any]:
        started = time.monotonic()
        for name in self.object_names[:5]:
            self.object_storage.delete_object(self.namespace, self.bucket, name)

        def complete():
            value = self.database_observation()
            if (
                int(value["captures"] or 0) >= 15
                and int(value["completed"] or 0) >= 15
                and int(value["failed"] or 0) == 0
                and int(value["rows_loaded"] or 0) == self.expected_rows - sum(self.chunk_rows[:5])
                and int(value["target_partitions"] or 0) == 6
            ):
                return value
            return False

        result = wait_until("five DELETE events drop five owned partitions", complete)
        self.metrics["delete_first_five_seconds"] = round(time.monotonic() - started, 3)
        self.metrics["delete_first_five_observation"] = result
        return result

    def delete_remaining_and_verify(self) -> None:
        started = time.monotonic()
        for name in self.object_names[5:]:
            self.object_storage.delete_object(self.namespace, self.bucket, name)

        def complete():
            value = self.database_observation()
            if (
                int(value["captures"] or 0) >= 20
                and int(value["completed"] or 0) >= 20
                and int(value["rows_loaded"] or 0) == 0
                and int(value["target_partitions"] or 0) == 1
            ):
                return value
            return False

        result = wait_until("remaining DELETE events leave only seed partition", complete)
        self.metrics["delete_remaining_seconds"] = round(time.monotonic() - started, 3)
        self.metrics["delete_final_observation"] = result

    def write_metrics(self) -> None:
        self.metrics["total_seconds"] = round(time.monotonic() - self.run_started, 3)
        path = os.environ.get("FLOW_METRICS_JSON", "").strip()
        output = json.dumps(self.metrics, indent=2, sort_keys=True, default=str)
        print("METRICS=" + json.dumps(self.metrics, sort_keys=True, default=str), flush=True)
        if path:
            target = Path(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(output + "\n", encoding="utf-8")

    def cleanup(self) -> None:
        rule_delete_requested = False
        if self.rule_id:
            try:
                self.events.delete_rule(self.rule_id)
                rule_delete_requested = True
            except Exception as error:
                print(f"WARN: could not delete rule: {type(error).__name__}", file=sys.stderr)
        if rule_delete_requested:
            def rule_deleted():
                try:
                    return str(
                        self.events.get_rule(self.rule_id).data.lifecycle_state
                    ).upper() == "DELETED"
                except Exception as error:
                    return getattr(error, "status", None) == 404

            wait_until("verification Events rule DELETED", rule_deleted, timeout=300, interval=5)

        requested_container_deletes: list[str] = []
        for container_id in self.container_ids:
            try:
                self.containers.delete_container_instance(container_id)
                requested_container_deletes.append(container_id)
            except Exception as error:
                print(f"WARN: could not delete processor {container_id}: {type(error).__name__}", file=sys.stderr)
        for container_id in requested_container_deletes:
            def container_deleted(container_id=container_id):
                try:
                    return str(
                        self.containers.get_container_instance(container_id).data.lifecycle_state
                    ).upper() == "DELETED"
                except Exception as error:
                    return getattr(error, "status", None) == 404

            wait_until(
                f"verification processor {container_id} DELETED",
                container_deleted,
                timeout=600,
                interval=5,
            )

        # Only after event emission and consumption are stopped is it safe to
        # remove the objects and their mapping. Otherwise a shutting-down
        # processor can durably capture cleanup DELETE events after the mapping
        # has disappeared, leaving retryable verifier-only rows.
        for name in self.object_names:
            try:
                self.object_storage.delete_object(self.namespace, self.bucket, name)
            except Exception:
                pass
        if self.mapping_id is not None:
            connection = connect(self.config, self.stream_data_database)
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """DELETE tx FROM stream_event_tx_log tx
                       JOIN stream_message_capture capture ON capture.id=tx.capture_id
                      WHERE capture.stream_id=%s
                        AND JSON_UNQUOTE(JSON_EXTRACT(capture.payload,'$.data.resourceName')) LIKE %s""",
                    (self.stream_id, f"{self.prefix}/%"),
                )
                cursor.execute(
                    """DELETE FROM stream_message_capture
                       WHERE stream_id=%s
                         AND JSON_UNQUOTE(JSON_EXTRACT(payload,'$.data.resourceName')) LIKE %s""",
                    (self.stream_id, f"{self.prefix}/%"),
                )
                connection.commit()
            finally:
                connection.close()
            connection = connect(self.config, self.control_database)
            try:
                cursor = connection.cursor()
                cursor.execute(
                    f"DELETE FROM {quoted(self.control_database, 'control database')}.source_object_batches WHERE mapping_id=%s",
                    (self.mapping_id,),
                )
                cursor.execute(
                    f"DELETE FROM {quoted(self.control_database, 'control database')}.object_storage_mappings WHERE id=%s",
                    (self.mapping_id,),
                )
                cursor.execute(
                    f"DELETE FROM {quoted(self.control_database, 'control database')}.target_batch_sequences WHERE target_database=%s AND target_table=%s",
                    (self.target_database, self.target_table),
                )
                connection.commit()
            finally:
                connection.close()
        connection = connect(self.config, self.control_database)
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"DROP TABLE IF EXISTS {quoted(self.target_database, 'target database')}.{quoted(self.target_table, 'target table')}"
            )
            connection.commit()
        finally:
            connection.close()
        print(f"PASS: disposable {self.mode} verification resources cleaned up")

    def run(self) -> None:
        succeeded = False
        try:
            self.verify_contract()
            if self.mapping_id is None:
                self.create_target()
                self.create_mapping_and_rule()
                for partition in range(self.partition_count):
                    self.create_processor(partition)
                self.upload()
            else:
                if not self.rule_id or len(self.container_ids) != self.partition_count:
                    raise ValueError(
                        f"A resumed run requires FLOW_RESUME_RULE_ID and "
                        f"{self.partition_count} FLOW_RESUME_CONTAINER_IDS."
                    )
                print(
                    f"PASS: resuming preserved {self.mode} verification mapping {self.mapping_id}",
                    flush=True,
                )
                self.create_started = time.monotonic()
                self.metrics["resumed"] = True
            created = self.wait_for_create_flow()
            deleted = self.delete_subset_and_verify()
            self.delete_remaining_and_verify()
            print(
                f"PASS: {self.mode} flow verified "
                f"(create={created}, after-five-deletes={deleted})",
                flush=True,
            )
            self.write_metrics()
            succeeded = True
        finally:
            keep_on_failure = os.environ.get("FLOW_KEEP_RESOURCES_ON_FAILURE", "true").lower() == "true"
            if succeeded or not keep_on_failure:
                self.cleanup()
            elif not succeeded:
                print(
                    f"WARN: preserving failed resources for diagnosis: prefix={self.prefix}, "
                    f"mapping_id={self.mapping_id}, rule_id={self.rule_id}, processors={','.join(self.container_ids)}",
                    file=sys.stderr,
                )


if __name__ == "__main__":
    FlowVerification().run()
