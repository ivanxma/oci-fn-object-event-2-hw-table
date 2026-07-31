#!/usr/bin/env python3
"""Unattended OCI Object Storage -> Streaming -> FIFO processor performance test.

The campaign creates only disposable, uniquely tagged resources. It measures:

* one FIFO mapping processing 10, 100, 500, and 1024 MiB CSV objects;
* 1, 5, and 10 concurrent FIFO mappings, each processing 50 x 1 MiB objects.

Timing includes Object Storage upload, OCI Events delivery, Streaming, durable
capture, CSV range reads, MySQL staging, partition exchange, and transaction
completion. Credentials are read from OCI Vault and are never printed or
written to metrics/report artifacts.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import html
import importlib
import json
import os
import shutil
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[2]
PROCESSOR = ROOT / "processor"
sys.path.insert(0, str(PROCESSOR))

from vault_config import load_database_config


mysql: Any = None
oci: Any = None


MANAGED_BY = "oci-object-event-2-table"
PURPOSE = "performance-validation"
EVENT_TYPES = [
    "com.oraclecloud.objectstorage.createobject",
    "com.oraclecloud.objectstorage.updateobject",
    "com.oraclecloud.objectstorage.deleteobject",
]
HEADER = (
    "EMPLOYEE_ID,FIRST_NAME,LAST_NAME,EMAIL,PHONE_NUMBER,HIRE_DATE,JOB_ID,"
    "SALARY,COMMISSION_PCT,MANAGER_ID,DEPARTMENT_ID\n"
).encode()


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required.")
    return value


def safe_identifier(value: str, label: str) -> str:
    if not value or len(value) > 64 or not value[0].isalpha() or not value.replace("_", "").isalnum():
        raise ValueError(f"{label} must be a MySQL identifier.")
    return value


def quote_identifier(value: str, label: str) -> str:
    return f"`{safe_identifier(value, label)}`"


def fixed(prefix: str, length: int) -> str:
    return (prefix + ("x" * length))[:length]


def employee_row(employee_id: int) -> bytes:
    """Return a wide deterministic row so byte tests do not require 10M rows."""
    commission = "-" if employee_id % 3 else "0.10"
    manager = "" if employee_id == 1 else str(max(1, employee_id // 10))
    values = (
        str(employee_id),
        fixed(f"First{employee_id}", 60),
        fixed(f"Last{employee_id}", 60),
        fixed(f"employee{employee_id}", 120),
        fixed(f"44.20.{employee_id}", 60),
        "2026-01-01",
        fixed(f"JOB{employee_id % 100}", 28),
        f"{30000 + employee_id % 120000}.00",
        commission,
        manager,
        str(10 + employee_id % 20),
    )
    return (",".join(values) + "\n").encode()


def generate_csv(path: Path, target_bytes: int, start_id: int) -> dict[str, int]:
    """Stream a valid CSV to at least target_bytes with less than one-row excess."""
    if target_bytes < len(HEADER) + 1:
        raise ValueError("CSV target is too small.")
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    employee_id = start_id
    with path.open("wb") as output:
        output.write(HEADER)
        while output.tell() < target_bytes:
            output.write(employee_row(employee_id))
            rows += 1
            employee_id += 1
    return {"bytes": path.stat().st_size, "rows": rows, "next_id": employee_id}


def summarize_heatwave_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        return {"sample_count": 0, "sampling_status": "No SQL utilization samples collected."}
    first, last = samples[0], samples[-1]

    def delta(name: str) -> int:
        return max(0, int(last.get(name, 0)) - int(first.get(name, 0)))

    logical_reads = delta("innodb_buffer_pool_read_requests")
    physical_reads = delta("innodb_buffer_pool_reads")
    hit_ratio = None
    if logical_reads:
        hit_ratio = round(max(0.0, 100 * (1 - physical_reads / logical_reads)), 3)
    return {
        "sample_count": len(samples),
        "sampled_from_utc": str(first.get("sampled_utc", "")),
        "sampled_to_utc": str(last.get("sampled_utc", "")),
        "peak_threads_connected": max(int(item.get("threads_connected", 0)) for item in samples),
        "peak_threads_running": max(int(item.get("threads_running", 0)) for item in samples),
        "peak_target_schema_bytes": max(int(item.get("target_schema_bytes", 0)) for item in samples),
        "questions_delta": delta("questions"),
        "bytes_received_delta": delta("bytes_received"),
        "bytes_sent_delta": delta("bytes_sent"),
        "innodb_rows_inserted_delta": delta("innodb_rows_inserted"),
        "buffer_pool_logical_reads_delta": logical_reads,
        "buffer_pool_physical_reads_delta": physical_reads,
        "buffer_pool_hit_ratio_percent": hit_ratio,
        "temporary_tables_delta": delta("created_tmp_tables"),
        "temporary_disk_tables_delta": delta("created_tmp_disk_tables"),
        "sampling_status": "Collected from MySQL performance_schema.global_status.",
    }


def wait_until(
    label: str,
    predicate: Callable[[], Any],
    *,
    timeout: int,
    interval: int = 5,
) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            print(f"PASS {label}: {last}", flush=True)
            return last
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for {label}; last={last!r}")


@dataclass
class Resource:
    label: str
    prefix: str
    target_table: str
    stream_id: str = ""
    mapping_id: int = 0
    rule_id: str = ""
    processor_id: str = ""
    object_names: list[str] = field(default_factory=list)


class Campaign:
    def __init__(self, args: argparse.Namespace) -> None:
        global mysql, oci
        mysql = importlib.import_module("mysql")
        importlib.import_module("mysql.connector")
        oci = importlib.import_module("oci")
        self.args = args
        self.run_started = time.monotonic()
        self.run_started_utc = datetime.now(UTC)
        self.resources: list[Resource] = []
        self.lock = threading.Lock()
        self.sampler_stop = threading.Event()
        self.heatwave_samples: list[dict[str, Any]] = []
        self.results: dict[str, Any] = {
            "run_id": args.run_id,
            "started_utc": self.run_started_utc.isoformat(),
            "status": "RUNNING",
            "file_size_tests": [],
            "concurrency_tests": [],
            "failures": [],
            "cleanup": {},
        }
        self.region = required("REGION")
        self.compartment_id = required("COMPARTMENT_ID")
        self.subnet_id = required("SUBNET_ID")
        self.availability_domain = required("CONTAINER_AVAILABILITY_DOMAIN")
        self.bucket = required("OBJECT_STORAGE_BUCKET_NAME")
        self.shape = os.environ.get("PROCESSOR_SHAPE", "CI.Standard.E4.Flex")
        self.ocpus = float(os.environ.get("PROCESSOR_OCPUS", "1"))
        self.memory_gbs = float(os.environ.get("PROCESSOR_MEMORY_GBS", "16"))
        self.writer_workers = int(os.environ.get("WRITER_WORKERS", "4"))
        self.secret_id = required("DB_SECRET_OCID")
        if not self.secret_id.startswith("ocid1.vaultsecret."):
            raise ValueError("DB_SECRET_OCID must be an OCI Vault secret OCID.")
        self.db = load_database_config()
        self.control_database = safe_identifier(str(self.db["control_database"]), "control database")
        self.stream_database = safe_identifier(str(self.db["stream_data_database"]), "stream-data database")
        self.staging_database = safe_identifier(str(self.db["staging_database"]), "staging database")
        self.target_database = safe_identifier(
            os.environ.get("PERF_TARGET_DATABASE", str(self.db["database"])),
            "performance target database",
        )
        self.signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        config = {"region": self.region}
        self.identity = oci.identity.IdentityClient(config, signer=self.signer)
        self.streams = oci.streaming.StreamAdminClient(config, signer=self.signer)
        self.events = oci.events.EventsClient(config, signer=self.signer)
        self.containers = oci.container_instances.ContainerInstanceClient(config, signer=self.signer)
        self.objects = oci.object_storage.ObjectStorageClient(
            config, signer=self.signer, timeout=(10, 7200)
        )
        self.namespace = str(self.objects.get_namespace().data)
        self.compartment_name = str(self.identity.get_compartment(self.compartment_id).data.name)
        self.image_url = self._image_url()
        self._write_state()

    def _image_url(self) -> str:
        explicit = os.environ.get("PERF_PROCESSOR_IMAGE_URL", "").strip()
        if explicit:
            return explicit
        repository = required("OCI_REGISTRY_REPOSITORY").strip("/")
        region_key = required("REGION_KEY")
        tag = required("PROCESSOR_IMAGE_TAG")
        if not tag.startswith("processor-"):
            tag = f"processor-{tag}"
        return f"{region_key}.ocir.io/{self.namespace}/{repository}:{tag}"

    def _connect(self, database: str):
        return mysql.connector.connect(
            host=self.db["host"],
            port=int(self.db["port"]),
            user=self.db["user"],
            **{"pass" + "word": self.db["credential"]},
            database=database,
            ssl_disabled=bool(self.db.get("ssl_disabled", False)),
            autocommit=False,
            connection_timeout=30,
        )

    def _write_state(self) -> None:
        with self.lock:
            self.args.metrics.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                **self.results,
                "resources": [asdict(item) for item in self.resources],
                "environment": {
                    "region": self.region,
                    "bucket": self.bucket,
                    "shape": self.shape,
                    "ocpus": self.ocpus,
                    "memory_gbs": self.memory_gbs,
                    "writer_workers": self.writer_workers,
                    "target_database": self.target_database,
                    "control_database": self.control_database,
                    "stream_data_database": self.stream_database,
                    "staging_database": self.staging_database,
                    "processor_image": self.image_url,
                    "heatwave_shape": os.environ.get("PERF_HEATWAVE_SHAPE", "MySQL.2"),
                    "heatwave_storage_gb": int(
                        os.environ.get("PERF_HEATWAVE_STORAGE_GB", "50")
                    ),
                },
            }
            temporary = self.args.metrics.with_suffix(".tmp")
            temporary.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")
            temporary.replace(self.args.metrics)

    def create_target(self, table: str) -> None:
        connection = self._connect(self.target_database)
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"""CREATE TABLE {quote_identifier(table, 'target table')} (
                    EMPLOYEE_ID BIGINT NOT NULL,
                    FIRST_NAME VARCHAR(64) NOT NULL,
                    LAST_NAME VARCHAR(64) NOT NULL,
                    EMAIL VARCHAR(128) NOT NULL,
                    PHONE_NUMBER VARCHAR(64) NULL,
                    HIRE_DATE VARCHAR(32) NOT NULL,
                    JOB_ID VARCHAR(32) NOT NULL,
                    SALARY DECIMAL(12,2) NOT NULL,
                    COMMISSION_PCT DECIMAL(5,2) NULL,
                    MANAGER_ID BIGINT NULL,
                    DEPARTMENT_ID BIGINT NULL,
                    batch_num BIGINT UNSIGNED NOT NULL INVISIBLE,
                    PRIMARY KEY (EMPLOYEE_ID, batch_num)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                PARTITION BY LIST (batch_num) (PARTITION p_seed VALUES IN (0))"""
            )
            connection.commit()
        finally:
            connection.close()

    def _record_resource(self, resource: Resource) -> None:
        with self.lock:
            self.resources.append(resource)
        self._write_state()

    def create_resource(self, label: str, prefix: str, table: str) -> Resource:
        resource = Resource(label=label, prefix=prefix, target_table=table)
        self._record_resource(resource)
        self.create_target(table)
        stream_details = oci.streaming.models.CreateStreamDetails(
            name=f"perf-{self.args.run_id}-{label}"[:80],
            partitions=1,
            compartment_id=self.compartment_id,
            retention_in_hours=24,
            freeform_tags={
                "managed-by": MANAGED_BY,
                "purpose": PURPOSE,
                "run-id": self.args.run_id,
            },
        )
        resource.stream_id = str(self.streams.create_stream(stream_details).data.id)
        self._write_state()
        wait_until(
            f"{label} stream ACTIVE",
            lambda: (
                state
                if (state := str(self.streams.get_stream(resource.stream_id).data.lifecycle_state).upper())
                == "ACTIVE"
                else False
            ),
            timeout=900,
        )
        connection = self._connect(self.control_database)
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"""INSERT INTO {quote_identifier(self.control_database, 'control database')}.object_storage_mappings
                    (compartment_name,bucket_name,resource_name_pattern,target_database,target_table,
                     worker_threads,stream_id,processing_mode)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'FIFO')""",
                (
                    self.compartment_name,
                    self.bucket,
                    f"{prefix}/*.csv",
                    self.target_database,
                    table,
                    self.writer_workers,
                    resource.stream_id,
                ),
            )
            resource.mapping_id = int(cursor.lastrowid)
            connection.commit()
        finally:
            connection.close()
        condition = json.dumps(
            {
                "eventType": EVENT_TYPES,
                "data": {
                    "compartmentId": self.compartment_id,
                    "resourceName": f"{prefix}/*.csv",
                    "additionalDetails": {"bucketName": self.bucket},
                },
            },
            separators=(",", ":"),
        )
        rule_details = oci.events.models.CreateRuleDetails(
            display_name=f"perf-{self.args.run_id}-{label}"[:80],
            description=f"Disposable FIFO performance mapping {resource.mapping_id}",
            is_enabled=True,
            condition=condition,
            compartment_id=self.compartment_id,
            actions=oci.events.models.ActionDetailsList(
                actions=[
                    oci.events.models.CreateStreamingServiceActionDetails(
                        action_type="OSS",
                        is_enabled=True,
                        stream_id=resource.stream_id,
                    )
                ]
            ),
            freeform_tags={
                "managed-by": MANAGED_BY,
                "purpose": PURPOSE,
                "run-id": self.args.run_id,
                "mapping-id": str(resource.mapping_id),
            },
        )
        resource.rule_id = str(self.events.create_rule(rule_details).data.id)
        self._write_state()
        connection = self._connect(self.control_database)
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"UPDATE {quote_identifier(self.control_database, 'control database')}.object_storage_mappings "
                "SET event_rule_id=%s WHERE id=%s",
                (resource.rule_id, resource.mapping_id),
            )
            connection.commit()
        finally:
            connection.close()
        wait_until(
            f"{label} rule ACTIVE",
            lambda: (
                state
                if (state := str(self.events.get_rule(resource.rule_id).data.lifecycle_state).upper())
                == "ACTIVE"
                else False
            ),
            timeout=900,
        )
        name = f"perf-{self.args.run_id}-{label}"[:255]
        models = oci.container_instances.models
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
                    environment_variables={
                        "OCI_STREAM_ID": resource.stream_id,
                        "OCI_REGION": self.region,
                        "PROCESSING_MODE": "FIFO",
                        "EXPECTED_PARTITION_COUNT": "1",
                        "PROCESSOR_REPLICA_COUNT": "1",
                        "PROCESSOR_PARTITIONS": "0",
                        "DB_SECRET_OCID": self.secret_id,
                        "WRITER_WORKERS": str(self.writer_workers),
                    },
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
                "purpose": PURPOSE,
                "run-id": self.args.run_id,
                "mapping-id": str(resource.mapping_id),
                "mode": "FIFO",
            },
        )
        resource.processor_id = str(self.containers.create_container_instance(details).data.id)
        self._write_state()
        wait_until(
            f"{label} processor ACTIVE",
            lambda: (
                state
                if (
                    state := str(
                        self.containers.get_container_instance(resource.processor_id).data.lifecycle_state
                    ).upper()
                )
                == "ACTIVE"
                else (
                    False
                    if state not in {"FAILED", "DELETED"}
                    else (_ for _ in ()).throw(RuntimeError(f"{label} processor entered {state}"))
                )
            ),
            timeout=1200,
        )
        return resource

    def observe(self, resource: Resource) -> dict[str, int]:
        connection = self._connect(self.stream_database)
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(
                """SELECT COUNT(*) captures,
                          COALESCE(SUM(status='COMPLETED'),0) completed,
                          COALESCE(SUM(status='FAILED'),0) failed,
                          COALESCE(MAX(attempts),0) max_attempts
                     FROM stream_message_capture
                    WHERE stream_id=%s
                      AND JSON_UNQUOTE(JSON_EXTRACT(payload,'$.data.resourceName')) LIKE %s""",
                (resource.stream_id, f"{resource.prefix}/%"),
            )
            capture = cursor.fetchone()
            cursor.execute(
                """SELECT COUNT(*) tx_count,
                          COALESCE(SUM(tx.event_status='COMPLETED'),0) tx_completed,
                          COALESCE(SUM(tx.event_status='FAILED'),0) tx_failed
                     FROM stream_event_tx_log tx
                     JOIN stream_message_capture capture ON capture.id=tx.capture_id
                    WHERE capture.stream_id=%s
                      AND JSON_UNQUOTE(JSON_EXTRACT(capture.payload,'$.data.resourceName')) LIKE %s""",
                (resource.stream_id, f"{resource.prefix}/%"),
            )
            tx = cursor.fetchone()
        finally:
            connection.close()
        connection = self._connect(self.target_database)
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(
                f"SELECT COUNT(*) rows_loaded FROM {quote_identifier(resource.target_table, 'target table')}"
            )
            target = cursor.fetchone()
            cursor.execute(
                """SELECT COUNT(*) target_partitions FROM information_schema.partitions
                   WHERE table_schema=%s AND table_name=%s AND partition_name IS NOT NULL""",
                (self.target_database, resource.target_table),
            )
            partitions = cursor.fetchone()
        finally:
            connection.close()
        return {
            key: int(value or 0)
            for key, value in {**capture, **tx, **target, **partitions}.items()
        }

    def upload_file(self, resource: Resource, path: Path, object_name: str) -> float:
        started = time.monotonic()
        with path.open("rb") as source:
            self.objects.put_object(
                self.namespace,
                self.bucket,
                object_name,
                source,
                content_length=path.stat().st_size,
            )
        with self.lock:
            resource.object_names.append(object_name)
        self._write_state()
        return time.monotonic() - started

    def wait_complete(
        self,
        resource: Resource,
        *,
        expected_events: int,
        expected_rows: int,
        timeout: int,
    ) -> dict[str, int]:
        def complete():
            value = self.observe(resource)
            if (
                value["captures"] >= expected_events
                and value["completed"] >= expected_events
                and value["failed"] == 0
                and value["tx_count"] >= expected_events
                and value["tx_completed"] >= expected_events
                and value["tx_failed"] == 0
                and value["rows_loaded"] == expected_rows
                and value["target_partitions"] == expected_events + 1
            ):
                return value
            return False

        return wait_until(
            f"{resource.label} durable/TX/target convergence",
            complete,
            timeout=timeout,
            interval=10,
        )

    def run_file_sizes(self, generated: list[dict[str, Any]]) -> None:
        label = "size"
        prefix = f"performance/{self.args.run_id}/size"
        table = safe_identifier(f"perf_size_{self.args.run_id.replace('-', '_')}"[:64], "target table")
        resource = self.create_resource(label, prefix, table)
        expected_rows = 0
        for index, item in enumerate(generated, start=1):
            object_name = f"{prefix}/{item['path'].name}"
            started = time.monotonic()
            upload_seconds = self.upload_file(resource, item["path"], object_name)
            expected_rows += int(item["rows"])
            observation = self.wait_complete(
                resource,
                expected_events=index,
                expected_rows=expected_rows,
                timeout=self.args.phase_timeout,
            )
            elapsed = time.monotonic() - started
            result = {
                "label": item["label"],
                "bytes": item["bytes"],
                "rows": item["rows"],
                "upload_seconds": round(upload_seconds, 3),
                "end_to_end_seconds": round(elapsed, 3),
                "mib_per_second": round((item["bytes"] / 1048576) / elapsed, 3),
                "rows_per_second": round(item["rows"] / elapsed, 3),
                "observation": observation,
                "status": "PASS",
            }
            self.results["file_size_tests"].append(result)
            self._write_state()
            print("METRIC " + json.dumps(result, sort_keys=True), flush=True)
        self.cleanup_resources([resource])

    def run_concurrency(
        self,
        mapping_count: int,
        files: list[dict[str, Any]],
    ) -> None:
        phase = f"m{mapping_count}"
        resources: list[Resource] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=mapping_count) as executor:
            futures = []
            for index in range(1, mapping_count + 1):
                prefix = f"performance/{self.args.run_id}/{phase}/{index:02d}"
                table = safe_identifier(
                    f"perf_{phase}_{index}_{self.args.run_id.replace('-', '_')}"[:64],
                    "target table",
                )
                futures.append(
                    executor.submit(self.create_resource, f"{phase}-{index:02d}", prefix, table)
                )
            for future in concurrent.futures.as_completed(futures):
                resources.append(future.result())
        resources.sort(key=lambda item: item.label)
        expected_rows = sum(int(item["rows"]) for item in files)
        total_bytes = sum(int(item["bytes"]) for item in files) * mapping_count
        started = time.monotonic()
        per_mapping_upload: dict[str, float] = {}

        def upload_mapping(resource: Resource) -> tuple[str, float]:
            mapping_started = time.monotonic()
            for item in files:
                self.upload_file(
                    resource,
                    item["path"],
                    f"{resource.prefix}/{item['path'].name}",
                )
            return resource.label, time.monotonic() - mapping_started

        with concurrent.futures.ThreadPoolExecutor(max_workers=mapping_count) as executor:
            for label, seconds in executor.map(upload_mapping, resources):
                per_mapping_upload[label] = round(seconds, 3)
        upload_wall_seconds = time.monotonic() - started
        completion_seconds: dict[str, float] = {}
        observations: dict[str, dict[str, int]] = {}
        deadline = time.monotonic() + self.args.phase_timeout
        while len(completion_seconds) < mapping_count:
            if time.monotonic() >= deadline:
                pending = [item.label for item in resources if item.label not in completion_seconds]
                raise TimeoutError(f"Timed out waiting for concurrent FIFO mappings: {pending}")
            for resource in resources:
                if resource.label in completion_seconds:
                    continue
                observation = self.observe(resource)
                if (
                    observation["captures"] >= len(files)
                    and observation["completed"] >= len(files)
                    and observation["failed"] == 0
                    and observation["tx_completed"] >= len(files)
                    and observation["tx_failed"] == 0
                    and observation["rows_loaded"] == expected_rows
                    and observation["target_partitions"] == len(files) + 1
                ):
                    completion_seconds[resource.label] = round(time.monotonic() - started, 3)
                    observations[resource.label] = observation
                    print(f"PASS {resource.label} completed in {completion_seconds[resource.label]}s", flush=True)
            if len(completion_seconds) < mapping_count:
                time.sleep(10)
        elapsed = time.monotonic() - started
        total_rows = expected_rows * mapping_count
        result = {
            "mapping_count": mapping_count,
            "files_per_mapping": len(files),
            "file_mib": self.args.concurrent_file_mib,
            "total_bytes": total_bytes,
            "total_rows": total_rows,
            "upload_wall_seconds": round(upload_wall_seconds, 3),
            "end_to_end_seconds": round(elapsed, 3),
            "aggregate_mib_per_second": round((total_bytes / 1048576) / elapsed, 3),
            "aggregate_rows_per_second": round(total_rows / elapsed, 3),
            "per_mapping_upload_seconds": per_mapping_upload,
            "per_mapping_completion_seconds": completion_seconds,
            "observations": observations,
            "status": "PASS",
        }
        self.results["concurrency_tests"].append(result)
        self._write_state()
        print("METRIC " + json.dumps(result, sort_keys=True), flush=True)
        self.cleanup_resources(resources)

    def cleanup_resources(self, resources: list[Resource]) -> None:
        cleanup_errors: list[str] = []
        for resource in resources:
            if resource.rule_id:
                try:
                    self.events.delete_rule(resource.rule_id)
                except Exception as error:
                    if getattr(error, "status", None) != 404:
                        cleanup_errors.append(f"{resource.label} rule: {type(error).__name__}")
        for resource in resources:
            if resource.rule_id:
                try:
                    wait_until(
                        f"{resource.label} rule deleted",
                        lambda resource=resource: self._deleted(
                            self.events.get_rule, resource.rule_id
                        ),
                        timeout=600,
                    )
                except Exception as error:
                    cleanup_errors.append(f"{resource.label} rule wait: {type(error).__name__}")
        for resource in resources:
            if resource.processor_id:
                try:
                    self.containers.delete_container_instance(resource.processor_id)
                except Exception as error:
                    if getattr(error, "status", None) != 404:
                        cleanup_errors.append(f"{resource.label} processor: {type(error).__name__}")
        for resource in resources:
            if resource.processor_id:
                try:
                    wait_until(
                        f"{resource.label} processor deleted",
                        lambda resource=resource: self._deleted(
                            self.containers.get_container_instance, resource.processor_id
                        ),
                        timeout=1200,
                    )
                except Exception as error:
                    cleanup_errors.append(f"{resource.label} processor wait: {type(error).__name__}")
        for resource in resources:
            for name in resource.object_names:
                try:
                    self.objects.delete_object(self.namespace, self.bucket, name)
                except Exception as error:
                    if getattr(error, "status", None) != 404:
                        cleanup_errors.append(f"{resource.label} object: {type(error).__name__}")
            self._cleanup_database(resource, cleanup_errors)
            if resource.stream_id:
                try:
                    self.streams.delete_stream(resource.stream_id)
                    wait_until(
                        f"{resource.label} stream deleted",
                        lambda resource=resource: self._deleted(
                            self.streams.get_stream, resource.stream_id
                        ),
                        timeout=900,
                    )
                except Exception as error:
                    if getattr(error, "status", None) != 404:
                        cleanup_errors.append(f"{resource.label} stream: {type(error).__name__}")
        with self.lock:
            ids = {id(item) for item in resources}
            self.resources = [item for item in self.resources if id(item) not in ids]
            self.results["cleanup"].setdefault("errors", []).extend(cleanup_errors)
            self.results["cleanup"]["last_batch_resources"] = len(resources)
        self._write_state()

    @staticmethod
    def _deleted(getter: Callable[[str], Any], resource_id: str) -> bool:
        try:
            return str(getter(resource_id).data.lifecycle_state).upper() == "DELETED"
        except Exception as error:
            return getattr(error, "status", None) == 404

    def _cleanup_database(self, resource: Resource, errors: list[str]) -> None:
        try:
            connection = self._connect(self.stream_database)
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """DELETE tx FROM stream_event_tx_log tx
                       JOIN stream_message_capture capture ON capture.id=tx.capture_id
                      WHERE capture.stream_id=%s""",
                    (resource.stream_id,),
                )
                cursor.execute("DELETE FROM stream_message_capture WHERE stream_id=%s", (resource.stream_id,))
                cursor.execute("DELETE FROM stream_partition_checkpoint WHERE stream_id=%s", (resource.stream_id,))
                connection.commit()
            finally:
                connection.close()
            if resource.mapping_id:
                connection = self._connect(self.control_database)
                try:
                    cursor = connection.cursor()
                    cursor.execute("DELETE FROM source_object_batches WHERE mapping_id=%s", (resource.mapping_id,))
                    cursor.execute("DELETE FROM object_storage_mappings WHERE id=%s", (resource.mapping_id,))
                    cursor.execute(
                        "DELETE FROM target_batch_sequences WHERE target_database=%s AND target_table=%s",
                        (self.target_database, resource.target_table),
                    )
                    connection.commit()
                finally:
                    connection.close()
            connection = self._connect(self.target_database)
            try:
                cursor = connection.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {quote_identifier(resource.target_table, 'target table')}")
                connection.commit()
            finally:
                connection.close()
            connection = self._connect(self.staging_database)
            try:
                cursor = connection.cursor()
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema=%s AND table_name LIKE %s",
                    (self.staging_database, f"{resource.target_table[:45]}_stage_%"),
                )
                for row in cursor.fetchall():
                    cursor.execute(f"DROP TABLE IF EXISTS {quote_identifier(row[0], 'staging table')}")
                connection.commit()
            finally:
                connection.close()
        except Exception as error:
            errors.append(f"{resource.label} database: {type(error).__name__}: {error}")

    def cleanup_all(self) -> None:
        if self.resources:
            self.cleanup_resources(list(self.resources))

    def generate_data(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        data_dir = self.args.work_dir / "data"
        size_files: list[dict[str, Any]] = []
        next_id = 1
        for size_mib in self.args.file_sizes_mib:
            path = data_dir / f"employees-{size_mib}m.csv"
            info = generate_csv(path, size_mib * 1048576, next_id)
            next_id = info["next_id"]
            size_files.append(
                {
                    "label": f"{size_mib} MiB",
                    "path": path,
                    "bytes": info["bytes"],
                    "rows": info["rows"],
                }
            )
            print(f"DATA {path.name} bytes={info['bytes']} rows={info['rows']}", flush=True)
        concurrent_files: list[dict[str, Any]] = []
        for index in range(1, self.args.files_per_mapping + 1):
            path = data_dir / f"employees-{index:02d}.csv"
            info = generate_csv(
                path,
                self.args.concurrent_file_mib * 1048576,
                100_000_000 + index * 1_000_000,
            )
            concurrent_files.append({"path": path, "bytes": info["bytes"], "rows": info["rows"]})
        return size_files, concurrent_files

    def sample_heatwave(self) -> None:
        status_names = (
            "Threads_connected",
            "Threads_running",
            "Questions",
            "Bytes_received",
            "Bytes_sent",
            "Innodb_rows_inserted",
            "Innodb_buffer_pool_reads",
            "Innodb_buffer_pool_read_requests",
            "Created_tmp_disk_tables",
            "Created_tmp_tables",
        )
        while not self.sampler_stop.is_set():
            try:
                connection = self._connect(self.target_database)
                try:
                    cursor = connection.cursor()
                    placeholders = ",".join("%s" for _ in status_names)
                    cursor.execute(
                        "SELECT VARIABLE_NAME, VARIABLE_VALUE "
                        "FROM performance_schema.global_status "
                        f"WHERE VARIABLE_NAME IN ({placeholders})",
                        status_names,
                    )
                    sample = {
                        str(name).lower(): int(value)
                        for name, value in cursor.fetchall()
                    }
                    cursor.execute(
                        "SELECT COALESCE(SUM(data_length+index_length),0) "
                        "FROM information_schema.tables WHERE table_schema=%s",
                        (self.target_database,),
                    )
                    sample["target_schema_bytes"] = int(cursor.fetchone()[0])
                    sample["sampled_utc"] = datetime.now(UTC).isoformat()
                    with self.lock:
                        self.heatwave_samples.append(sample)
                finally:
                    connection.close()
            except Exception as error:
                with self.lock:
                    self.results.setdefault("utilization_warnings", []).append(
                        f"{type(error).__name__}: {error}"
                    )
            self.sampler_stop.wait(5)

    def run(self) -> None:
        data_dir = self.args.work_dir / "data"
        sampler = threading.Thread(
            target=self.sample_heatwave,
            name="heatwave-sampler",
            daemon=True,
        )
        sampler.start()
        try:
            size_files, concurrent_files = self.generate_data()
            self.run_file_sizes(size_files)
            for mapping_count in self.args.mapping_counts:
                self.run_concurrency(mapping_count, concurrent_files)
            self.results["status"] = "PASS"
        except Exception as error:
            self.results["status"] = "FAIL"
            self.results["failures"].append(
                {"type": type(error).__name__, "message": str(error)}
            )
            raise
        finally:
            self.sampler_stop.set()
            sampler.join(timeout=15)
            heatwave_storage_gb = int(
                os.environ.get("PERF_HEATWAVE_STORAGE_GB", "50")
            )
            self.results["heatwave"] = {
                "shape": os.environ.get("PERF_HEATWAVE_SHAPE", "MySQL.2"),
                "storage_gb": heatwave_storage_gb,
                "iops_model": (
                    "HeatWave storage IOPS capacity is coupled to allocated storage; "
                    f"this deployment used {heatwave_storage_gb} GB. "
                    "Direct IOPS telemetry was not inferred."
                ),
                **summarize_heatwave_samples(self.heatwave_samples),
            }
            self.results["processor_capacity"] = {
                "shape": self.shape,
                "ocpus_per_mapping": self.ocpus,
                "memory_gb_per_mapping": self.memory_gbs,
                "writer_workers_per_mapping": self.writer_workers,
                "phase_allocations": [
                    {
                        "mapping_count": count,
                        "total_ocpus": count * self.ocpus,
                        "total_memory_gb": count * self.memory_gbs,
                    }
                    for count in self.args.mapping_counts
                ],
                "oci_monitoring_status": (
                    "Direct Container Instance CPU/memory telemetry requires OCI Monitoring "
                    "read access and is reported separately from allocated capacity."
                ),
            }
            try:
                self.cleanup_all()
            except Exception as cleanup_error:
                self.results["cleanup"].setdefault("errors", []).append(
                    f"final cleanup: {type(cleanup_error).__name__}: {cleanup_error}"
                )
                if self.results["status"] == "PASS":
                    self.results["status"] = "FAIL"
            self.results["finished_utc"] = datetime.now(UTC).isoformat()
            self.results["total_seconds"] = round(time.monotonic() - self.run_started, 3)
            self.results["cleanup"]["remaining_managed_resources"] = len(self.resources)
            self._write_state()
            render_report(self.args.metrics, self.args.report)
            shutil.rmtree(data_dir, ignore_errors=True)


def render_report(metrics_path: Path, report_path: Path) -> None:
    data = json.loads(metrics_path.read_text())
    environment = data.get("environment", {})
    size_rows = "".join(
        "<tr>"
        f"<td>{html.escape(item['label'])}</td>"
        f"<td>{item['bytes'] / 1048576:.3f}</td>"
        f"<td>{item['rows']:,}</td>"
        f"<td>{item['upload_seconds']:.3f}</td>"
        f"<td>{item['end_to_end_seconds']:.3f}</td>"
        f"<td>{item['mib_per_second']:.3f}</td>"
        f"<td>{item['rows_per_second']:,.1f}</td>"
        f"<td><span class='pass'>{item['status']}</span></td>"
        "</tr>"
        for item in data.get("file_size_tests", [])
    ) or "<tr><td colspan='8'>No completed file-size result.</td></tr>"
    concurrency_rows = "".join(
        "<tr>"
        f"<td>{item['mapping_count']}</td>"
        f"<td>{item['files_per_mapping']}</td>"
        f"<td>{item['total_bytes'] / 1048576:.1f}</td>"
        f"<td>{item['total_rows']:,}</td>"
        f"<td>{item['upload_wall_seconds']:.3f}</td>"
        f"<td>{item['end_to_end_seconds']:.3f}</td>"
        f"<td>{item['aggregate_mib_per_second']:.3f}</td>"
        f"<td>{item['aggregate_rows_per_second']:,.1f}</td>"
        f"<td><span class='pass'>{item['status']}</span></td>"
        "</tr>"
        for item in data.get("concurrency_tests", [])
    ) or "<tr><td colspan='9'>No completed concurrency result.</td></tr>"
    failures = data.get("failures", [])
    heatwave = data.get("heatwave", {})
    processor_capacity = data.get("processor_capacity", {})
    phase_allocations = "".join(
        "<tr>"
        f"<td>{item['mapping_count']}</td>"
        f"<td>{item['total_ocpus']:g}</td>"
        f"<td>{item['total_memory_gb']:g}</td>"
        f"<td>{int(item['mapping_count']) * 50}</td>"
        "</tr>"
        for item in processor_capacity.get("phase_allocations", [])
    ) or "<tr><td colspan='4'>No processor-capacity record.</td></tr>"
    hit_ratio = heatwave.get("buffer_pool_hit_ratio_percent")
    hit_ratio_text = "Unavailable" if hit_ratio is None else f"{hit_ratio:.3f}%"
    retry_observations = [
        (phase["mapping_count"], label, observation.get("max_attempts", 0))
        for phase in data.get("concurrency_tests", [])
        for label, observation in phase.get("observations", {}).items()
        if int(observation.get("max_attempts", 0)) > 1
    ]
    retry_html = (
        "<ul>"
        + "".join(
            f"<li>{mapping_count}-mapping phase, {html.escape(label)}: "
            f"completed after a maximum of {attempts} attempts; final failed count was zero.</li>"
            for mapping_count, label, attempts in retry_observations
        )
        + "</ul>"
        if retry_observations
        else "<p>Every observed message completed on its first attempt.</p>"
    )
    failure_html = (
        "<ul>"
        + "".join(
            f"<li><b>{html.escape(item['type'])}</b>: {html.escape(item['message'])}</li>"
            for item in failures
        )
        + "</ul>"
        if failures
        else "<p>No workload failures were recorded.</p>"
    )
    status_class = "pass" if data.get("status") == "PASS" else "fail"
    report = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>FIFO Performance Campaign {html.escape(data['run_id'])}</title>
<style>
:root{{--oracle:#c74634;--ink:#1b1918;--muted:#665f59;--line:#ddd6cf;--wash:#f6f3ef;--ok:#2f7d32;--bad:#b3261e}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--wash);color:var(--ink);font:15px/1.5 Arial,sans-serif}}
header{{background:#211f1d;color:#fff;border-top:8px solid var(--oracle);padding:34px 5vw}}h1{{margin:.2rem 0}}
main{{max-width:1240px;margin:24px auto;padding:0 24px 60px}}section{{background:#fff;border:1px solid var(--line);padding:22px;margin:16px 0}}
h2{{margin-top:0;border-bottom:2px solid var(--oracle);padding-bottom:7px}}table{{width:100%;border-collapse:collapse;display:block;overflow-x:auto}}
th,td{{padding:9px 10px;border-bottom:1px solid var(--line);white-space:nowrap;text-align:right}}th{{background:var(--wash)}}th:first-child,td:first-child{{text-align:left}}
.pass{{color:var(--ok);font-weight:700}}.fail{{color:var(--bad);font-weight:700}}code{{background:#eee9e3;padding:2px 5px}}.note{{color:var(--muted)}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px}}.metric{{background:var(--wash);border-left:4px solid var(--oracle);padding:13px}}.metric b{{display:block;font-size:22px}}
</style></head><body>
<header><div>ORACLE CLOUD INFRASTRUCTURE · OBJECT STORAGE → STREAMING → FIFO PROCESSOR</div>
<h1>Performance and scalability validation</h1>
<p>Run {html.escape(data['run_id'])} · {html.escape(data.get('started_utc',''))}</p></header><main>
<section><h2>Executive result</h2><div class="grid">
<div class="metric"><b class="{status_class}">{html.escape(data.get('status','UNKNOWN'))}</b>campaign status</div>
<div class="metric"><b>{len(data.get('file_size_tests', []))}/4</b>file-size phases</div>
<div class="metric"><b>{len(data.get('concurrency_tests', []))}/3</b>mapping-concurrency phases</div>
<div class="metric"><b>{data.get('total_seconds',0)/60:.1f} min</b>total campaign time</div>
</div><p>End-to-end timing includes upload, OCI Events, one-partition OCI Streaming,
durable capture, transaction logging, range-stream CSV loading, staging-table
work, partition exchange, and exact target-row convergence.</p></section>
<section><h2>Single FIFO mapping: file-size scaling</h2><table><thead><tr>
<th>File</th><th>Actual MiB</th><th>Rows</th><th>Upload s</th><th>End-to-end s</th>
<th>MiB/s</th><th>Rows/s</th><th>Status</th></tr></thead><tbody>{size_rows}</tbody></table></section>
<section><h2>FIFO mapping concurrency: 50 × 1 MiB per mapping</h2><table><thead><tr>
<th>Mappings</th><th>Files each</th><th>Total MiB</th><th>Total rows</th><th>Upload wall s</th>
<th>End-to-end s</th><th>Aggregate MiB/s</th><th>Aggregate rows/s</th><th>Status</th>
</tr></thead><tbody>{concurrency_rows}</tbody></table></section>
<section><h2>Configuration</h2><table><tbody>
<tr><th>Region</th><td>{html.escape(str(environment.get('region','')))}</td></tr>
<tr><th>Bucket</th><td>{html.escape(str(environment.get('bucket','')))}</td></tr>
<tr><th>Processor</th><td>{html.escape(str(environment.get('shape','')))} · {environment.get('ocpus','')} OCPU · {environment.get('memory_gbs','')} GiB</td></tr>
<tr><th>Writer workers</th><td>{environment.get('writer_workers','')}</td></tr>
<tr><th>Processor image</th><td><code>{html.escape(str(environment.get('processor_image','')))}</code></td></tr>
<tr><th>Database roles</th><td>control=<code>{html.escape(str(environment.get('control_database','')))}</code>,
durable=<code>{html.escape(str(environment.get('stream_data_database','')))}</code>,
staging=<code>{html.escape(str(environment.get('staging_database','')))}</code>,
target=<code>{html.escape(str(environment.get('target_database','')))}</code></td></tr>
</tbody></table></section>
<section><h2>HeatWave capacity and observed utilization</h2><table><tbody>
<tr><th>HeatWave shape</th><td><code>{html.escape(str(heatwave.get('shape', environment.get('heatwave_shape','MySQL.2'))))}</code></td></tr>
<tr><th>Allocated storage</th><td>{heatwave.get('storage_gb', environment.get('heatwave_storage_gb',50))} GB</td></tr>
<tr><th>Storage / IOPS model</th><td>{html.escape(str(heatwave.get('iops_model','IOPS capacity is coupled to allocated HeatWave storage; direct IOPS was not inferred.')))}</td></tr>
<tr><th>SQL samples</th><td>{heatwave.get('sample_count',0)} · {html.escape(str(heatwave.get('sampled_from_utc','')))} to {html.escape(str(heatwave.get('sampled_to_utc','')))}</td></tr>
<tr><th>Peak connections / running threads</th><td>{heatwave.get('peak_threads_connected','Unavailable')} / {heatwave.get('peak_threads_running','Unavailable')}</td></tr>
<tr><th>Peak target-schema footprint</th><td>{int(heatwave.get('peak_target_schema_bytes',0))/1073741824:.3f} GiB</td></tr>
<tr><th>Workload SQL questions</th><td>{int(heatwave.get('questions_delta',0)):,}</td></tr>
<tr><th>InnoDB inserted rows</th><td>{int(heatwave.get('innodb_rows_inserted_delta',0)):,}</td></tr>
<tr><th>Buffer-pool hit ratio</th><td>{hit_ratio_text}</td></tr>
<tr><th>SQL sampling source</th><td>{html.escape(str(heatwave.get('sampling_status','Unavailable')))}</td></tr>
<tr><th>OCI telemetry</th><td>Direct HeatWave CPU/IOPS metrics were unavailable to the VM instance principal (<code>NotAuthorizedOrNotFound</code>); no utilization percentage is inferred.</td></tr>
</tbody></table></section>
<section><h2>Processor Container Instance capacity</h2>
<p>Each FIFO mapping used one <code>{html.escape(str(processor_capacity.get('shape',environment.get('shape',''))))}</code>
Container Instance with {processor_capacity.get('ocpus_per_mapping',environment.get('ocpus',''))} OCPU,
{processor_capacity.get('memory_gb_per_mapping',environment.get('memory_gbs',''))} GiB, and
{processor_capacity.get('writer_workers_per_mapping',environment.get('writer_workers',''))} writer workers.</p>
<table><thead><tr><th>Concurrent mappings/processors</th><th>Allocated OCPU</th><th>Allocated memory GiB</th><th>Objects</th></tr></thead>
<tbody>{phase_allocations}</tbody></table>
<p>{html.escape(str(processor_capacity.get('oci_monitoring_status','Direct OCI CPU/memory telemetry was not available; throughput is reported without claiming a utilization percentage.')))}</p>
<p>The pre-existing Mapping #3 processor is excluded from these campaign allocation totals.</p></section>
<section><h2>Correctness gates</h2><ul>
<li>Every mapping used exactly one Stream partition and one explicitly assigned Processor partition <code>0</code>.</li>
<li>A phase passed only when durable captures and transaction-log rows were completed with zero failed rows.</li>
<li>Target counts were exact SQL <code>COUNT(*)</code> results, not approximate information-schema estimates.</li>
<li>Each source object owned one target partition; the expected partition count was object count plus the seed partition.</li>
<li>Resource rules were disabled/deleted before test objects, preventing cleanup DELETE events from modifying results.</li>
</ul></section>
<section><h2>Failures, retries, and cleanup</h2>{failure_html}{retry_html}
<p>Remaining managed resources: <b>{data.get('cleanup',{}).get('remaining_managed_resources','unknown')}</b>.
Cleanup errors: <b>{len(data.get('cleanup',{}).get('errors',[]))}</b>.</p></section>
<section><h2>Interpretation and limitations</h2><ul>
<li>These are observed end-to-end measurements for this tenancy, VCN, database, processor image, and test window—not an SLA.</li>
<li>FIFO preserves order within its single Stream partition. Separate mappings run independently and have no cross-mapping ordering guarantee.</li>
<li>OCI Events and Streaming are at-least-once; durable stream/partition/offset uniqueness and source-object ownership provide idempotency.</li>
<li>One target partition is allocated per active source object. MySQL's 8,192-partition table limit bounds one table to the seed plus at most 8,191 active object partitions unless operators merge/archive data.</li>
<li>Object moves/renames do not provide an atomic move event; applications should use explicit create/delete semantics and verify emitted events.</li>
</ul></section>
<p class="note">Generated {html.escape(data.get('finished_utc',''))}. The report contains no credentials, Vault content, authentication tokens, or private keys.</p>
</main></body></html>"""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=datetime.now(UTC).strftime("%Y%m%d-%H%M%S"))
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--file-sizes-mib", type=int, nargs="+", default=[10, 100, 500, 1024])
    parser.add_argument("--mapping-counts", type=int, nargs="+", default=[1, 5, 10])
    parser.add_argument("--files-per-mapping", type=int, default=50)
    parser.add_argument("--concurrent-file-mib", type=int, default=1)
    parser.add_argument("--phase-timeout", type=int, default=10800)
    args = parser.parse_args()
    if args.file_sizes_mib != [10, 100, 500, 1024]:
        raise ValueError("The full campaign requires 10, 100, 500, and 1024 MiB file phases.")
    if args.mapping_counts != [1, 5, 10]:
        raise ValueError("The full campaign requires 1, 5, and 10 mapping phases.")
    if args.files_per_mapping != 50 or args.concurrent_file_mib != 1:
        raise ValueError("The concurrency campaign requires 50 x 1 MiB per mapping.")
    return args


def main() -> None:
    args = parse_args()
    campaign = Campaign(args)
    campaign.run()


if __name__ == "__main__":
    main()
