"""Read-only topology dashboard for Object Storage streaming flows."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from flask import Blueprint, current_app, flash, jsonify

from .common import login_required, mysql_for_request, render_dashboard
from .orchestration_routes import _orchestration_service
from ..services.event_rule_service import EventRuleService
from ..services.mapping_service import MappingService
from ..services.streaming_service import StreamingService
from ..services.vault_secret_service import VaultSecretService


flow_bp = Blueprint("flow", __name__, url_prefix="/flow")


def _processor_secret_id(deployment: dict) -> str:
    for container in deployment.get("containers", []):
        for item in container.get("environment", []):
            if item.get("name") == "DB_SECRET_OCID":
                return str(item.get("value") or "")
    return ""


def _status_class(value: str) -> str:
    value = value.upper()
    if value in {"ACTIVE", "RUNNING", "CONFIGURED", "COMPLETED"}:
        return "status-active"
    if value in {"CREATING", "UPDATING", "PROCESSING", "PENDING"}:
        return "status-pending"
    if value in {"FAILED", "ERROR", "DELETED", "NOT DEPLOYED"}:
        return "status-error"
    return "status-unknown"


def _load_topology(mappings: list[dict], config: dict) -> tuple[list[dict], list[str]]:
    """Resolve slow OCI topology independently of the initial page response."""
    if not mappings:
        return [], []
    rules, streams, deployments, secrets = [], [], [], []
    warnings: list[str] = []
    rule_service = EventRuleService(
        compartment_id=config["OCI_COMPARTMENT_ID"], region=config["OCI_REGION"],
        enabled=bool(config["OCI_EVENT_RULE_MANAGEMENT_ENABLED"]), rule_prefix=config["OCI_EVENT_RULE_PREFIX"],
    )
    streaming_service = StreamingService(
        compartment_id=config["OCI_COMPARTMENT_ID"], region=config["OCI_REGION"],
        enabled=bool(config["OCI_STREAMING_MANAGEMENT_ENABLED"]),
    )
    orchestration = _orchestration_service()
    secret_service = VaultSecretService(
        compartment_id=config["OCI_COMPARTMENT_ID"], region=config["OCI_REGION"]
    )
    loaders = {
        "rules": rule_service.list_stream_rules,
        "streams": streaming_service.list_streams,
        # Deleted instances can be numerous and cannot process a mapping. Avoid
        # one nested Container API traversal for every historical deployment.
        "deployments": lambda: orchestration.list_deployments(state_filter="ACTIVE"),
        "secrets": secret_service.list_active_secrets,
    }
    loaded = {name: [] for name in loaders}
    with ThreadPoolExecutor(max_workers=len(loaders)) as executor:
        pending = {name: executor.submit(loader) for name, loader in loaders.items()}
        for name, future in pending.items():
            try:
                loaded[name] = future.result()
            except Exception as error:
                label = {
                    "rules": "rules",
                    "streams": "streams",
                    "deployments": "processors",
                    "secrets": "database secrets",
                }[name]
                warnings.append(f"Could not load Flow {label}: {type(error).__name__}: {error}")
    rules, streams = loaded["rules"], loaded["streams"]
    deployments, secrets = loaded["deployments"], loaded["secrets"]

    # Only active processors assigned to visible mappings need nested Container
    # details. Fetch them concurrently so one slow OCI call does not serialize
    # the entire topology page.
    mapping_ids = {str(item["id"]) for item in mappings}
    relevant = [item for item in deployments if str(item.get("mapping_id", "")) in mapping_ids]
    if relevant:
        with ThreadPoolExecutor(max_workers=min(8, len(relevant))) as executor:
            detail_futures = {
                item["id"]: executor.submit(orchestration.get_deployment, item["id"])
                for item in relevant
            }
            for deployment in relevant:
                try:
                    deployment.update(detail_futures[deployment["id"]].result())
                except Exception:
                    # The list record still makes a useful, non-secret topology node.
                    deployment["containers"] = []

    stream_by_id = {item.id: item for item in streams}
    rule_by_id = {item.id: item for item in rules}
    secret_by_id = {item.id: item for item in secrets}
    flows = []
    for mapping in mappings:
        mapping_id = str(mapping["id"])
        flow_processors = [item for item in deployments if str(item.get("mapping_id", "")) == mapping_id]
        # OCI retains deleted Container Instances in list responses.  Prefer a
        # live replacement and only show a deleted record when no live
        # processor exists for the mapping.
        flow_processors.sort(
            key=lambda item: (
                str(item.get("lifecycle_state", "")).upper() != "ACTIVE",
                str(item.get("lifecycle_state", "")).upper() in {"DELETED", "DELETING"},
                str(item.get("id", "")),
            )
        )
        processor = flow_processors[0] if flow_processors else None
        secret_id = _processor_secret_id(processor or {}) or str(config.get("DB_SECRET_OCID") or "")
        stream = stream_by_id.get(str(mapping.get("stream_id") or ""))
        rule = rule_by_id.get(str(mapping.get("event_rule_id") or ""))
        flows.append({
            "id": mapping_id,
            "event": f"{mapping.get('bucket_name', 'Bucket')}/{mapping.get('resource_name_pattern', '')}",
            "rule": rule.display_name if rule else ("Rule not deployed" if not mapping.get("event_rule_id") else "OCI rule"),
            "rule_state": rule.lifecycle_state if rule else "UNRESOLVED",
            "rule_status_class": _status_class(rule.lifecycle_state if rule else "UNRESOLVED"),
            "stream": stream.name if stream else "Stream unavailable",
            "stream_id": str(mapping.get("stream_id") or ""),
            "stream_state": stream.lifecycle_state if stream else "UNRESOLVED",
            "stream_status_class": _status_class(stream.lifecycle_state if stream else "UNRESOLVED"),
            "processing_mode": str(mapping.get("processing_mode") or "FIFO"),
            "processor": processor.get("display_name") if processor else "No managed processor",
            "processor_state": processor.get("lifecycle_state") if processor else "NOT DEPLOYED",
            "processor_status_class": _status_class(str(processor.get("lifecycle_state")) if processor else "NOT DEPLOYED"),
            "secret": secret_by_id.get(secret_id).name if secret_id in secret_by_id else ("No selected secret" if not secret_id else "Vault secret"),
            "secret_id": secret_id,
            "secret_state": secret_by_id.get(secret_id).lifecycle_state if secret_id in secret_by_id else ("UNRESOLVED" if secret_id else "NOT CONFIGURED"),
            "secret_status_class": _status_class(secret_by_id.get(secret_id).lifecycle_state if secret_id in secret_by_id else "UNRESOLVED"),
            "database_endpoint": ":".join(part for part in (str(config.get("DB_HOST") or ""), str(config.get("DB_PORT") or "")) if part) or "Endpoint supplied by Vault configuration",
            "target": f"{mapping.get('target_database')}.{mapping.get('target_table')}",
        })
    return flows, warnings


@flow_bp.get("/")
@login_required
def index():
    mappings = []
    try:
        mappings = MappingService(mysql_for_request()).list_mappings()
    except Exception as error:
        flash(f"Could not load Flow mappings: {type(error).__name__}: {error}", "warning")
    initial_flows = [
        {
            "id": str(mapping["id"]),
            "event": f"{mapping.get('bucket_name', 'Bucket')}/{mapping.get('resource_name_pattern', '')}",
            "target": f"{mapping.get('target_database')}.{mapping.get('target_table')}",
        }
        for mapping in mappings
    ]
    return render_dashboard("flow.html", active_page="flow", flows=initial_flows)


@flow_bp.get("/topology")
@login_required
def topology():
    try:
        mappings = MappingService(mysql_for_request()).list_mappings()
    except Exception as error:
        return jsonify(
            flows=[],
            warnings=[f"Could not load Flow mappings: {type(error).__name__}: {error}"],
        ), 503
    flows, warnings = _load_topology(mappings, dict(current_app.config))
    return jsonify(flows=flows, warnings=warnings)
