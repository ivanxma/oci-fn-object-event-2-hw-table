from flask import Blueprint, current_app, flash, redirect, request, url_for
from .common import login_required, mysql_for_request, render_dashboard
from ..services.stream_capture_service import StreamCaptureService
from ..services.mapping_service import MappingService
from ..services.orchestration_service import ConsumerRuntime, ContainerOrchestrationService, DeploymentSettings, OrchestrationError
from ..services.streaming_service import StreamingService
from ..services.vault_secret_service import VaultSecretError, VaultSecretService

orchestration_bp = Blueprint("orchestration", __name__, url_prefix="/orchestration")


def _orchestration_service() -> ContainerOrchestrationService:
    config = current_app.config
    return ContainerOrchestrationService(DeploymentSettings(
        enabled=bool(config["OCI_CONTAINER_ORCHESTRATION_ENABLED"]), compartment_id=config["OCI_COMPARTMENT_ID"], region=config["OCI_REGION"],
        subnet_id=config["SUBNET_ID"], availability_domain=config["CONTAINER_AVAILABILITY_DOMAIN"], shape=config["CONSUMER_SHAPE"],
        ocpus=float(config["CONSUMER_OCPUS"] or 0), memory_gbs=float(config["CONSUMER_MEMORY_GBS"] or 0), image_url=config["CONSUMER_IMAGE_URL"],
        db_secret_ocid=config["DB_SECRET_OCID"], db_host=config["DB_HOST"], db_port=config["DB_PORT"], db_user=config["DB_USER"], db_name=config["DB_NAME"], stream_data_db_name=config["STREAM_DATA_DB_NAME"], control_database=config["CONTROL_DATABASE"], writer_workers=int(config["WRITER_WORKERS"] or 4),
        name_prefix=config["CONSUMER_CONTAINER_NAME_PREFIX"],
    ))

@orchestration_bp.get("/")
@login_required
def index():
    captures, archived_captures, vault_secrets, capture_summary, capture_detail, archive_detail = [], [], [], {}, None, None
    mappings, streams, deployments = [], [], []
    try:
        capture_service = StreamCaptureService(mysql_for_request(), current_app.config["STREAM_DATA_DB_NAME"])
        capture_status = request.args.get("capture_status", "").upper()
        capture_stream = request.args.get("capture_stream", "")
        captures = capture_service.list_recent(status=capture_status, stream_id=capture_stream)
        archived_captures = capture_service.list_archived()
        capture_summary = capture_service.summary()
        if request.args.get("capture_id"):
            capture_detail = capture_service.get(request.args["capture_id"])
        if request.args.get("archive_id"):
            archive_detail = capture_service.get(request.args["archive_id"], archived=True)
    except Exception as error:
        flash(f"Could not load durable stream captures: {type(error).__name__}: {error}", "error")
    try:
        mappings = MappingService(mysql_for_request()).list_mappings()
        streams = StreamingService(compartment_id=current_app.config["OCI_COMPARTMENT_ID"], region=current_app.config["OCI_REGION"], enabled=bool(current_app.config["OCI_STREAMING_MANAGEMENT_ENABLED"])).list_streams()
    except Exception as error:
        flash(f"Could not load mapping deployment choices: {type(error).__name__}: {error}", "warning")
    if current_app.config["OCI_CONTAINER_ORCHESTRATION_ENABLED"]:
        try:
            deployments = _orchestration_service().list_deployments()
        except OrchestrationError as error:
            flash(str(error), "warning")
    try:
        vault_secrets = VaultSecretService(
            compartment_id=current_app.config["OCI_COMPARTMENT_ID"], region=current_app.config["OCI_REGION"]
        ).list_active_secrets()
    except VaultSecretError as error:
        flash(f"OCI Vault secret choices are unavailable: {error}", "warning")
    return render_dashboard(
        "orchestration.html", active_page="orchestration", captures=captures, archived_captures=archived_captures,
        capture_summary=capture_summary, capture_detail=capture_detail, archive_detail=archive_detail,
        capture_status=request.args.get("capture_status", "").upper(), capture_stream=request.args.get("capture_stream", ""), mappings=mappings,
        streams=streams, deployments=deployments, vault_secrets=vault_secrets,
        settings=_orchestration_service().settings,
        orchestration_enabled=current_app.config["OCI_CONTAINER_ORCHESTRATION_ENABLED"],
    )


@orchestration_bp.post("/deploy")
@login_required
def deploy():
    try:
        mapping_id = int(request.form.get("mapping_id", ""))
        mapping = MappingService(mysql_for_request()).get_mapping(mapping_id)
        if not mapping:
            raise ValueError("Select a valid mapping.")
        stream = next((item for item in StreamingService(compartment_id=current_app.config["OCI_COMPARTMENT_ID"], region=current_app.config["OCI_REGION"], enabled=bool(current_app.config["OCI_STREAMING_MANAGEMENT_ENABLED"])).list_streams() if item.id == mapping.get("stream_id")), None)
        if not stream:
            raise ValueError("The mapping stream is not available in the UI compartment.")
        service = _orchestration_service()
        runtime = ConsumerRuntime.from_form(request.form, service.settings)
        allowed_secrets = {item.id for item in VaultSecretService(
            compartment_id=current_app.config["OCI_COMPARTMENT_ID"], region=current_app.config["OCI_REGION"]
        ).list_active_secrets()}
        if runtime.db_secret_ocid not in allowed_secrets:
            raise ValueError("Choose an active OCI Vault secret from this compartment.")
        result = service.create(mapping=mapping, stream_partitions=stream.partitions, partition_assignment=request.form.get("partition_assignment", ""), runtime=runtime)
        flash(f"Container Instance requested: {result['display_name']} ({result['lifecycle_state']}).", "success")
    except (ValueError, OrchestrationError, VaultSecretError) as error:
        flash(str(error), "error")
    return redirect(url_for("orchestration.index"))

@orchestration_bp.post("/captures/<capture_id>/retry")
@login_required
def retry_capture(capture_id: str):
    try:
        retried = StreamCaptureService(mysql_for_request(), current_app.config["STREAM_DATA_DB_NAME"]).retry(int(capture_id))
        flash("Capture queued for retry." if retried else "Only failed captures can be retried.", "success" if retried else "warning")
    except Exception as error:
        flash(f"Could not retry capture: {error}", "error")
    return redirect(url_for("orchestration.index"))


@orchestration_bp.post("/captures/<capture_id>/archive")
@login_required
def archive_capture(capture_id: str):
    try:
        archived = StreamCaptureService(mysql_for_request(), current_app.config["STREAM_DATA_DB_NAME"]).archive(capture_id)
        flash("Durable message archived." if archived else "Only completed or failed messages can be archived.", "success" if archived else "warning")
    except Exception as error:
        flash(f"Could not archive capture: {type(error).__name__}: {error}", "error")
    return redirect(url_for("orchestration.index"))


@orchestration_bp.post("/archive/<archive_id>/delete")
@login_required
def delete_archived_capture(archive_id: str):
    try:
        deleted = StreamCaptureService(mysql_for_request(), current_app.config["STREAM_DATA_DB_NAME"]).delete_archived(archive_id)
        flash("Archived durable message deleted." if deleted else "Archived message was not found.", "success" if deleted else "warning")
    except Exception as error:
        flash(f"Could not delete archived capture: {type(error).__name__}: {error}", "error")
    return redirect(url_for("orchestration.index"))
