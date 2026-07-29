from flask import Blueprint, current_app, flash, redirect, request, url_for
from .common import login_required, mysql_for_request, render_dashboard
from ..services.stream_capture_service import StreamCaptureService
from ..services.mapping_service import MappingService
from ..services.orchestration_service import ContainerOrchestrationService, DeploymentSettings, OrchestrationError
from ..services.streaming_service import StreamingService

orchestration_bp = Blueprint("orchestration", __name__, url_prefix="/orchestration")


def _orchestration_service() -> ContainerOrchestrationService:
    config = current_app.config
    return ContainerOrchestrationService(DeploymentSettings(
        enabled=bool(config["OCI_CONTAINER_ORCHESTRATION_ENABLED"]), compartment_id=config["OCI_COMPARTMENT_ID"], region=config["OCI_REGION"],
        subnet_id=config["SUBNET_ID"], availability_domain=config["CONTAINER_AVAILABILITY_DOMAIN"], shape=config["CONSUMER_SHAPE"],
        ocpus=float(config["CONSUMER_OCPUS"] or 0), memory_gbs=float(config["CONSUMER_MEMORY_GBS"] or 0), image_url=config["CONSUMER_IMAGE_URL"],
        db_secret_ocid=config["DB_SECRET_OCID"], db_host=config["DB_HOST"], db_port=config["DB_PORT"], db_user=config["DB_USER"], db_name=config["DB_NAME"], stream_data_db_name=config["STREAM_DATA_DB_NAME"],
        name_prefix=config["CONSUMER_CONTAINER_NAME_PREFIX"],
    ))

@orchestration_bp.get("/")
@login_required
def index():
    captures = []
    mappings, streams, deployments = [], [], []
    try:
        captures = StreamCaptureService(mysql_for_request(), current_app.config["STREAM_DATA_DB_NAME"]).list_recent()
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
    return render_dashboard("orchestration.html", active_page="orchestration", captures=captures, mappings=mappings, streams=streams, deployments=deployments, orchestration_enabled=current_app.config["OCI_CONTAINER_ORCHESTRATION_ENABLED"])


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
        result = _orchestration_service().create(mapping=mapping, stream_partitions=stream.partitions, partition_assignment=request.form.get("partition_assignment", ""))
        flash(f"Container Instance requested: {result['display_name']} ({result['lifecycle_state']}).", "success")
    except (ValueError, OrchestrationError) as error:
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
