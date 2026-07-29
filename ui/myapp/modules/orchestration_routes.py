from flask import Blueprint, current_app, flash, redirect, request, url_for
from .common import login_required, mysql_for_request, render_dashboard
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
    vault_secrets, vaults, vault_keys = [], [], []
    selected_vault_id = request.args.get("vault_id", "")
    mappings, streams, deployments = [], [], []
    managed_state = request.args.get("managed_state", "ALL").upper()
    active_tab = request.args.get("tab", "deployment").lower()
    if managed_state not in {"ALL", "ACTIVE"}:
        managed_state = "ALL"
    if active_tab not in {"deployment", "instances", "database-secret"}:
        active_tab = "deployment"
    try:
        mappings = MappingService(mysql_for_request()).list_mappings()
        streams = StreamingService(compartment_id=current_app.config["OCI_COMPARTMENT_ID"], region=current_app.config["OCI_REGION"], enabled=bool(current_app.config["OCI_STREAMING_MANAGEMENT_ENABLED"])).list_streams()
    except Exception as error:
        flash(f"Could not load mapping deployment choices: {type(error).__name__}: {error}", "warning")
    if current_app.config["OCI_CONTAINER_ORCHESTRATION_ENABLED"]:
        try:
            deployments = _orchestration_service().list_deployments(state_filter=managed_state)
        except (ValueError, OrchestrationError) as error:
            flash(str(error), "warning")
    vault_service = VaultSecretService(compartment_id=current_app.config["OCI_COMPARTMENT_ID"], region=current_app.config["OCI_REGION"])
    try:
        vault_secrets = vault_service.list_active_secrets()
    except VaultSecretError as error:
        flash(f"OCI Vault secret choices are unavailable: {error}", "warning")
    try:
        vaults = vault_service.list_active_vaults()
    except VaultSecretError as error:
        flash(f"OCI Vault choices are unavailable: {error}", "warning")
    if selected_vault_id:
        try:
            vault_keys = vault_service.list_active_keys(selected_vault_id)
        except VaultSecretError as error:
            flash(f"OCI Vault encryption-key choices are unavailable: {error}", "warning")
    return render_dashboard(
        "orchestration.html", active_page="orchestration", active_tab=active_tab, managed_state=managed_state, mappings=mappings,
        streams=streams, deployments=deployments, vault_secrets=vault_secrets, vaults=vaults, vault_keys=vault_keys, selected_vault_id=selected_vault_id,
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


@orchestration_bp.post("/database-secret")
@login_required
def create_database_secret():
    try:
        vault_service = VaultSecretService(compartment_id=current_app.config["OCI_COMPARTMENT_ID"], region=current_app.config["OCI_REGION"])
        values = dict(host=request.form.get("db_host", ""), port=request.form.get("db_port", "3306"), user=request.form.get("db_user", ""), password=request.form.get("db_password", ""), database=request.form.get("db_name", ""), control_database=request.form.get("control_database", ""), stream_data_database=request.form.get("stream_data_database", ""))
        existing_secret_id = request.form.get("existing_secret_id", "")
        if existing_secret_id:
            vault_service.update_database_secret(secret_id=existing_secret_id, **values)
            flash("OCI Vault database secret updated as a new version. Existing processors keep the same secret OCID.", "success")
        else:
            secret = vault_service.create_database_secret(name=request.form.get("secret_name", ""), vault_id=request.form.get("vault_id", ""), key_id=request.form.get("key_id", ""), **values)
            flash(f"OCI Vault database secret created: {secret.name} ({secret.id}). Select it in Processor deployment.", "success")
    except (ValueError, VaultSecretError) as error:
        flash(str(error), "error")
    return redirect(url_for("orchestration.index", tab="database-secret"))


@orchestration_bp.get("/deployments/<deployment_id>")
@login_required
def deployment_detail(deployment_id: str):
    try:
        deployment = _orchestration_service().get_deployment(deployment_id)
        return render_dashboard("orchestration_detail.html", active_page="orchestration", deployment=deployment)
    except (ValueError, OrchestrationError) as error:
        flash(str(error), "error")
        return redirect(url_for("orchestration.index", tab="instances"))


@orchestration_bp.post("/deployments/<deployment_id>/delete")
@login_required
def delete_deployment(deployment_id: str):
    try:
        _orchestration_service().delete(deployment_id)
        flash("Container Instance deletion requested.", "success")
    except (ValueError, OrchestrationError) as error:
        flash(str(error), "error")
    return redirect(url_for("orchestration.index", tab="instances"))


@orchestration_bp.post("/deployments/batch-delete")
@login_required
def delete_deployments():
    deployment_ids = request.form.getlist("deployment_ids")
    try:
        if not deployment_ids:
            raise ValueError("Select one or more active managed Container Instances.")
        service = _orchestration_service()
        for deployment_id in deployment_ids:
            service.delete(deployment_id)
        flash(f"Deletion requested for {len(deployment_ids)} managed Container Instance(s).", "success")
    except (ValueError, OrchestrationError) as error:
        flash(str(error), "error")
    return redirect(url_for("orchestration.index", tab="instances", managed_state=request.form.get("managed_state", "ALL")))
