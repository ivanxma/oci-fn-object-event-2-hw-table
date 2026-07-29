import json

from flask import Blueprint, current_app, flash, redirect, request, url_for
from .common import login_required, mysql_for_request, render_dashboard
from ..services.stream_capture_service import StreamCaptureService
from ..services.streaming_service import StreamingService

durable_messages_bp = Blueprint("durable_messages", __name__, url_prefix="/durable-messages")

def _service(): return StreamCaptureService(mysql_for_request(), current_app.config["STREAM_DATA_DB_NAME"])

def _message_summary(payload):
    """Return a compact, non-mutating durable-message preview for the table."""
    try:
        value = json.loads(payload) if isinstance(payload, str) else payload
        data = value.get("data", {}) if isinstance(value, dict) else {}
        details = data.get("additionalDetails", {}) if isinstance(data, dict) else {}
        event_type = str(value.get("eventType") or value.get("type") or "Message")
        resource = str(data.get("resourceName") or details.get("objectName") or "")
        return f"{event_type} · {resource}" if resource else event_type
    except (TypeError, ValueError, json.JSONDecodeError):
        return "Structured message (select View selected)"

@durable_messages_bp.get("/")
@login_required
def index():
    captures, archived, partitions, summary, detail, archive_detail, streams = [], [], [], {}, None, None, []
    selected_archive_partition = ""
    active_tab = request.args.get("tab", "current").lower()
    if active_tab == "partitions": active_tab = "archived"
    if active_tab not in {"current", "archived"}: active_tab = "current"
    try:
        service = _service(); status = request.args.get("capture_status", "").upper(); stream = request.args.get("capture_stream", "")
        captures = service.list_recent(status=status, stream_id=stream); archived, partitions = service.list_archived(); summary = service.summary()
        selected_partitions = request.args.getlist("partition_names")
        if selected_partitions:
            selected_archive_partition = selected_partitions[0]
            archived = [item for item in archived if item["archive_partition"] == selected_archive_partition]
        if request.args.get("capture_id"): detail = service.get(request.args["capture_id"])
        archive_reference = request.args.getlist("archive_refs")
        if archive_reference:
            partition_name, archive_id = archive_reference[0].rsplit(":", 1)
            archive_detail = service.get_archived(partition_name, archive_id)
        elif request.args.get("archive_id") and request.args.get("archive_partition"):
            archive_detail = service.get_archived(request.args["archive_partition"], request.args["archive_id"])
    except Exception as error: flash(f"Could not load durable stream captures: {type(error).__name__}: {error}", "error")
    try:
        streams = StreamingService(compartment_id=current_app.config["OCI_COMPARTMENT_ID"], region=current_app.config["OCI_REGION"], enabled=bool(current_app.config["OCI_STREAMING_MANAGEMENT_ENABLED"])).list_streams()
    except Exception as error: flash(f"Could not load Stream choices: {type(error).__name__}: {error}", "warning")
    # Durable records retain the immutable OCID.  Resolve a display label only
    # for the operator table, so the Stream name and OCID remain together.
    stream_names = {item.id: item.name for item in streams}
    for record in [*captures, *archived]:
        stream_id = str(record.get("stream_id", ""))
        if stream_id in stream_names:
            record["stream_id"] = f"{stream_names[stream_id]} — {stream_id}"
    for record in [*captures, *archived]:
        record["message_summary"] = _message_summary(record.get("payload"))
    return render_dashboard("durable_messages.html", active_page="durable_messages", active_tab=active_tab, captures=captures, archived_captures=archived, archive_partitions=partitions, capture_summary=summary, capture_detail=detail, selected_archive_partition=selected_archive_partition, streams=streams, capture_status=request.args.get("capture_status", "").upper(), capture_stream=request.args.get("capture_stream", ""))

@durable_messages_bp.post("/captures/<capture_id>/retry")
@login_required
def retry_capture(capture_id):
    try:
        changed = _service().retry(capture_id); flash("Capture queued for retry." if changed else "Only failed captures can be retried.", "success" if changed else "warning")
    except Exception as error: flash(f"Could not retry capture: {type(error).__name__}: {error}", "error")
    return redirect(url_for("durable_messages.index"))

@durable_messages_bp.post("/captures/<capture_id>/archive")
@login_required
def archive_capture(capture_id):
    try:
        changed = _service().archive(capture_id, request.form.get("archive_granularity", "MONTH")); flash("Durable message archived." if changed else "Only completed or failed messages can be archived.", "success" if changed else "warning")
    except Exception as error: flash(f"Could not archive capture: {type(error).__name__}: {error}", "error")
    return redirect(url_for("durable_messages.index"))

@durable_messages_bp.post("/captures/batch")
@login_required
def batch_capture_action():
    try:
        capture_ids = request.form.getlist("capture_ids")
        if not capture_ids:
            raise ValueError("Select one or more durable messages.")
        service, operation = _service(), request.form.get("operation", "")
        if operation == "retry":
            changed = sum(1 for capture_id in capture_ids if service.retry(capture_id))
            flash(f"{changed} failed capture(s) queued for retry.", "success" if changed else "warning")
        elif operation == "archive":
            changed = sum(1 for capture_id in capture_ids if service.archive(capture_id, request.form.get("archive_granularity", "MONTH")))
            flash(f"{changed} terminal capture(s) archived.", "success" if changed else "warning")
        else:
            raise ValueError("Choose a durable message action.")
    except Exception as error:
        flash(f"Could not apply durable message action: {type(error).__name__}: {error}", "error")
    return redirect(url_for("durable_messages.index"))

@durable_messages_bp.post("/archive/<partition_name>/<archive_id>/delete")
@login_required
def delete_archived_capture(partition_name, archive_id):
    try:
        changed = _service().delete_archived(partition_name, archive_id); flash("Archived durable message deleted." if changed else "Archived message was not found.", "success" if changed else "warning")
    except Exception as error: flash(f"Could not delete archived capture: {type(error).__name__}: {error}", "error")
    return redirect(url_for("durable_messages.index"))

@durable_messages_bp.post("/archive/batch")
@login_required
def batch_archived_action():
    try:
        references = request.form.getlist("archive_refs")
        if not references:
            raise ValueError("Select one or more archived messages.")
        if request.form.get("operation") != "delete":
            raise ValueError("Choose an archived-message action.")
        changed = 0
        for reference in references:
            partition_name, archive_id = reference.rsplit(":", 1)
            changed += int(bool(_service().delete_archived(partition_name, archive_id)))
        flash(f"{changed} archived message(s) deleted.", "success" if changed else "warning")
    except Exception as error:
        flash(f"Could not apply archived-message action: {type(error).__name__}: {error}", "error")
    return redirect(url_for("durable_messages.index", tab="archived"))

@durable_messages_bp.post("/archive/<partition_name>/delete")
@login_required
def delete_archive_partition(partition_name):
    try:
        changed = _service().delete_archive_partition(partition_name); flash("Archived message partition deleted." if changed else "Archived partition was not found.", "success" if changed else "warning")
    except Exception as error: flash(f"Could not delete archived partition: {type(error).__name__}: {error}", "error")
    return redirect(url_for("durable_messages.index"))

@durable_messages_bp.post("/archive/partitions/batch")
@login_required
def batch_archive_partition_action():
    try:
        partition_names = request.form.getlist("partition_names")
        if not partition_names:
            raise ValueError("Select one or more archive partitions.")
        if request.form.get("operation") != "delete":
            raise ValueError("Choose an archive-partition action.")
        changed = sum(int(bool(_service().delete_archive_partition(name))) for name in partition_names)
        flash(f"{changed} archive partition(s) deleted.", "success" if changed else "warning")
    except Exception as error:
        flash(f"Could not apply archive-partition action: {type(error).__name__}: {error}", "error")
    return redirect(url_for("durable_messages.index", tab="partitions"))
