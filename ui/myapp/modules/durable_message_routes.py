from flask import Blueprint, current_app, flash, redirect, request, url_for
from .common import login_required, mysql_for_request, render_dashboard
from ..services.stream_capture_service import StreamCaptureService

durable_messages_bp = Blueprint("durable_messages", __name__, url_prefix="/durable-messages")

def _service(): return StreamCaptureService(mysql_for_request(), current_app.config["STREAM_DATA_DB_NAME"])

@durable_messages_bp.get("/")
@login_required
def index():
    captures, archived, partitions, summary, detail, archive_detail = [], [], [], {}, None, None
    try:
        service = _service(); status = request.args.get("capture_status", "").upper(); stream = request.args.get("capture_stream", "")
        captures = service.list_recent(status=status, stream_id=stream); archived, partitions = service.list_archived(); summary = service.summary()
        if request.args.get("capture_id"): detail = service.get(request.args["capture_id"])
        if request.args.get("archive_id") and request.args.get("archive_partition"): archive_detail = service.get_archived(request.args["archive_partition"], request.args["archive_id"])
    except Exception as error: flash(f"Could not load durable stream captures: {type(error).__name__}: {error}", "error")
    return render_dashboard("durable_messages.html", active_page="durable_messages", captures=captures, archived_captures=archived, archive_partitions=partitions, capture_summary=summary, capture_detail=detail, archive_detail=archive_detail, capture_status=request.args.get("capture_status", "").upper(), capture_stream=request.args.get("capture_stream", ""))

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

@durable_messages_bp.post("/archive/<partition_name>/<archive_id>/delete")
@login_required
def delete_archived_capture(partition_name, archive_id):
    try:
        changed = _service().delete_archived(partition_name, archive_id); flash("Archived durable message deleted." if changed else "Archived message was not found.", "success" if changed else "warning")
    except Exception as error: flash(f"Could not delete archived capture: {type(error).__name__}: {error}", "error")
    return redirect(url_for("durable_messages.index"))

@durable_messages_bp.post("/archive/<partition_name>/delete")
@login_required
def delete_archive_partition(partition_name):
    try:
        changed = _service().delete_archive_partition(partition_name); flash("Archived message partition deleted." if changed else "Archived partition was not found.", "success" if changed else "warning")
    except Exception as error: flash(f"Could not delete archived partition: {type(error).__name__}: {error}", "error")
    return redirect(url_for("durable_messages.index"))
