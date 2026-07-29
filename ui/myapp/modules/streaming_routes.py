"""Authenticated Streaming pages."""
from __future__ import annotations

from flask import Blueprint, current_app, flash, redirect, request, url_for

from ..services.streaming_service import StreamingError, StreamingService
from .common import login_required, render_dashboard

streaming_bp = Blueprint("streaming", __name__, url_prefix="/streaming")

def _service() -> StreamingService:
    return StreamingService(compartment_id=current_app.config["OCI_COMPARTMENT_ID"], region=current_app.config["OCI_REGION"], enabled=bool(current_app.config["OCI_STREAMING_MANAGEMENT_ENABLED"]))

@streaming_bp.get("/")
@login_required
def index():
    streams = []
    messages = []
    active_tab = request.args.get("tab", "server").strip().lower()
    if active_tab not in {"server", "content"}:
        active_tab = "server"
    selected_stream_id = request.args.get("stream_id", "").strip()
    try:
        streams = _service().list_streams()
        if active_tab == "content" and selected_stream_id:
            messages = _service().read_messages(stream_id=selected_stream_id)
    except StreamingError as error:
        flash(str(error), "error")
    return render_dashboard("streaming.html", active_page="streaming", streams=streams, messages=messages, selected_stream_id=selected_stream_id, active_tab=active_tab)

@streaming_bp.post("/create")
@login_required
def create():
    try:
        stream = _service().create_stream(name=request.form.get("name", ""), partitions=int(request.form.get("partitions", "1")), retention_hours=int(request.form.get("retention_hours", "24")))
        flash(f"Stream {stream.name} is being created.", "success")
        return redirect(url_for("streaming.index", tab="server"))
    except (ValueError, StreamingError) as error:
        flash(str(error), "error")
        return redirect(url_for("streaming.index"))

@streaming_bp.post("/test-message")
@login_required
def test_message():
    try:
        _service().publish_test_message(stream_id=request.form.get("stream_id", ""), payload=request.form.get("payload", "{}"))
        flash("Test message published.", "success")
    except (ValueError, StreamingError) as error:
        flash(str(error), "error")
    return redirect(url_for("streaming.index", tab="content", stream_id=request.form.get("stream_id", "")))
