"""Authenticated queue dashboard and controlled recovery actions."""

from __future__ import annotations

import json

from flask import Blueprint, current_app, flash, redirect, request, url_for
from mysql.connector import Error as MySQLError

from ..services.mapping_service import MappingService
from ..services.queue_service import QueueService
from .common import connection_state, login_required, mysql_for_request, render_dashboard


queue_bp = Blueprint("queue", __name__, url_prefix="/queue")


def _service() -> QueueService:
    return QueueService(mysql_for_request())


def _username() -> str:
    state = connection_state()
    return state.username if state else "unknown"


def _ids() -> list[int]:
    values = request.form.getlist("queue_id")
    if not values:
        raise ValueError("Select at least one queue entry.")
    try:
        return [int(value) for value in values]
    except ValueError as error:
        raise ValueError("Queue selections must be numeric.") from error


def _wake(binding_key: str) -> None:
    function_id = str(current_app.config.get("OCI_FUNCTION_ID") or "")
    region = str(current_app.config.get("OCI_REGION") or "")
    if not function_id or not region:
        raise RuntimeError("OCI_FUNCTION_ID and OCI_REGION are required to wake a queue worker.")
    try:
        import oci

        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        management = oci.functions.FunctionsManagementClient({"region": region}, signer=signer)
        function = management.get_function(function_id).data
        endpoint = str(function.invoke_endpoint or "")
        if not endpoint:
            raise RuntimeError("OCI did not return the Function invoke endpoint.")
        client = oci.functions.FunctionsInvokeClient({"region": region}, signer=signer, service_endpoint=endpoint)
        client.invoke_function(
            function_id=function_id,
            invoke_function_body=json.dumps({"_queue_worker": True, "_queue_binding_key": binding_key}).encode(),
            fn_intent="cloudevent",
            fn_invoke_type="detached",
        )
    except RuntimeError:
        raise
    except Exception as error:
        raise RuntimeError(f"Could not wake the detached queue worker: {type(error).__name__}: {error}") from error


@queue_bp.get("/")
@login_required
def dashboard():
    filters = {name: request.args.get(name, "").strip() for name in ("status", "queue_scope", "binding_key", "resource_name")}
    try:
        counts, entries, lanes = _service().dashboard(filters)
        mappings = MappingService(mysql_for_request()).list_mappings()
    except (MySQLError, ValueError, RuntimeError) as error:
        flash(f"Could not read queue records: {type(error).__name__}: {error}", "error")
        counts, entries, lanes, mappings = {}, [], [], []
    return render_dashboard("queue_dashboard.html", active_page="queue", counts=counts, entries=entries, lanes=lanes, mappings=mappings, filters=filters)


@queue_bp.route("/new", methods=["GET", "POST"])
@login_required
def create_entry():
    service = _service()
    if request.method == "POST":
        try:
            queue_id, binding_key = service.create_manual(request.form, _username())
            if request.form.get("wake_worker"):
                _wake(binding_key)
            flash(f"Queue entry {queue_id} created.", "success")
            return redirect(url_for("queue.dashboard"))
        except (MySQLError, ValueError, RuntimeError) as error:
            flash(str(error), "error")
    try:
        mappings = MappingService(mysql_for_request()).list_mappings()
    except MySQLError as error:
        flash(str(error), "error")
        mappings = []
    return render_dashboard("queue_form.html", active_page="queue", form_mode="Create", entry=request.form.to_dict(), mappings=mappings)


@queue_bp.post("/edit-selected")
@login_required
def edit_selected():
    try:
        queue_ids = _ids()
        if len(queue_ids) != 1:
            raise ValueError("Select exactly one queue entry to edit.")
        return redirect(url_for("queue.edit_entry", queue_id=queue_ids[0]))
    except ValueError as error:
        flash(str(error), "warning")
        return redirect(url_for("queue.dashboard"))


@queue_bp.route("/<int:queue_id>/edit", methods=["GET", "POST"])
@login_required
def edit_entry(queue_id: int):
    service = _service()
    if request.method == "POST":
        try:
            service.edit_entry(queue_id, request.form, _username())
            flash(f"Queue entry {queue_id} updated.", "success")
            return redirect(url_for("queue.dashboard"))
        except (MySQLError, ValueError) as error:
            flash(str(error), "error")
    try:
        entry = service.get_entry(queue_id)
        if not entry:
            raise ValueError("Queue entry does not exist.")
    except (MySQLError, ValueError) as error:
        flash(str(error), "error")
        return redirect(url_for("queue.dashboard"))
    return render_dashboard("queue_form.html", active_page="queue", form_mode="Edit", entry=entry, mappings=[])


def _transition(action: str):
    try:
        changed, bindings = _service().transition(_ids(), action, _username(), request.form.get("reason", "Operator action."))
        wake_errors = []
        if action == "RETRY":
            for binding_key in bindings:
                try:
                    _wake(binding_key)
                except RuntimeError as error:
                    wake_errors.append(str(error))
        flash(f"{changed} queue entr{'y' if changed == 1 else 'ies'} updated.", "success" if changed else "warning")
        if wake_errors:
            flash(f"Retry entries are ready, but worker wake-up failed: {'; '.join(wake_errors)}", "error")
    except (MySQLError, ValueError) as error:
        flash(str(error), "error")
    return redirect(url_for("queue.dashboard"))


@queue_bp.post("/cancel-selected")
@login_required
def cancel_selected():
    return _transition("CANCEL")


@queue_bp.post("/retry-selected")
@login_required
def retry_selected():
    return _transition("RETRY")


@queue_bp.post("/wake")
@login_required
def wake():
    binding_key = request.form.get("binding_key", "").strip()
    if not binding_key or len(binding_key) > 191:
        flash("Select a valid queue binding to wake.", "error")
    else:
        try:
            _wake(binding_key)
            flash(f"Detached worker wake-up submitted for {binding_key}.", "success")
        except RuntimeError as error:
            flash(str(error), "error")
    return redirect(url_for("queue.dashboard", binding_key=binding_key))
