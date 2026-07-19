"""Authenticated maintenance pages for mappings, OCI rules, and Function capacity."""

from __future__ import annotations

from flask import Blueprint, current_app, flash, jsonify, redirect, request, url_for
from mysql.connector import Error as MySQLError

from ..services.event_rule_service import EventRuleError, EventRuleService
from ..services.function_configuration_service import (
    FunctionConfigurationError,
    FunctionConfigurationService,
)
from ..services.mapping_service import MappingService
from ..services.object_storage_upload_service import (
    ObjectStorageUploadError,
    ObjectStorageUploadService,
    default_folder,
)
from .common import login_required, mysql_for_request, render_dashboard


mappings_bp = Blueprint("mappings", __name__, url_prefix="/mappings")


def _mapping_id(value: str) -> int:
    try:
        mapping_id = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError("The mapping identifier is invalid.") from error
    if mapping_id < 1:
        raise ValueError("The mapping identifier is invalid.")
    return mapping_id


def _service() -> MappingService:
    return MappingService(mysql_for_request())


def _rule_service() -> EventRuleService:
    return EventRuleService(
        function_id=current_app.config["OCI_FUNCTION_ID"],
        compartment_id=current_app.config["OCI_COMPARTMENT_ID"],
        region=current_app.config["OCI_REGION"],
        enabled=bool(current_app.config["OCI_EVENT_RULE_MANAGEMENT_ENABLED"]),
        rule_prefix=current_app.config["OCI_EVENT_RULE_PREFIX"],
    )


def _function_service() -> FunctionConfigurationService:
    return FunctionConfigurationService(
        function_id=current_app.config["OCI_FUNCTION_ID"],
        region=current_app.config["OCI_REGION"],
    )


def _object_storage_service() -> ObjectStorageUploadService:
    return ObjectStorageUploadService(
        region=current_app.config["OCI_REGION"],
        compartment_id=current_app.config["OCI_COMPARTMENT_ID"],
        namespace=current_app.config["OCI_OBJECT_STORAGE_NAMESPACE"],
    )


def _form_context(mapping: dict | None = None) -> dict:
    mapping = mapping or {}
    service = _service()
    databases = service.list_target_databases()
    database = mapping.get("target_database", "")
    tables = service.list_target_tables(database) if database in databases else []
    return {"mapping": mapping, "target_databases": databases, "target_tables": tables}


def _rule_requested() -> bool:
    return request.form.get("create_rule") == "on"


def _selected_rule_ids() -> list[str]:
    rule_ids = list(dict.fromkeys(value.strip() for value in request.form.getlist("rule_id") if value.strip()))
    if not rule_ids:
        raise ValueError("Select at least one OCI Events rule.")
    if len(rule_ids) > 100:
        raise ValueError("Select no more than 100 OCI Events rules at one time.")
    if any(not value.startswith("ocid1.eventrule.") for value in rule_ids):
        raise ValueError("One or more selected OCI Events rule identifiers are invalid.")
    return rule_ids


def _selected_mapping_ids() -> list[int]:
    raw_ids = list(dict.fromkeys(value.strip() for value in request.form.getlist("mapping_id") if value.strip()))
    if not raw_ids:
        raise ValueError("Select at least one mapping.")
    if len(raw_ids) > 100:
        raise ValueError("Select no more than 100 mappings at one time.")
    return [_mapping_id(value) for value in raw_ids]


def _validate_rule_request(service: MappingService, values: dict[str, str], *, exclude: int | None = None) -> None:
    pattern = values["resource_name_pattern"]
    if any(character in pattern for character in "?[]"):
        raise ValueError(
            "OCI rule creation supports literal characters and * wildcards only; ?, [ and ] are mapping-only syntax."
        )
    conflict = service.exact_pattern_conflict(values, exclude_mapping_id=exclude)
    if conflict is not None:
        raise ValueError(
            f"Mapping {conflict} already uses this exact compartment, bucket, and object pattern. "
            "OCI rule scopes must be mutually exclusive."
        )


def _reconcile_rule(
    service: MappingService,
    *,
    mapping_id: int,
    values: dict[str, str],
    existing_rule_id: str | None,
    requested: bool,
) -> None:
    if requested:
        rule = _rule_service().ensure_mapping_rule(
            mapping_id=mapping_id,
            mapping=values,
            existing_rule_id=existing_rule_id,
        )
        service.set_event_rule(mapping_id, rule.id)
        flash(f"OCI Events rule {rule.display_name} is enabled for this mapping.", "success")
    elif existing_rule_id:
        _rule_service().delete_function_rule(existing_rule_id)
        service.set_event_rule(mapping_id, None)
        flash("The managed OCI Events rule was deleted; the mapping remains available.", "success")


@mappings_bp.get("/")
@login_required
def list_mappings():
    active_tab = request.args.get("tab", "mappings")
    if active_tab not in {"mappings", "rules", "function", "upload"}:
        active_tab = "mappings"
    mappings: list[dict] = []
    rules = []
    function_configuration = None
    selected_upload_mapping = None
    upload_objects = []
    try:
        mappings = _service().list_mappings()
    except MySQLError as error:
        current_app.logger.exception("Could not read resource mappings")
        flash(f"Could not read mappings from {current_app.config['CONTROL_DATABASE']}: {type(error).__name__}: {error}", "error")

    if active_tab == "rules":
        try:
            rules = _rule_service().list_function_rules()
        except EventRuleError as error:
            current_app.logger.exception("Could not read OCI Events rules")
            flash(str(error), "error")
    elif active_tab == "function":
        if not current_app.config["OCI_FUNCTION_CONFIGURATION_ENABLED"]:
            flash("OCI Function configuration management is disabled for this UI deployment.", "warning")
        else:
            try:
                function_configuration = _function_service().get()
            except FunctionConfigurationError as error:
                current_app.logger.exception("Could not read OCI Function configuration")
                flash(str(error), "error")
    elif active_tab == "upload":
        try:
            selected_id = request.args.get("mapping_id", "").strip()
            if selected_id:
                selected_key = _mapping_id(selected_id)
                selected_upload_mapping = next(
                    (mapping for mapping in mappings if int(mapping["id"]) == selected_key), None
                )
                if selected_upload_mapping is None:
                    raise ValueError("The selected upload mapping does not exist.")
                upload_objects = _object_storage_service().list_mapping_objects(selected_upload_mapping)
        except (ValueError, ObjectStorageUploadError) as error:
            current_app.logger.exception("Could not load mapping-scoped Object Storage files")
            flash(str(error), "error")

    return render_dashboard(
        "mappings.html",
        active_page="mappings",
        active_tab=active_tab,
        mappings=mappings,
        mapping_lookup={int(mapping["id"]): mapping for mapping in mappings},
        rule_mapping_lookup={
            rule.id: next(
                (
                    mapping
                    for mapping in mappings
                    if mapping.get("event_rule_id") == rule.id
                    or (rule.mapping_id is not None and int(mapping["id"]) == rule.mapping_id)
                ),
                None,
            )
            for rule in rules
        },
        rules=rules,
        function_configuration=function_configuration,
        selected_upload_mapping=selected_upload_mapping,
        upload_objects=upload_objects,
        upload_folders={int(mapping["id"]): default_folder(mapping["resource_name_pattern"]) for mapping in mappings},
    )


@mappings_bp.route("/new", methods=["GET", "POST"])
@login_required
def create_mapping():
    if request.method == "POST":
        try:
            service = _service()
            values = MappingService.normalize(request.form)
            requested = _rule_requested()
            if requested:
                _validate_rule_request(service, values)
            mapping_key = service.add_mapping(values)
            flash("Mapping added to the control database.", "success")
            try:
                _reconcile_rule(
                    service,
                    mapping_id=mapping_key,
                    values=values,
                    existing_rule_id=None,
                    requested=requested,
                )
            except EventRuleError as error:
                current_app.logger.exception("Mapping saved but OCI rule creation failed")
                flash(f"Mapping {mapping_key} was saved, but its OCI rule was not created. {error}", "error")
            return redirect(url_for("mappings.list_mappings"))
        except (ValueError, MySQLError) as error:
            flash(_message(error), "error")
    try:
        mapping = request.form.to_dict()
        mapping["create_rule"] = _rule_requested()
        return render_dashboard(
            "mapping_form.html", active_page="mappings", form_mode="Add", **_form_context(mapping)
        )
    except MySQLError as error:
        flash(_message(error), "error")
        return redirect(url_for("mappings.list_mappings"))


@mappings_bp.route("/<mapping_id>/edit", methods=["GET", "POST"])
@login_required
def edit_mapping(mapping_id: str):
    try:
        mapping_key = _mapping_id(mapping_id)
        service = _service()
        existing = service.get_mapping(mapping_key)
    except (ValueError, MySQLError) as error:
        flash(_message(error), "error")
        return redirect(url_for("mappings.list_mappings"))
    if not existing:
        flash("That mapping does not exist.", "warning")
        return redirect(url_for("mappings.list_mappings"))

    if request.method == "POST":
        try:
            values = MappingService.normalize(request.form)
            if service.has_nonterminal_queue_work(existing, values["queue_scope"]):
                raise ValueError("Queue binding cannot be changed while the current or destination binding has non-terminal work.")
            requested = _rule_requested()
            if requested:
                _validate_rule_request(service, values, exclude=mapping_key)
            if not service.update_mapping(mapping_key, values):
                flash("That mapping no longer exists.", "warning")
            else:
                flash("Mapping updated.", "success")
                try:
                    _reconcile_rule(
                        service,
                        mapping_id=mapping_key,
                        values=values,
                        existing_rule_id=existing.get("event_rule_id"),
                        requested=requested,
                    )
                except EventRuleError as error:
                    current_app.logger.exception("Mapping saved but OCI rule reconciliation failed")
                    flash(f"Mapping {mapping_key} was saved, but its OCI rule was not reconciled. {error}", "error")
            return redirect(url_for("mappings.list_mappings"))
        except (ValueError, MySQLError) as error:
            flash(_message(error), "error")
            mapping = request.form.to_dict()
            mapping["create_rule"] = _rule_requested()
            mapping["event_rule_id"] = existing.get("event_rule_id")
            return render_dashboard(
                "mapping_form.html",
                active_page="mappings",
                mapping_id=mapping_key,
                form_mode="Edit",
                **_form_context(mapping),
            )
    existing["create_rule"] = bool(existing.get("event_rule_id"))
    return render_dashboard(
        "mapping_form.html",
        active_page="mappings",
        mapping_id=mapping_key,
        form_mode="Edit",
        **_form_context(existing),
    )


@mappings_bp.post("/<mapping_id>/delete")
@login_required
def delete_mapping(mapping_id: str):
    try:
        service = _service()
        mapping_key = _mapping_id(mapping_id)
        mapping = service.get_mapping(mapping_key)
        if not mapping:
            flash("That mapping no longer exists.", "warning")
        else:
            if service.mapping_has_nonterminal_queue_work(mapping_key):
                raise ValueError("Mapping cannot be deleted while it has non-terminal queue work. Cancel or complete those entries first.")
            rule_id = mapping.get("event_rule_id")
            if rule_id:
                _rule_service().delete_function_rule(rule_id)
            if service.delete_mapping(mapping_key):
                flash("Mapping and its managed OCI Events rule were deleted.", "success")
    except (ValueError, MySQLError, EventRuleError) as error:
        current_app.logger.exception("Could not delete mapping and managed rule")
        flash(_message(error), "error")
    return redirect(url_for("mappings.list_mappings"))


@mappings_bp.post("/edit-selected")
@login_required
def edit_selected_mapping():
    try:
        mapping_ids = _selected_mapping_ids()
        if len(mapping_ids) != 1:
            raise ValueError("Select exactly one mapping to edit.")
        if _service().get_mapping(mapping_ids[0]) is None:
            raise ValueError("The selected mapping no longer exists.")
        return redirect(url_for("mappings.edit_mapping", mapping_id=mapping_ids[0]))
    except (ValueError, MySQLError) as error:
        flash(_message(error), "error")
        return redirect(url_for("mappings.list_mappings", tab="mappings"))


@mappings_bp.post("/delete-selected")
@login_required
def delete_selected_mappings():
    try:
        mapping_ids = _selected_mapping_ids()
        service = _service()
        deleted = 0
        deleted_rules = 0
        failures: list[str] = []
        for mapping_id in mapping_ids:
            try:
                mapping = service.get_mapping(mapping_id)
                if mapping is None:
                    raise ValueError(f"Mapping {mapping_id} no longer exists.")
                if service.mapping_has_nonterminal_queue_work(mapping_id):
                    raise ValueError(f"Mapping {mapping_id} has non-terminal queue work. Cancel or complete it first.")
                rule_id = mapping.get("event_rule_id")
                if rule_id:
                    _rule_service().delete_function_rule(rule_id)
                    deleted_rules += 1
                if service.delete_mapping(mapping_id):
                    deleted += 1
            except (ValueError, EventRuleError, MySQLError) as error:
                failures.append(str(error))
        if deleted:
            flash(f"Deleted {deleted} mapping(s) and {deleted_rules} managed OCI Events rule(s).", "success")
        if failures:
            flash(f"{len(failures)} mapping(s) could not be deleted. {'; '.join(failures)}", "error")
    except ValueError as error:
        flash(str(error), "error")
    return redirect(url_for("mappings.list_mappings", tab="mappings"))


@mappings_bp.post("/rules/<path:rule_id>/delete")
@login_required
def delete_rule(rule_id: str):
    try:
        _rule_service().delete_function_rule(rule_id)
        cleared = _service().clear_event_rule_reference(rule_id)
        flash(
            f"OCI Events rule deleted. {cleared} mapping association(s) cleared; mappings were retained.",
            "success",
        )
    except (EventRuleError, MySQLError) as error:
        current_app.logger.exception("Could not delete OCI Events rule")
        flash(_message(error), "error")
    return redirect(url_for("mappings.list_mappings", tab="rules"))


@mappings_bp.post("/rules/edit-selected")
@login_required
def edit_selected_rule():
    try:
        rule_ids = _selected_rule_ids()
        if len(rule_ids) != 1:
            raise ValueError("Select exactly one rule to edit.")
        rule = _rule_service().get_function_rule(rule_ids[0])
        service = _service()
        mapping = service.get_mapping_by_rule_id(rule.id)
        if mapping is None and rule.mapping_id is not None:
            mapping = service.get_mapping(rule.mapping_id)
        if mapping is None:
            raise ValueError(
                "The selected OCI rule is not associated with a current mapping. "
                "Create or associate a mapping before editing it."
            )
        return redirect(url_for("mappings.edit_mapping", mapping_id=mapping["id"]))
    except (ValueError, EventRuleError, MySQLError) as error:
        current_app.logger.exception("Could not edit selected OCI Events rule")
        flash(_message(error), "error")
        return redirect(url_for("mappings.list_mappings", tab="rules"))


@mappings_bp.post("/rules/disable-selected")
@login_required
def disable_selected_rules():
    try:
        rule_ids = _selected_rule_ids()
        failures: list[str] = []
        disabled = 0
        for rule_id in rule_ids:
            try:
                _rule_service().set_rule_enabled(rule_id, enabled=False)
                disabled += 1
            except EventRuleError as error:
                failures.append(str(error))
        if disabled:
            flash(f"Disabled {disabled} OCI Events rule(s).", "success")
        if failures:
            flash(f"{len(failures)} rule(s) could not be disabled. {'; '.join(failures)}", "error")
    except ValueError as error:
        flash(str(error), "error")
    return redirect(url_for("mappings.list_mappings", tab="rules"))


@mappings_bp.post("/rules/delete-selected")
@login_required
def delete_selected_rules():
    try:
        rule_ids = _selected_rule_ids()
        service = _service()
        failures: list[str] = []
        deleted = 0
        cleared = 0
        for rule_id in rule_ids:
            try:
                _rule_service().delete_function_rule(rule_id)
                cleared += service.clear_event_rule_reference(rule_id)
                deleted += 1
            except (EventRuleError, MySQLError) as error:
                failures.append(str(error))
        if deleted:
            flash(f"Deleted {deleted} OCI Events rule(s) and cleared {cleared} mapping association(s).", "success")
        if failures:
            flash(f"{len(failures)} rule(s) could not be deleted. {'; '.join(failures)}", "error")
    except ValueError as error:
        flash(str(error), "error")
    return redirect(url_for("mappings.list_mappings", tab="rules"))


@mappings_bp.post("/upload")
@login_required
def upload_mapping_csv():
    mapping_key = None
    try:
        mapping_key = _mapping_id(request.form.get("mapping_id", ""))
        mapping = _service().get_mapping(mapping_key)
        if mapping is None:
            raise ValueError("The selected upload mapping does not exist.")
        csv_file = request.files.get("csv_file")
        if csv_file is None or not csv_file.filename:
            raise ValueError("Choose a CSV file to upload.")
        object_name = _object_storage_service().upload_csv(
            mapping=mapping,
            folder=request.form.get("folder", ""),
            filename=csv_file.filename,
            stream=csv_file.stream,
        )
        flash(
            f"Uploaded {object_name} to bucket {mapping['bucket_name']}. A matching enabled OCI rule can now invoke the Function.",
            "success",
        )
    except (ValueError, MySQLError, ObjectStorageUploadError) as error:
        current_app.logger.exception("Could not upload mapping-scoped CSV")
        flash(_message(error), "error")
    return redirect(url_for("mappings.list_mappings", tab="upload", mapping_id=mapping_key or ""))


@mappings_bp.post("/upload/delete-selected")
@login_required
def delete_mapping_objects():
    mapping_key = None
    try:
        mapping_key = _mapping_id(request.form.get("mapping_id", ""))
        mapping = _service().get_mapping(mapping_key)
        if mapping is None:
            raise ValueError("The selected upload mapping does not exist.")
        deleted = _object_storage_service().delete_objects(
            mapping=mapping,
            object_names=request.form.getlist("object_name"),
        )
        flash(f"Deleted {deleted} matching Object Storage file(s) from {mapping['bucket_name']}.", "success")
    except (ValueError, MySQLError, ObjectStorageUploadError) as error:
        current_app.logger.exception("Could not delete mapping-scoped Object Storage files")
        flash(_message(error), "error")
    return redirect(url_for("mappings.list_mappings", tab="upload", mapping_id=mapping_key or ""))


@mappings_bp.post("/function")
@login_required
def update_function_configuration():
    try:
        if not current_app.config["OCI_FUNCTION_CONFIGURATION_ENABLED"]:
            raise FunctionConfigurationError("OCI Function configuration management is disabled.")
        configuration = _function_service().update(request.form)
        flash(
            f"OCI Function {configuration.display_name} configuration updated: "
            f"Sync {configuration.sync_timeout_seconds}s, Detached {configuration.detached_timeout_seconds}s, "
            f"Memory {configuration.memory_in_mbs} MB.",
            "success",
        )
    except (ValueError, FunctionConfigurationError) as error:
        current_app.logger.exception("Could not update OCI Function configuration")
        flash(str(error), "error")
    return redirect(url_for("mappings.list_mappings", tab="function"))


@mappings_bp.get("/tables")
@login_required
def target_tables():
    try:
        database = request.args.get("database", "")
        return jsonify(tables=_service().list_target_tables(database))
    except (ValueError, MySQLError):
        return jsonify(tables=[]), 400


def _message(error: Exception) -> str:
    if isinstance(error, MySQLError):
        return (
            f"Control database operation failed: {type(error).__name__}: {error}. "
            "Confirm this MySQL user can create and use the configured control database."
        )
    return str(error)
