"""Authenticated maintenance page for Object Storage import mappings."""

from __future__ import annotations

from mysql.connector import Error as MySQLError
from flask import Blueprint, flash, jsonify, redirect, request, url_for

from ..services.mapping_service import MappingService
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


def _form_context(mapping: dict | None = None) -> dict:
    mapping = mapping or {}
    service = _service()
    databases = service.list_target_databases()
    database = mapping.get("target_database", "")
    tables = service.list_target_tables(database) if database in databases else []
    return {"mapping": mapping, "target_databases": databases, "target_tables": tables}


@mappings_bp.get("/")
@login_required
def list_mappings():
    try:
        mappings = _service().list_mappings()
    except MySQLError:
        flash("Could not read mappings from fndb. Confirm this MySQL user can create and use that database.", "error")
        mappings = []
    return render_dashboard("mappings.html", active_page="mappings", mappings=mappings)


@mappings_bp.route("/new", methods=["GET", "POST"])
@login_required
def create_mapping():
    if request.method == "POST":
        try:
            _service().add_mapping(MappingService.normalize(request.form))
            flash("Mapping added to fndb.", "success")
            return redirect(url_for("mappings.list_mappings"))
        except (ValueError, MySQLError) as error:
            flash(_message(error), "error")
    try:
        return render_dashboard("mapping_form.html", active_page="mappings", form_mode="Add", **_form_context(request.form.to_dict()))
    except MySQLError as error:
        flash(_message(error), "error")
        return redirect(url_for("mappings.list_mappings"))


@mappings_bp.route("/<mapping_id>/edit", methods=["GET", "POST"])
@login_required
def edit_mapping(mapping_id: str):
    try:
        mapping_key = _mapping_id(mapping_id)
    except ValueError as error:
        flash(str(error), "error")
        return redirect(url_for("mappings.list_mappings"))
    if request.method == "POST":
        try:
            if not _service().update_mapping(mapping_key, MappingService.normalize(request.form)):
                flash("That mapping no longer exists.", "warning")
            else:
                flash("Mapping updated.", "success")
            return redirect(url_for("mappings.list_mappings"))
        except (ValueError, MySQLError) as error:
            flash(_message(error), "error")
            return render_dashboard("mapping_form.html", active_page="mappings", mapping_id=mapping_key, form_mode="Edit", **_form_context(request.form.to_dict()))
    try:
        mapping = _service().get_mapping(mapping_key)
    except MySQLError as error:
        flash(_message(error), "error")
        return redirect(url_for("mappings.list_mappings"))
    if not mapping:
        flash("That mapping does not exist.", "warning")
        return redirect(url_for("mappings.list_mappings"))
    return render_dashboard("mapping_form.html", active_page="mappings", mapping_id=mapping_key, form_mode="Edit", **_form_context(mapping))


@mappings_bp.post("/<mapping_id>/delete")
@login_required
def delete_mapping(mapping_id: str):
    try:
        if _service().delete_mapping(_mapping_id(mapping_id)):
            flash("Mapping deleted.", "success")
        else:
            flash("That mapping no longer exists.", "warning")
    except (ValueError, MySQLError) as error:
        flash(_message(error), "error")
    return redirect(url_for("mappings.list_mappings"))


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
        return "Could not save the mapping in fndb. Confirm this MySQL user can create and use that database."
    return str(error)
