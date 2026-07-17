"""CSV inspection, schema review, and LOAD DATA routes."""

from __future__ import annotations

import uuid
from pathlib import Path

from flask import Blueprint, current_app, flash, redirect, request, url_for
from werkzeug.utils import secure_filename

from ..services.csv_service import inspect_csv
from ..services.import_service import ImportExecutionError, ImportService
from ..services.naming import table_name_from_filename, validate_identifier
from .common import connection_state, login_required, mysql_for_request, render_dashboard

import_bp = Blueprint("imports", __name__, url_prefix="/imports")


@import_bp.get("/")
@login_required
def home():
    return render_dashboard("import_start.html", active_page="import", databases=mysql_for_request().list_databases())


@import_bp.post("/inspect")
@login_required
def inspect():
    upload = request.files.get("csv_file")
    if not upload or not upload.filename.lower().endswith(".csv"):
        flash("Choose a CSV file.", "error")
        return redirect(url_for("imports.home"))
    try:
        filename = secure_filename(upload.filename)
        create_database = request.form.get("create_database") == "on"
        database_field = "new_database" if create_database else "existing_database"
        database = validate_identifier(request.form.get(database_field, ""), "database name")
        table = validate_identifier(request.form.get("table", "") or table_name_from_filename(filename), "table name")
        path = Path(current_app.config["UPLOAD_FOLDER"]) / f"{uuid.uuid4()}-{filename}"
        upload.save(path)
        result = inspect_csv(path)
        job_id = uuid.uuid4().hex
        connection_state().imports[job_id] = {"path": str(path), "database": database, "table": table, "create_database": create_database, **result}
        return redirect(url_for("imports.review", job_id=job_id))
    except (ValueError, UnicodeDecodeError) as error:
        flash(str(error), "error")
        return redirect(url_for("imports.home"))


@import_bp.get("/<job_id>/review")
@login_required
def review(job_id: str):
    job = connection_state().imports.get(job_id)
    if not job:
        flash("That import review has expired. Upload the CSV again.", "warning")
        return redirect(url_for("imports.home"))
    columns = [{"source_name": name, "name": name, "type": job["types"][name], "nullable": True} for name in job["headers"]]
    return render_dashboard("import_review.html", active_page="import", job=job, job_id=job_id, columns=columns)


def reviewed_definition(job: dict) -> tuple[list[dict], list[str], bool, bool]:
    """Validate and normalize the reviewed form without executing database SQL."""
    columns = []
    for index, source_name in enumerate(job["headers"]):
        columns.append({"source_name": source_name, "name": validate_identifier(request.form.get(f"column_name_{index}", ""), "column name"), "type": request.form.get(f"column_type_{index}", ""), "nullable": request.form.get(f"nullable_{index}") == "on"})
    names = [column["name"] for column in columns]
    if len(set(names)) != len(names):
        raise ValueError("Column names must be unique.")
    primary_key = []
    for index in request.form.getlist("primary_key_index"):
        if not index.isdigit() or int(index) >= len(names):
            raise ValueError("Primary-key selection is invalid.")
        primary_key.append(names[int(index)])
    partition_by_batch = request.form.get("partition_by_batch") == "on"
    add_row_id = request.form.get("add_row_id") == "on" or (partition_by_batch and not primary_key)
    return columns, primary_key, add_row_id, partition_by_batch


@import_bp.post("/<job_id>/prepare")
@login_required
def prepare(job_id: str):
    state = connection_state()
    job = state.imports.get(job_id)
    if not job:
        flash("That import review has expired. Upload the CSV again.", "warning")
        return redirect(url_for("imports.home"))
    try:
        columns, primary_key, add_row_id, partition_by_batch = reviewed_definition(job)
        importer = ImportService(mysql_for_request())
        job["reviewed"] = {"columns": columns, "primary_key": primary_key, "add_row_id": add_row_id, "partition_by_batch": partition_by_batch}
        return render_dashboard("sql_preview.html", active_page="import", job=job, job_id=job_id, ddl=importer.ddl(job["database"], job["table"], columns, primary_key, add_row_id, partition_by_batch), create_database_statement=importer.create_database_statement(job["database"]) if job["create_database"] else None, load_statement=importer.load_data_statement(job["database"], job["table"], columns, job["delimiter"], partition_by_batch))
    except ValueError as error:
        flash(str(error), "error")
        return redirect(url_for("imports.review", job_id=job_id))


@import_bp.post("/<job_id>/load")
@login_required
def load(job_id: str):
    state = connection_state()
    job = state.imports.get(job_id)
    review = job.get("reviewed") if job else None
    if not job or not review:
        flash("Review the generated SQL before running an import.", "warning")
        return redirect(url_for("imports.review", job_id=job_id))
    try:
        row_count = ImportService(mysql_for_request()).load_data(Path(job["path"]), job["database"], job["table"], review["columns"], review["primary_key"], review["add_row_id"], job["delimiter"], partition_by_batch=review.get("partition_by_batch", False), create_database=job["create_database"])
        Path(job["path"]).unlink(missing_ok=True)
        state.imports.pop(job_id, None)
        flash(f"Loaded {row_count} row(s) into {job['database']}.{job['table']} using LOAD DATA LOCAL INFILE.", "success")
    except ImportExecutionError as error:
        current_app.logger.exception("CSV LOAD DATA import failed")
        flash(str(error), "error")
        return redirect(url_for("imports.review", job_id=job_id))
    except ValueError as error:
        flash(str(error), "error")
        return redirect(url_for("imports.review", job_id=job_id))
    except Exception:
        current_app.logger.exception("CSV LOAD DATA import failed")
        flash("Import failed unexpectedly. Check the application logs.", "error")
        return redirect(url_for("imports.review", job_id=job_id))
    return redirect(url_for("imports.home"))
