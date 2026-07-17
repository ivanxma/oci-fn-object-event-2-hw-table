"""Authenticated Event TX reporting page."""

from __future__ import annotations

import csv
import io

from flask import Blueprint, Response, flash, request
from mysql.connector import Error as MySQLError

from ..services.event_tx_service import EventTransactionService
from ..services.naming import validate_identifier
from .common import login_required, mysql_for_request, render_dashboard


event_tx_bp = Blueprint("event_tx", __name__, url_prefix="/event-tx")


def _limit(value: str | None) -> int:
    try:
        limit = int(value or "10")
    except ValueError as error:
        raise ValueError("Recent TX limit must be a whole number.") from error
    if not 1 <= limit <= 500:
        raise ValueError("Recent TX limit must be between 1 and 500.")
    return limit


def _page(value: str | None) -> int:
    try:
        page = int(value or "1")
    except ValueError as error:
        raise ValueError("Object event page must be a whole number.") from error
    if page < 1:
        raise ValueError("Object event page must be at least 1.")
    return page


def _safe_csv_value(value):
    """Avoid spreadsheet formula evaluation when a CSV is opened locally."""
    value = "" if value is None else str(value)
    return f"'{value}" if value.startswith(("=", "+", "-", "@")) else value


@event_tx_bp.get("/")
@login_required
def list_event_transactions():
    try:
        service = EventTransactionService(mysql_for_request())
        tables, event_log_exists = service.registered_tables()
        limit = _limit(request.args.get("limit"))
        active_tab = request.args.get("tab", "recent")
        if active_tab not in {"recent", "registered", "object-events", "logs"}:
            raise ValueError("Unknown Event TX tab.")
        database, table = request.args.get("database", ""), request.args.get("table", "")
        if not database and tables:
            database, table = tables[0]["target_database"], tables[0]["target_table"]
        if database or table:
            if not database or not table:
                raise ValueError("Select both a target database and table.")
            database, table = validate_identifier(database, "target database"), validate_identifier(table, "target table")
            events = service.recent_events(database, table)
        else:
            events = []
        recent_events = service.recent_events_all(limit)
        audit_logs = service.audit_logs(limit)
        error_logs = service.error_logs(limit)
        object_event_tables = service.object_event_tables()
        object_databases = {item["database_name"] for item in object_event_tables}
        selected_object_database = request.args.get("object_database", "")
        if not selected_object_database and object_event_tables:
            selected_object_database = object_event_tables[0]["database_name"]
        if selected_object_database and selected_object_database not in object_databases:
            raise ValueError("Select an available object_event table.")
        object_page = _page(request.args.get("object_page"))
        object_page_size = 25
        if selected_object_database:
            object_event_columns, object_event_rows, object_event_total, object_event_sort, object_event_direction = service.object_event_page(
                selected_object_database,
                page=object_page,
                page_size=object_page_size,
                sort_column=request.args.get("object_sort"),
                sort_direction=request.args.get("object_direction", "desc"),
            )
            object_event_page_count = max(1, (object_event_total + object_page_size - 1) // object_page_size)
            if object_page > object_event_page_count:
                object_page = object_event_page_count
                object_event_columns, object_event_rows, object_event_total, object_event_sort, object_event_direction = service.object_event_page(
                    selected_object_database,
                    page=object_page,
                    page_size=object_page_size,
                    sort_column=object_event_sort,
                    sort_direction=object_event_direction,
                )
        else:
            object_event_columns, object_event_rows, object_event_total = [], [], 0
            object_event_sort, object_event_direction, object_event_page_count = "", "desc", 1
    except (MySQLError, ValueError):
        flash("Could not read Event TX records from fndb. Confirm this MySQL user can read the control tables.", "error")
        tables, event_log_exists, database, table, events, recent_events, audit_logs, error_logs, limit, active_tab = [], False, "", "", [], [], [], [], 10, "recent"
        object_event_tables, selected_object_database, object_event_columns, object_event_rows = [], "", [], []
        object_event_total, object_event_sort, object_event_direction, object_page, object_page_size, object_event_page_count = 0, "", "desc", 1, 25, 1
    return render_dashboard(
        "event_transactions.html",
        active_page="event_tx",
        registered_tables=tables,
        event_log_exists=event_log_exists,
        selected_database=database,
        selected_table=table,
        events=events,
        recent_events=recent_events,
        audit_logs=audit_logs,
        error_logs=error_logs,
        recent_limit=limit,
        active_tab=active_tab,
        object_event_tables=object_event_tables,
        selected_object_database=selected_object_database,
        object_event_columns=object_event_columns,
        object_event_rows=object_event_rows,
        object_event_total=object_event_total,
        object_event_sort=object_event_sort,
        object_event_direction=object_event_direction,
        object_event_page=object_page,
        object_event_page_size=object_page_size,
        object_event_page_count=object_event_page_count,
    )


@event_tx_bp.get("/object-events/download")
@login_required
def download_object_events():
    try:
        service = EventTransactionService(mysql_for_request())
        database = validate_identifier(request.args.get("object_database", ""), "object event database")
        allowed_databases = {item["database_name"] for item in service.object_event_tables()}
        if database not in allowed_databases:
            raise ValueError("Select an available object_event table.")
        columns, rows = service.object_event_export(
            database,
            sort_column=request.args.get("object_sort"),
            sort_direction=request.args.get("object_direction", "desc"),
        )
    except (MySQLError, ValueError):
        flash("Could not export object storage events. Confirm this MySQL user can read the object_event table.", "error")
        return render_dashboard("event_transactions.html", active_page="event_tx", registered_tables=[], event_log_exists=False,
                                selected_database="", selected_table="", events=[], recent_events=[], audit_logs=[], error_logs=[], recent_limit=10,
                                active_tab="object-events", object_event_tables=[], selected_object_database="",
                                object_event_columns=[], object_event_rows=[], object_event_total=0, object_event_sort="",
                                object_event_direction="desc", object_event_page=1, object_event_page_size=25,
                                object_event_page_count=1)
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(columns)
    writer.writerows([[_safe_csv_value(row.get(column)) for column in columns] for row in rows])
    filename = f"{database}_object_event.csv"
    return Response(
        buffer.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@event_tx_bp.get("/registered/download")
@login_required
def download_registered_events():
    try:
        database = validate_identifier(request.args.get("database", ""), "target database")
        table = validate_identifier(request.args.get("table", ""), "target table")
        rows = EventTransactionService(mysql_for_request()).recent_events(database, table)
    except (MySQLError, ValueError):
        flash("Could not export registered table transactions.", "error")
        return list_event_transactions()
    columns = ["id", "mapping_id", "batch_num", "event_action", "event_status", "bucket_name", "resource_name", "object_version", "message", "created_at"]
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(columns)
    writer.writerows([[_safe_csv_value(row.get(column)) for column in columns] for row in rows])
    return Response(buffer.getvalue(), mimetype="text/csv", headers={"Content-Disposition": f'attachment; filename="{database}_{table}_event_tx.csv"'})
