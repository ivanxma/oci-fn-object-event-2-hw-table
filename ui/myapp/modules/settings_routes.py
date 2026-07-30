"""Authenticated UI and database configuration settings."""

from __future__ import annotations

import os
import re
from pathlib import Path

from flask import Blueprint, current_app, flash, redirect, request, url_for

from ..services.mapping_service import MappingService
from ..services.naming import quote_identifier, validate_identifier
from .common import connection_state, login_required, mysql_for_request, render_dashboard

settings_bp = Blueprint("settings", __name__, url_prefix="/settings")
ROOT = Path(__file__).resolve().parents[3]
ACCOUNT = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,31}$")


def _databases(form) -> dict[str, str]:
    values = {
        "LOADER_DATABASE": validate_identifier(form.get("loader_database", ""), "loader database"),
        "CONTROL_DATABASE": validate_identifier(form.get("control_database", ""), "control database"),
        "STREAM_DATA_DB_NAME": validate_identifier(form.get("stream_data_database", ""), "stream data database"),
        "STREAM_USER": (form.get("stream_user", "") or "").strip(),
    }
    if not ACCOUNT.fullmatch(values["STREAM_USER"]):
        raise ValueError("Stream user must start with a letter and contain only letters, numbers, or underscores (32 characters maximum).")
    if len({values["LOADER_DATABASE"], values["CONTROL_DATABASE"], values["STREAM_DATA_DB_NAME"]}) < 3:
        raise ValueError("Loader, control, and stream data databases must be separate.")
    return values


def _apply(values: dict[str, str]) -> None:
    store = current_app.extensions["profile_store"]
    store.set_ui_configuration(values)
    for key, value in values.items():
        current_app.config[key] = value
        os.environ[key] = value
    # DB_NAME is the legacy processor/loader alias.
    current_app.config["DB_NAME"] = values["LOADER_DATABASE"]
    os.environ["DB_NAME"] = values["LOADER_DATABASE"]


def _split_sql(path: Path, replacement: dict[str, str] | None = None) -> list[str]:
    text = path.read_text(encoding="utf-8")
    for key, value in (replacement or {}).items():
        text = text.replace(key, value)
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("--")]
    return [item.strip() for item in "\n".join(lines).split(";") if item.strip()]


def _initialize(mysql, values: dict[str, str]) -> None:
    with mysql.connection() as connection:
        cursor = connection.cursor()
        for database in (values["LOADER_DATABASE"], values["CONTROL_DATABASE"], values["STREAM_DATA_DB_NAME"]):
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(database, 'database')} CHARACTER SET utf8mb4")
        control = quote_identifier(values["CONTROL_DATABASE"], "control database")
        for statement in _split_sql(ROOT / "loader_core/sql/init_control_schema.sql", {"__CONTROL_DATABASE__": control}):
            cursor.execute(statement)
        # MappingService owns its external mapping migrations and retired-object cleanup.
        os.environ["CONTROL_DATABASE"] = values["CONTROL_DATABASE"]
        MappingService(mysql)._ensure_schema(cursor)
        durable = quote_identifier(values["STREAM_DATA_DB_NAME"], "stream data database")
        cursor.execute(f"USE {durable}")
        for statement in _split_sql(ROOT / "processor/sql/init_stream_capture.sql"):
            cursor.execute(statement)
        for path in (ROOT / "processor/sql/migrate_stream_capture_retry.sql", ROOT / "processor/sql/migrate_stream_capture_processing.sql"):
            for statement in _split_sql(path):
                try:
                    cursor.execute(statement)
                except Exception as error:
                    if "duplicate column" not in str(error).lower():
                        raise
        archive = f"{durable}.`stream_message_archive_partitions`"
        for statement in _split_sql(ROOT / "ui/myapp/sql/init_stream_message_archive.sql", {"__ARCHIVE_REGISTRY__": archive}):
            cursor.execute(statement)


def _create_stream_user(mysql, values: dict[str, str], password: str, targets: list[str]) -> None:
    if not password:
        raise ValueError("Stream user password is required for user creation or rotation.")
    databases = {values["LOADER_DATABASE"], values["CONTROL_DATABASE"], values["STREAM_DATA_DB_NAME"], *targets}
    with mysql.connection() as connection:
        cursor = connection.cursor()
        account = "'" + values["STREAM_USER"].replace("\\", "\\\\").replace("'", "\\'") + "'@'%'"
        cursor.execute(f"CREATE USER IF NOT EXISTS {account} IDENTIFIED BY %s", (password,))
        cursor.execute(f"ALTER USER {account} IDENTIFIED BY %s", (password,))
        for database in databases:
            cursor.execute(f"GRANT ALL PRIVILEGES ON {quote_identifier(database, 'database')}.* TO {account}")
        cursor.execute(f"GRANT ALL PRIVILEGES ON {quote_identifier(values['STREAM_DATA_DB_NAME'], 'stream data database')}.* TO {account}")
        cursor.execute("FLUSH PRIVILEGES")


@settings_bp.route("/", methods=["GET", "POST"])
@login_required
def manage():
    values = {
        "LOADER_DATABASE": current_app.config.get("LOADER_DATABASE") or os.environ.get("DB_NAME", "loader_db"),
        "CONTROL_DATABASE": current_app.config.get("CONTROL_DATABASE") or os.environ.get("CONTROL_DATABASE", "stream_db"),
        "STREAM_DATA_DB_NAME": current_app.config.get("STREAM_DATA_DB_NAME") or os.environ.get("STREAM_DATA_DB_NAME", "stream_data"),
        "STREAM_USER": current_app.config.get("STREAM_USER") or os.environ.get("STREAM_USER", "streamuser"),
    }
    if request.method == "POST":
        try:
            action = request.form.get("action", "save")
            values = _databases(request.form)
            _apply(values)
            if action == "initialize":
                _initialize(mysql_for_request(), values)
                flash("Database structures initialized from the repository SQL files.", "success")
            elif action == "create_stream_user":
                targets = [validate_identifier(item.strip(), "target database") for item in (request.form.get("target_databases", "")).split(",") if item.strip()]
                _create_stream_user(mysql_for_request(), values, request.form.get("stream_password", ""), targets)
                flash(f"Stream user {values['STREAM_USER']} created/updated and granted access.", "success")
            else:
                flash("UI database configuration saved.", "success")
        except (OSError, ValueError) as error:
            flash(str(error), "error")
    return render_dashboard("settings.html", active_page="settings", settings=values)
