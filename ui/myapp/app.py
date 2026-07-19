"""Application factory for the CSV-to-MySQL import console."""

from __future__ import annotations

import os
from pathlib import Path

from flask import Flask

from .modules.auth_routes import auth_bp
from .modules.event_tx_routes import event_tx_bp
from .modules.import_routes import import_bp
from .modules.mapping_routes import mappings_bp
from .modules.profile_routes import profile_bp
from .services.profile_store import ProfileStore
from .services.session_store import SessionStore


def create_app(test_config: dict | None = None) -> Flask:
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY=os.environ.get("FLASK_SECRET_KEY", os.urandom(32)),
        SESSION_COOKIE_NAME="csv_import_session",
        SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE", "0").lower() in {"1", "true", "yes"},
        SESSION_COOKIE_SAMESITE="Lax",
        UPLOAD_FOLDER=os.environ.get("UPLOAD_FOLDER", str(Path(app.instance_path) / "uploads")),
        PROFILE_STORE=os.environ.get("PROFILE_STORE", str(Path(app.instance_path) / "profiles.json")),
        PROFILE_SETTINGS=os.environ.get("PROFILE_SETTINGS", str(Path(app.instance_path) / "profile_settings.json")),
        SSH_KEY_FOLDER=os.environ.get("SSH_KEY_FOLDER", str(Path(app.instance_path) / "profile_ssh_keys")),
        MAX_CONTENT_LENGTH=25 * 1024 * 1024,
        CONTROL_DATABASE=os.environ.get("CONTROL_DATABASE", "fndb"),
        OCI_FUNCTION_ID=os.environ.get("OCI_FUNCTION_ID", ""),
        OCI_COMPARTMENT_ID=os.environ.get("OCI_COMPARTMENT_ID", ""),
        OCI_REGION=os.environ.get("OCI_REGION", ""),
        OCI_FUNCTION_CONFIGURATION_ENABLED=os.environ.get("OCI_FUNCTION_CONFIGURATION_ENABLED", "false").lower() in {"1", "true", "yes"},
        OCI_EVENT_RULE_MANAGEMENT_ENABLED=os.environ.get("OCI_EVENT_RULE_MANAGEMENT_ENABLED", "false").lower() in {"1", "true", "yes"},
        OCI_EVENT_RULE_PREFIX=os.environ.get("OCI_EVENT_RULE_PREFIX", "object-storage-heatwave"),
        OCI_OBJECT_STORAGE_NAMESPACE=os.environ.get("OCI_OBJECT_STORAGE_NAMESPACE", ""),
    )
    if test_config:
        app.config.update(test_config)

    for directory in (app.instance_path, app.config["UPLOAD_FOLDER"], app.config["SSH_KEY_FOLDER"]):
        Path(directory).mkdir(parents=True, exist_ok=True)
    app.extensions["profile_store"] = ProfileStore(
        Path(app.config["PROFILE_STORE"]),
        Path(app.config["SSH_KEY_FOLDER"]),
        Path(app.config["PROFILE_SETTINGS"]),
    )
    app.extensions["session_store"] = SessionStore()

    app.register_blueprint(auth_bp)
    app.register_blueprint(event_tx_bp)
    app.register_blueprint(profile_bp)
    app.register_blueprint(import_bp)
    app.register_blueprint(mappings_bp)
    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=8080, debug=True)
