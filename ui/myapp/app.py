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
from .modules.streaming_routes import streaming_bp
from .modules.orchestration_routes import orchestration_bp
from .modules.durable_message_routes import durable_messages_bp
from .modules.flow_routes import flow_bp
from .modules.settings_routes import settings_bp
from .services.profile_store import ProfileStore
from .services.session_store import SessionStore


def create_app(test_config: dict | None = None) -> Flask:
    app = Flask(__name__, instance_relative_config=True)
    profile_store = os.environ.get("PROFILE_STORE", str(Path(app.instance_path) / "profiles.json"))
    profile_settings = os.environ.get(
        "PROFILE_SETTINGS",
        str(Path(profile_store).with_name("profile_settings.json")),
    )
    app.config.from_mapping(
        SECRET_KEY=os.environ.get("FLASK_SECRET_KEY", os.urandom(32)),
        SESSION_COOKIE_NAME="csv_import_session",
        SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE", "0").lower() in {"1", "true", "yes"},
        SESSION_COOKIE_SAMESITE="Lax",
        UPLOAD_FOLDER=os.environ.get("UPLOAD_FOLDER", str(Path(app.instance_path) / "uploads")),
        PROFILE_STORE=profile_store,
        PROFILE_SETTINGS=profile_settings,
        SSH_KEY_FOLDER=os.environ.get("SSH_KEY_FOLDER", str(Path(app.instance_path) / "profile_ssh_keys")),
        MAX_CONTENT_LENGTH=25 * 1024 * 1024,
        CONTROL_DATABASE=os.environ.get("CONTROL_DATABASE", ""),
        STREAM_USER=os.environ.get("STREAM_USER", "streamuser"),
        OCI_COMPARTMENT_ID=os.environ.get("OCI_COMPARTMENT_ID", ""),
        OCI_REGION=os.environ.get("OCI_REGION", ""),
        OCI_REGION_KEY=os.environ.get("REGION_KEY", os.environ.get("OCI_REGION_KEY", "")),
        OCI_OBJECT_STORAGE_NAMESPACE=os.environ.get("OCI_OBJECT_STORAGE_NAMESPACE", ""),
        OCI_REGISTRY_REPOSITORY=os.environ.get("OCI_REGISTRY_REPOSITORY", ""),
        VAULT_ID=os.environ.get("VAULT_ID", ""),
        VAULT_KEY_ID=os.environ.get("VAULT_KEY_ID", ""),
        OCI_EVENT_RULE_MANAGEMENT_ENABLED=os.environ.get("OCI_EVENT_RULE_MANAGEMENT_ENABLED", "false").lower() in {"1", "true", "yes"},
        OCI_EVENT_RULE_PREFIX=os.environ.get("OCI_EVENT_RULE_PREFIX", "object-storage-heatwave"),
        OCI_STREAMING_MANAGEMENT_ENABLED=os.environ.get("OCI_STREAMING_MANAGEMENT_ENABLED", "false").lower() in {"1", "true", "yes"},
        OCI_CONTAINER_ORCHESTRATION_ENABLED=os.environ.get("OCI_CONTAINER_ORCHESTRATION_ENABLED", "false").lower() in {"1", "true", "yes"},
        CONTAINER_AVAILABILITY_DOMAIN=os.environ.get("CONTAINER_AVAILABILITY_DOMAIN", ""), PROCESSOR_SHAPE=os.environ.get("PROCESSOR_SHAPE", ""),
        PROCESSOR_OCPUS=os.environ.get("PROCESSOR_OCPUS", ""), PROCESSOR_MEMORY_GBS=os.environ.get("PROCESSOR_MEMORY_GBS", ""),
        PROCESSOR_IMAGE_URL=os.environ.get("PROCESSOR_IMAGE_URL", ""), PROCESSOR_CONTAINER_NAME_PREFIX=os.environ.get("PROCESSOR_CONTAINER_NAME_PREFIX", "object-storage-stream-processor"),
        WRITER_WORKERS=os.environ.get("WRITER_WORKERS", "4"),
        DB_SECRET_OCID=os.environ.get("DB_SECRET_OCID", ""), DB_HOST=os.environ.get("DB_HOST", ""), DB_PORT=os.environ.get("DB_PORT", "3306"), DB_USER=os.environ.get("DB_USER", ""), DB_NAME=os.environ.get("DB_NAME", ""), STREAM_DATA_DB_NAME=os.environ.get("STREAM_DATA_DB_NAME", ""), STAGING_DATABASE=os.environ.get("STAGING_DATABASE", "stream_staging"), SUBNET_ID=os.environ.get("SUBNET_ID", ""),
    )
    if test_config:
        app.config.update(test_config)
        if "PROFILE_STORE" in test_config and "PROFILE_SETTINGS" not in test_config:
            app.config["PROFILE_SETTINGS"] = str(
                Path(app.config["PROFILE_STORE"]).with_name("profile_settings.json")
            )

    for directory in (app.instance_path, app.config["UPLOAD_FOLDER"], app.config["SSH_KEY_FOLDER"]):
        Path(directory).mkdir(parents=True, exist_ok=True)
    app.extensions["profile_store"] = ProfileStore(
        Path(app.config["PROFILE_STORE"]),
        Path(app.config["SSH_KEY_FOLDER"]),
        Path(app.config["PROFILE_SETTINGS"]),
    )
    persisted_ui = app.extensions["profile_store"].ui_configuration()
    for key in ("CONTROL_DATABASE", "STREAM_DATA_DB_NAME", "STREAM_USER", "OCI_REGISTRY_REPOSITORY"):
        if persisted_ui.get(key):
            app.config[key] = str(persisted_ui[key])
            os.environ[key] = str(persisted_ui[key])
    app.extensions["session_store"] = SessionStore()

    app.register_blueprint(auth_bp)
    app.register_blueprint(event_tx_bp)
    app.register_blueprint(profile_bp)
    app.register_blueprint(import_bp)
    app.register_blueprint(mappings_bp)
    app.register_blueprint(streaming_bp)
    app.register_blueprint(orchestration_bp)
    app.register_blueprint(durable_messages_bp)
    app.register_blueprint(flow_bp)
    app.register_blueprint(settings_bp)
    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=8080, debug=True)
