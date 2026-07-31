"""Login and logout routes."""

from __future__ import annotations

from flask import Blueprint, current_app, flash, redirect, render_template, request, session, url_for

from ..services.mysql_service import MySQLService
from ..services.schema_inventory import missing_application_objects
from ..services.ssh_tunnel import open_tunnel
from .common import connection_state

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    store = current_app.extensions["profile_store"]
    if request.method == "POST":
        profile = store.get(request.form.get("profile", ""))
        username, credential = request.form.get("username", ""), request.form.get("credential", "")
        if not profile or not username or not credential:
            flash("Choose a profile and enter a username and database credential.", "error")
        else:
            tunnel = None
            try:
                if profile["mode"] == "ssh":
                    tunnel = open_tunnel(profile, store.key_path(profile))
                provisional = type("State", (), {"profile": profile, "username": username, "credential": credential, "tunnel": tunnel})()
                MySQLService(provisional).health_check()
                connection_id = current_app.extensions["session_store"].create(profile, username, credential, tunnel)
                session.clear()
                session["connection_id"] = connection_id
                control = str(current_app.config.get("CONTROL_DATABASE") or "").strip()
                durable = str(current_app.config.get("STREAM_DATA_DB_NAME") or "").strip()
                staging = str(current_app.config.get("STAGING_DATABASE") or "").strip()
                if control and durable and staging:
                    try:
                        missing = missing_application_objects(
                            MySQLService(provisional),
                            control_database=control,
                            stream_data_database=durable,
                            staging_database=staging,
                        )
                    except Exception:
                        current_app.logger.info(
                            "Application schema readiness check failed after login.",
                            exc_info=True,
                        )
                        flash(
                            "Connected, but application schema readiness could not be verified. "
                            "Review Settings before continuing.",
                            "warning",
                        )
                        return redirect(url_for("settings.manage", setup="required"))
                    if missing:
                        flash(
                            "Connected. Application database structures are not installed. "
                            "Install them from Settings before continuing.",
                            "warning",
                        )
                        return redirect(url_for("settings.manage", setup="required"))
                if store.profile_creation_enabled():
                    return redirect(url_for("profiles.creation_policy"))
                return redirect(url_for("imports.home"))
            except Exception:
                if tunnel:
                    tunnel.stop()
                current_app.logger.info("Connection login failed for profile %s", profile["name"])
                flash("Could not connect with that profile and username. Check the connection details and credentials.", "error")
    return render_template(
        "login.html",
        profiles=store.list(),
        profile_creation_enabled=store.profile_creation_enabled(),
    )


@auth_bp.post("/logout")
def logout():
    current_app.extensions["session_store"].clear(session.get("connection_id"))
    session.clear()
    flash("You have been signed out.", "success")
    return redirect(url_for("auth.login"))


@auth_bp.get("/")
def root():
    return redirect(url_for("imports.home") if connection_state() else url_for("auth.login"))
