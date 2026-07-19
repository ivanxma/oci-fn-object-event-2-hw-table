"""Login and logout routes."""

from __future__ import annotations

from flask import Blueprint, current_app, flash, redirect, render_template, request, session, url_for

from ..services.mysql_service import MySQLService
from ..services.ssh_tunnel import open_tunnel
from .common import connection_state

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    store = current_app.extensions["profile_store"]
    if request.method == "POST":
        profile = store.get(request.form.get("profile", ""))
        username, password = request.form.get("username", ""), request.form.get("password", "")
        if not profile or not username or not password:
            flash("Choose a profile and enter a username and password.", "error")
        else:
            tunnel = None
            try:
                if profile["mode"] == "ssh":
                    tunnel = open_tunnel(profile, store.key_path(profile))
                provisional = type("State", (), {"profile": profile, "username": username, "password": password, "tunnel": tunnel})()
                MySQLService(provisional).health_check()
                connection_id = current_app.extensions["session_store"].create(profile, username, password, tunnel)
                session.clear()
                session["connection_id"] = connection_id
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
