"""Authenticated connection-profile management."""

from __future__ import annotations

from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for

from .common import login_required, render_dashboard

profile_bp = Blueprint("profiles", __name__, url_prefix="/profiles")


@profile_bp.route("/new", methods=["GET", "POST"])
def create():
    """Create a non-secret connection profile before first sign-in."""
    store = current_app.extensions["profile_store"]
    if request.method == "POST":
        try:
            store.save(request.form, request.files.get("ssh_key"))
            flash("Profile created. Sign in with its MySQL username and password.", "success")
            return redirect(url_for("auth.login"))
        except (ValueError, OSError) as error:
            flash(str(error), "error")
    return render_template("profile_form.html", public_creation=True)


@profile_bp.route("/", methods=["GET", "POST"])
@login_required
def manage():
    store = current_app.extensions["profile_store"]
    if request.method == "POST":
        try:
            store.save(request.form, request.files.get("ssh_key"), original_name=request.form.get("original_name") or None)
            flash("Profile saved. Sign out and back in to use its updated settings.", "success")
            return redirect(url_for("profiles.manage"))
        except (ValueError, OSError) as error:
            flash(str(error), "error")
    return render_dashboard("profiles.html", active_page="profiles", profiles=store.list())


@profile_bp.route("/<path:name>/edit", methods=["GET", "POST"])
@login_required
def edit(name: str):
    store = current_app.extensions["profile_store"]
    profile = store.get(name)
    if not profile:
        flash("That connection profile does not exist.", "error")
        return redirect(url_for("profiles.manage"))
    if request.method == "POST":
        try:
            store.save(request.form, request.files.get("ssh_key"), original_name=name)
            flash("Profile updated. Sign out and back in to use the new settings.", "success")
            return redirect(url_for("profiles.manage"))
        except (ValueError, OSError) as error:
            flash(str(error), "error")
    return render_template("profile_form.html", profile=profile, edit=True, original_name=name, public_creation=False)


@profile_bp.post("/<path:name>/delete")
@login_required
def delete(name: str):
    store = current_app.extensions["profile_store"]
    try:
        store.delete(name)
        flash("Profile deleted.", "success")
    except (ValueError, OSError) as error:
        flash(str(error), "error")
    return redirect(url_for("profiles.manage"))
