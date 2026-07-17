"""Shared authentication and connection helpers for route modules."""

from __future__ import annotations

from functools import wraps

from flask import current_app, flash, redirect, render_template, session, url_for

from ..services.mysql_service import MySQLService


def connection_state():
    return current_app.extensions["session_store"].get(session.get("connection_id"))


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        state = connection_state()
        if not state:
            session.clear()
            flash("Please sign in to a connection profile.", "warning")
            return redirect(url_for("auth.login"))
        try:
            MySQLService(state).health_check()
        except Exception:
            current_app.extensions["session_store"].clear(session.get("connection_id"))
            session.clear()
            flash("The database connection is no longer available. Please sign in again.", "warning")
            return redirect(url_for("auth.login"))
        return view(*args, **kwargs)
    return wrapped


def mysql_for_request() -> MySQLService:
    state = connection_state()
    if not state:
        raise RuntimeError("No active connection session.")
    return MySQLService(state)


def render_dashboard(template: str, *, active_page: str, **context):
    """Render authenticated pages with consistent dashboard context."""
    state = connection_state()
    return render_template(
        template,
        active_page=active_page,
        selected_profile=state.profile if state else {},
        current_username=state.username if state else "",
        **context,
    )
