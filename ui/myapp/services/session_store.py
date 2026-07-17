"""Opaque browser session IDs with server-owned credentials and import jobs."""

from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ConnectionSession:
    profile: dict
    username: str
    password: str
    tunnel: object | None = None
    imports: dict[str, dict] = field(default_factory=dict)


class SessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, ConnectionSession] = {}

    def create(self, profile: dict, username: str, password: str, tunnel: object | None = None) -> str:
        session_id = secrets.token_urlsafe(32)
        self._sessions[session_id] = ConnectionSession(profile, username, password, tunnel)
        return session_id

    def get(self, session_id: str | None) -> ConnectionSession | None:
        return self._sessions.get(session_id or "")

    def clear(self, session_id: str | None) -> None:
        connection = self._sessions.pop(session_id or "", None)
        if not connection:
            return
        for job in connection.imports.values():
            Path(job["path"]).unlink(missing_ok=True)
        if connection.tunnel:
            connection.tunnel.stop()
