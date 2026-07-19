"""Non-secret connection profile storage and safe SSH key handling."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

from .naming import validate_identifier


class ProfileStore:
    def __init__(self, path: Path, key_folder: Path, settings_path: Path | None = None) -> None:
        self.path, self.key_folder = path, key_folder
        self.settings_path = settings_path or path.with_name("profile_settings.json")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.key_folder.mkdir(parents=True, exist_ok=True)
        os.chmod(self.key_folder, 0o700)
        if not self.path.exists():
            self._save([{"name": "Local MySQL", "mode": "direct", "host": "127.0.0.1", "port": 3306, "database": ""}])
        if not self.settings_path.exists():
            self.set_profile_creation_enabled(True)

    def _save(self, profiles: list[dict]) -> None:
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(profiles, indent=2) + "\n", encoding="utf-8")
        os.chmod(temporary, 0o600)
        temporary.replace(self.path)

    def list(self) -> list[dict]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def profile_creation_enabled(self) -> bool:
        """Return whether the unauthenticated login-screen create link is shown."""
        try:
            settings = json.loads(self.settings_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return False
        return bool(settings.get("profile_creation_enabled", True))

    def set_profile_creation_enabled(self, enabled: bool) -> None:
        temporary = self.settings_path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps({"profile_creation_enabled": bool(enabled)}, indent=2) + "\n",
            encoding="utf-8",
        )
        os.chmod(temporary, 0o600)
        temporary.replace(self.settings_path)

    def get(self, name: str) -> dict | None:
        return next((profile for profile in self.list() if profile["name"] == name), None)

    def save(
        self,
        profile: dict,
        key_upload=None,
        *,
        original_name: str | None = None,
        create_only: bool = False,
    ) -> None:
        name = validate_identifier(profile["name"].replace(" ", "_"), "profile name").replace("_", " ")
        if create_only and self.get(name):
            raise ValueError("A connection profile with that name already exists.")
        previous = self.get(original_name or name) or {}
        mode = profile.get("mode", "direct")
        if mode not in {"direct", "ssh"}:
            raise ValueError("Connection mode must be direct or SSH tunnel.")
        cleaned = {"name": name, "mode": mode, "host": profile["host"].strip(), "port": self._port(profile.get("port"), "MySQL port"), "database": profile.get("database", "").strip()}
        if not cleaned["host"]:
            raise ValueError("Database host is required.")
        if mode == "ssh":
            cleaned.update({"ssh_host": profile["ssh_host"].strip(), "ssh_port": self._port(profile.get("ssh_port"), "SSH port"), "ssh_user": profile["ssh_user"].strip()})
            if not all(cleaned[key] for key in ("ssh_host", "ssh_user")):
                raise ValueError("SSH host and SSH user are required for a tunnel profile.")
            existing = previous
            if existing.get("ssh_key_id"):
                cleaned["ssh_key_id"] = existing["ssh_key_id"]
            if key_upload and key_upload.filename:
                key_id = validate_identifier(name.replace(" ", "_"), "profile name").lower()
                target = self.key_folder / key_id
                target.mkdir(mode=0o700, exist_ok=True)
                os.chmod(target, 0o700)
                temporary = target / "id_key.tmp"
                key_upload.save(temporary)
                os.chmod(temporary, 0o600)
                temporary.replace(target / "id_key")
                cleaned["ssh_key_id"] = key_id
            key_path = self.key_path(cleaned)
            if not key_path or not key_path.is_file():
                raise ValueError("An SSH tunnel profile requires a private key upload stored on the server.")
        old_names = {name}
        if original_name:
            old_names.add(original_name)
        profiles = [item for item in self.list() if item["name"] not in old_names]
        profiles.append(cleaned)
        self._save(sorted(profiles, key=lambda item: item["name"].lower()))
        old_key = previous.get("ssh_key_id")
        if old_key and old_key != cleaned.get("ssh_key_id"):
            shutil.rmtree(self.key_folder / old_key, ignore_errors=True)

    def delete(self, name: str) -> None:
        profiles = self.list()
        profile = next((item for item in profiles if item["name"] == name), None)
        if not profile:
            raise ValueError("That connection profile does not exist.")
        self._save([item for item in profiles if item["name"] != name])
        key_id = profile.get("ssh_key_id")
        if key_id:
            shutil.rmtree(self.key_folder / key_id, ignore_errors=True)

    @staticmethod
    def _port(value: str | int | None, label: str) -> int:
        if value in (None, ""):
            raise ValueError(f"{label} is required.")
        try:
            port = int(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"{label} must be a number.") from error
        if not 1 <= port <= 65535:
            raise ValueError(f"{label} must be between 1 and 65535.")
        return port

    def key_path(self, profile: dict) -> Path | None:
        key_id = profile.get("ssh_key_id")
        return self.key_folder / key_id / "id_key" if key_id else None
