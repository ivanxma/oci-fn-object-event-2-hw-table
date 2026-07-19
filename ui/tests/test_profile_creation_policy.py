from __future__ import annotations

import tempfile
import unittest
import os
from pathlib import Path
from unittest.mock import patch

from myapp.app import create_app


class ProfileCreationPolicyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.app = create_app(
            {
                "TESTING": True,
                "SECRET_KEY": "test-only-key",
                "SESSION_COOKIE_SECURE": False,
                "PROFILE_STORE": str(root / "profiles.json"),
                "PROFILE_SETTINGS": str(root / "profile_settings.json"),
                "UPLOAD_FOLDER": str(root / "uploads"),
                "SSH_KEY_FOLDER": str(root / "keys"),
            }
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_login_shows_creation_link_when_enabled(self) -> None:
        response = self.client.get("/login")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Create a profile", response.data)

    def test_login_hides_creation_link_when_disabled(self) -> None:
        store = self.app.extensions["profile_store"]
        store.set_profile_creation_enabled(False)
        response = self.client.get("/login")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"href=\"/profiles/new\"", response.data)
        self.assertIn(b"Profile creation is disabled", response.data)
        blocked = self.client.get("/profiles/new")
        self.assertEqual(blocked.status_code, 302)
        self.assertTrue(blocked.headers["Location"].endswith("/login"))

    def test_policy_is_private_persistent_and_fails_closed_if_corrupt(self) -> None:
        store = self.app.extensions["profile_store"]
        store.set_profile_creation_enabled(False)
        self.assertEqual(os.stat(store.settings_path).st_mode & 0o777, 0o600)
        self.assertFalse(store.profile_creation_enabled())
        store.settings_path.write_text("not-json", encoding="utf-8")
        self.assertFalse(store.profile_creation_enabled())

    def test_public_creation_cannot_replace_an_existing_profile(self) -> None:
        response = self.client.post(
            "/profiles/new",
            data={
                "name": "Local MySQL",
                "mode": "direct",
                "host": "attacker.invalid",
                "port": "3306",
                "database": "",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"already exists", response.data)
        profile = self.app.extensions["profile_store"].get("Local MySQL")
        self.assertEqual(profile["host"], "127.0.0.1")

    def test_successful_login_prompts_when_creation_is_enabled(self) -> None:
        with patch("myapp.modules.auth_routes.MySQLService.health_check", return_value=None):
            response = self.client.post(
                "/login",
                data={"profile": "Local MySQL", "username": "admin", "password": "not-rendered"},
            )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/profiles/creation-policy"))
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None):
            prompt = self.client.get(response.headers["Location"])
        self.assertEqual(prompt.status_code, 200)
        self.assertIn(b"Connection established successfully", prompt.data)
        self.assertIn(b"Disable profile creation", prompt.data)

    def test_policy_can_be_disabled_and_enabled_from_connection_profiles(self) -> None:
        profile = {"name": "test", "mode": "direct", "host": "db", "port": 3306}
        connection_id = self.app.extensions["session_store"].create(profile, "admin", "not-rendered")
        with self.client.session_transaction() as browser_session:
            browser_session["connection_id"] = connection_id
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None):
            page = self.client.get("/profiles/")
            disabled = self.client.post("/profiles/creation-policy", data={"enabled": "false"})
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"Disable profile creation", page.data)
        self.assertEqual(disabled.status_code, 302)
        self.assertFalse(self.app.extensions["profile_store"].profile_creation_enabled())
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None):
            enabled = self.client.post("/profiles/creation-policy", data={"enabled": "true"})
        self.assertEqual(enabled.status_code, 302)
        self.assertTrue(self.app.extensions["profile_store"].profile_creation_enabled())


if __name__ == "__main__":
    unittest.main()
