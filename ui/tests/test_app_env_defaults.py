from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from myapp.app import create_app


class AppEnvironmentDefaultsTest(unittest.TestCase):
    def test_selected_vault_and_key_are_available_to_the_ui(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.dict(
                "os.environ",
                {
                    "VAULT_ID": "ocid1.vault.test",
                    "VAULT_KEY_ID": "ocid1.key.test",
                },
            ):
                app = create_app(
                    {
                        "TESTING": True,
                        "PROFILE_STORE": str(root / "profiles.json"),
                        "PROFILE_SETTINGS": str(root / "profile-settings.json"),
                        "UPLOAD_FOLDER": str(root / "uploads"),
                        "SSH_KEY_FOLDER": str(root / "keys"),
                    }
                )
        self.assertEqual(app.config["VAULT_ID"], "ocid1.vault.test")
        self.assertEqual(app.config["VAULT_KEY_ID"], "ocid1.key.test")


if __name__ == "__main__":
    unittest.main()
