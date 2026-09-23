"""Exercise installer branches with local stubs, never OCI or package installs."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
CONFIG = """export OBJECT_STORAGE_BUCKET_NAME='test-bucket'
export OCI_REGISTRY_REPOSITORY_ID='ocid1.containerrepo.test'
export DB_SECRET_OCID='ocid1.vaultsecret.test'
export PROCESSOR_IMAGE_TAG='20260923T120000Z-test'
export FLASK_SECRET_KEY='fixture-only-signing-key'
export GENERATE_SELF_SIGNED_CERT=false
"""


class ValidationInstallerFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.deploy = self.root / "deploy"
        self.deploy.mkdir()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        (self.root / "tests/integration").mkdir(parents=True)
        shutil.copyfile(
            ROOT / "deploy/install_validation_vm.sh",
            self.deploy / "install_validation_vm.sh",
        )
        self.write("deploy/env-template", CONFIG, 0o600)
        self.write(
            "deploy/ol9_packages.sh",
            'ol9_dnf_install() { echo "packages:$*" >> "$CALL_LOG"; }\n',
        )
        self.script("deploy/bootstrap_streaming.sh", 'echo bootstrap >> "$CALL_LOG"')
        self.script(
            "deploy/setup_env.sh",
            'echo "setup:$*" >> "$CALL_LOG"\n'
            'cp "$(dirname "$0")/env-template" "$(dirname "$0")/env.sh"\n'
            'chmod 600 "$(dirname "$0")/env.sh"\n'
            'if [[ -n "${DB_PASSWORD_FILE:-}" ]]; then rm -- "$DB_PASSWORD_FILE"; fi',
        )
        self.script(
            "bin/python3.12",
            'if [[ "$1" == -m && "$2" == venv ]]; then\n'
            '  mkdir -p "$3/bin"\n'
            '  cp "$0" "$3/bin/python"\n'
            '  echo venv >> "$CALL_LOG"\n'
            'elif [[ "$1" == -m ]]; then\n'
            '  echo pip >> "$CALL_LOG"\n'
            'else\n'
            '  echo "initialize:$DB_SECRET_OCID:${DB_PASSWORD_FILE-unset}" >> "$CALL_LOG"\n'
            'fi',
        )
        for name in ("durable_capture", "streaming_deployment"):
            self.script(
                f"tests/integration/verify_{name}.sh",
                f'echo {name} >> "$CALL_LOG"',
            )
        self.script(
            "deploy/publish_release.sh",
            'echo "publish:$*:$GENERATE_SELF_SIGNED_CERT" >> "$CALL_LOG"',
        )
        self.env = {
            "PATH": f"{self.bin}:{os.defpath}",
            "HOME": str(self.root),
            "CALL_LOG": str(self.root / "calls"),
        }

    def write(self, name, content, mode=0o644):
        path = self.root / name
        path.write_text(content, encoding="utf-8")
        path.chmod(mode)
        return path

    def script(self, name, body):
        self.write(name, "#!/bin/bash\nset -eu\n" + body + "\n", 0o755)

    def run_installer(self, config, **env):
        return subprocess.run(
            ["/bin/bash", str(self.deploy / "install_validation_vm.sh"), "--config", config],
            cwd=self.root,
            env={**self.env, **env},
            text=True,
            capture_output=True,
            check=False,
        )

    def calls(self):
        path = self.root / "calls"
        return path.read_text().splitlines() if path.exists() else []

    def test_generated_config_is_preserved_and_setup_is_skipped(self):
        config = self.write("deploy/env.sh", CONFIG, 0o600)
        before = config.stat().st_mtime_ns
        result = self.run_installer("./deploy/env.sh")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(config.read_text(), CONFIG)
        self.assertEqual(config.stat().st_mtime_ns, before)
        self.assertEqual(config.stat().st_mode & 0o777, 0o600)
        calls = self.calls()
        self.assertFalse(any(x.startswith("setup:") for x in calls))
        self.assertIn("venv", calls)
        self.assertIn("initialize:ocid1.vaultsecret.test:unset", calls)
        self.assertEqual(calls[-1], "publish:--version 20260923T120000Z-test:false")
        self.assertLess(calls.index("durable_capture"), calls.index("streaming_deployment"))
        self.assertNotIn("fixture-only-signing-key", result.stdout + result.stderr)

    def test_separate_bare_config_runs_fresh_setup_and_consumes_password_file(self):
        password = self.write("password", "test-only-password", 0o600)
        self.write(
            "input.env",
            "export OBJECT_STORAGE_BUCKET_NAME=test-bucket\n"
            "export OCI_REGISTRY_REPOSITORY_ID=ocid1.containerrepo.test\n"
            "export DB_HOST=test-host\nexport DB_USER=test-user\n"
            f"export DB_PASSWORD_FILE='{password}'\n",
            0o600,
        )
        result = self.run_installer("input.env")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(password.exists())
        calls = self.calls()
        self.assertIn("setup:--non-interactive --force", calls)
        self.assertIn("initialize:ocid1.vaultsecret.test:unset", calls)
        self.assertLess(calls.index("setup:--non-interactive --force"), calls.index("venv"))

    def test_configured_processor_only_install_preserves_no_ui_choice(self):
        self.write("deploy/env.sh", CONFIG + "export INSTALL_UI=false\n", 0o600)
        result = self.run_installer("deploy/env.sh")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.calls()[-1], "publish:--version 20260923T120000Z-test --skip-ui:false")

    def test_incomplete_generated_config_fails_before_bootstrap(self):
        self.write("deploy/env.sh", CONFIG.replace("export DB_SECRET_OCID='ocid1.vaultsecret.test'\n", ""), 0o600)
        result = self.run_installer("deploy/env.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("complete setup_env.sh first", result.stderr)
        self.assertEqual(self.calls(), [])

    def test_invalid_config_permissions_fail_before_bootstrap(self):
        self.write("deploy/env.sh", CONFIG, 0o644)
        result = self.run_installer("deploy/env.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("mode 0600", result.stderr)
        self.assertEqual(self.calls(), [])


if __name__ == "__main__":
    unittest.main()
