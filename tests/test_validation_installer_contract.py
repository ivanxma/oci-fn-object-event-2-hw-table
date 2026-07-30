from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ValidationInstallerContractTest(unittest.TestCase):
    def test_generated_environment_is_loaded_before_database_initialization(self):
        source = (ROOT / "deploy" / "install_validation_vm.sh").read_text(
            encoding="utf-8"
        )
        setup = source.index('"$ROOT_DIR/deploy/setup_env.sh" --non-interactive --force')
        load = source.index('. "$ROOT_DIR/deploy/env.sh"', setup)
        initialize = source.index('"$ROOT_DIR/deploy/initialize_databases.py"', load)
        self.assertLess(setup, load)
        self.assertLess(load, initialize)
        self.assertIn("unset DB_PASSWORD_FILE", source[setup:initialize])
        self.assertIn("Generated env.sh does not contain a valid DB_SECRET_OCID", source)

    def test_clean_install_accepts_password_file_without_secret_ocid(self):
        source = (ROOT / "deploy" / "install_validation_vm.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("for value in OBJECT_STORAGE_BUCKET_NAME", source)
        self.assertIn("for value in DB_HOST DB_USER DB_PASSWORD_FILE", source)
        self.assertIn('[[ -n "${DB_SECRET_OCID:-}" ]]', source)

    def test_noninteractive_setup_can_choose_a_unique_vault_or_key(self):
        source = (ROOT / "deploy" / "setup_env.sh").read_text(encoding="utf-8")
        self.assertIn('[[ -z "$selected" && ${#ids[@]} -eq 1 ]]', source)
        self.assertIn("zero or multiple choices are available", source)

    def test_all_deployment_package_installs_use_retry_helper(self):
        for relative in (
            "deploy/bootstrap_streaming.sh",
            "deploy/install_validation_vm.sh",
            "deploy/deploy_ui.sh",
        ):
            source = (ROOT / relative).read_text(encoding="utf-8")
            self.assertIn('source "$ROOT_DIR/deploy/ol9_packages.sh"', source)
            self.assertNotIn("sudo dnf install -y", source)


if __name__ == "__main__":
    unittest.main()
