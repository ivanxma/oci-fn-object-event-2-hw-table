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
        self.assertIn("for value in OBJECT_STORAGE_BUCKET_NAME OCI_REGISTRY_REPOSITORY_ID", source)
        self.assertIn("for value in DB_HOST DB_USER DB_PASSWORD_FILE", source)
        self.assertIn('[[ -n "${DB_SECRET_OCID:-}" ]]', source)

    def test_noninteractive_setup_can_choose_a_unique_vault_or_key(self):
        source = (ROOT / "deploy" / "setup_env.sh").read_text(encoding="utf-8")
        self.assertIn('[[ -z "$selected" && ${#ids[@]} -eq 1 ]]', source)
        self.assertIn("zero or multiple choices are available", source)

    def test_generated_environment_persists_authoritative_processor_repository(self):
        source = (ROOT / "deploy" / "setup_env.sh").read_text(encoding="utf-8")
        build_source = (ROOT / "deploy" / "build_processor_image.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            'select_from_tsv OCI_REGISTRY_REPOSITORY_ID "OCI Container Registry repository"',
            source,
        )
        self.assertIn('write_export OCI_REGISTRY_REPOSITORY_ID "$OCI_REGISTRY_REPOSITORY_ID"', source)
        self.assertIn('write_export OCI_REGISTRY_REPOSITORY "$OCI_REGISTRY_REPOSITORY"', source)
        self.assertIn('write_export PROCESSOR_IMAGE_URL "$PROCESSOR_IMAGE_URL"', source)
        self.assertIn("artifacts container repository get --repository-id", build_source)
        self.assertIn("OCI_REGISTRY_REPOSITORY does not match OCI_REGISTRY_REPOSITORY_ID", build_source)
        self.assertIn(
            'IMAGE="$REGION_KEY.ocir.io/$NAMESPACE/$OCI_REGISTRY_REPOSITORY:$PROCESSOR_REGISTRY_IMAGE_TAG"',
            build_source,
        )
        self.assertIn('PROCESSOR_REGISTRY_IMAGE_TAG="processor-$PROCESSOR_IMAGE_TAG"', build_source)
        self.assertIn("Processor image tag already exists", build_source)
        self.assertIn("Increase PROCESSOR_IMAGE_TAG; released image tags are immutable.", build_source)

    def test_all_deployment_package_installs_use_retry_helper(self):
        for relative in (
            "deploy/bootstrap_streaming.sh",
            "deploy/install_validation_vm.sh",
            "deploy/deploy_ui.sh",
        ):
            source = (ROOT / relative).read_text(encoding="utf-8")
            self.assertIn('source "$ROOT_DIR/deploy/ol9_packages.sh"', source)
            self.assertNotIn("sudo dnf install -y", source)

    def test_ui_release_can_be_overridden_without_editing_generated_env(self):
        source = (ROOT / "deploy" / "deploy_ui.sh").read_text(encoding="utf-8")
        self.assertIn('UI_IMAGE_TAG_OVERRIDE="${UI_IMAGE_TAG_OVERRIDE:-}"', source)
        self.assertIn("UI_IMAGE_TAG_OVERRIDE:-${UI_IMAGE_TAG:-", source)


if __name__ == "__main__":
    unittest.main()
