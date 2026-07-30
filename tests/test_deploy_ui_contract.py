import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class UiDeploymentContractTest(unittest.TestCase):
    def test_process_local_sessions_use_one_gunicorn_worker(self):
        script = (ROOT / "deploy" / "deploy_ui.sh").read_text(encoding="utf-8")
        dockerfile = (ROOT / "ui" / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn('UI_WORKERS="${UI_WORKERS:-1}"', script)
        self.assertIn('"--workers", "1"', dockerfile)
        self.assertIn("gunicorn", script)

    def test_processor_repository_is_derived_for_older_generated_environments(self):
        script = (ROOT / "deploy" / "deploy_ui.sh").read_text(encoding="utf-8")
        self.assertIn(
            'OCI_REGISTRY_REPOSITORY="${REPOSITORY_PREFIX,,}/$PROCESSOR_IMAGE_NAME"',
            script,
        )

    def test_ui_image_is_published_to_the_processor_repository(self):
        script = (ROOT / "deploy" / "deploy_ui.sh").read_text(encoding="utf-8")
        self.assertIn('UI_REGISTRY_IMAGE_TAG="ui-$UI_IMAGE_TAG"', script)
        self.assertIn(
            'UI_REGISTRY_IMAGE_NAME="$REGION_KEY.ocir.io/$OBJECT_STORAGE_NAMESPACE/$OCI_REGISTRY_REPOSITORY"',
            script,
        )
        self.assertIn(
            'sudo env "PATH=$PATH" podman push --authfile "$REGISTRY_AUTH_FILE" "$UI_IMAGE"',
            script,
        )
        self.assertIn(
            'sudo env "PATH=$PATH" podman pull --authfile "$REGISTRY_AUTH_FILE" "$UI_IMAGE"',
            script,
        )
        self.assertIn('--image-name "$UI_REGISTRY_IMAGE_NAME"', script)


if __name__ == "__main__":
    unittest.main()
