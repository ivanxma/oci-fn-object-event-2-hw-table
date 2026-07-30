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


if __name__ == "__main__":
    unittest.main()
