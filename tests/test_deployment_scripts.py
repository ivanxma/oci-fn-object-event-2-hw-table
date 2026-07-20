from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DeploymentScriptTest(unittest.TestCase):
    def source(self, relative: str) -> str:
        return (ROOT / relative).read_text(encoding="utf-8")

    def test_bootstrap_installs_complete_ui_and_host_tooling(self):
        source = self.source("deploy/bootstrap.sh")
        for package in (
            "firewalld",
            "nginx",
            "openssl",
            "podman",
            "policycoreutils-python-utils",
            "python3-pip",
        ):
            self.assertIn(package, source)
        self.assertIn("container-enginetype: podman", source)

    def test_user_local_tool_path_precedes_prerequisite_checks(self):
        for relative in (
            "deploy/bootstrap.sh",
            "deploy/deploy.sh",
            "deploy/deploy_ui.sh",
            "deploy/showlog.sh",
            "deploy/validate.sh",
            "performance_test/setup.sh",
            "performance_test/run_case.sh",
        ):
            source = self.source(relative)
            self.assertLess(source.index('export PATH="$HOME/.fn/bin'), source.index("command -v"), relative)

    def test_function_deploy_enables_events_and_verifies_rule_target(self):
        source = self.source("deploy/deploy.sh")
        self.assertIn("ENSURE_BUCKET_OBJECT_EVENTS", source)
        self.assertIn("--object-events-enabled true", source)
        self.assertIn('[[ "$RULE_FUNCTION_ID" == "$FUNCTION_ID" ]]', source)
        self.assertIn('[[ "$FUNCTION_STATE" == ACTIVE ]]', source)

    def test_function_deploy_includes_adjustable_runtime_and_queue_settings(self):
        deploy = self.source("deploy/deploy.sh")
        example = self.source("deploy/env.sh.example")
        for setting in (
            "WRITER_WORKERS",
            "LOAD_LEASE_SECONDS",
            "QUEUE_LEASE_SECONDS",
            "QUEUE_REORDER_GRACE_SECONDS",
            "QUEUE_SYNC_RESERVE_SECONDS",
            "QUEUE_SYNC_MINIMUM_START_SECONDS",
            "QUEUE_SHUTDOWN_RESERVE_SECONDS",
            "QUEUE_MINIMUM_START_SECONDS",
            "QUEUE_UNKNOWN_JOB_SECONDS",
            "QUEUE_EXPECTED_BYTES_PER_SECOND",
            "QUEUE_PREDICTION_SAFETY_FACTOR",
        ):
            self.assertIn(setting, deploy)
            self.assertIn(setting, example)
        self.assertIn("Sync queue reserve plus minimum start budget", deploy)
        self.assertIn("Detached queue reserve plus minimum start budget", deploy)

    def test_ui_deploy_reuses_tls_and_has_bounded_health_gate(self):
        source = self.source("deploy/deploy_ui.sh")
        self.assertIn("Reusing existing generated TLS certificate", source)
        self.assertIn("for _attempt in $(seq 1 30)", source)
        self.assertIn("journalctl -u", source)
        self.assertIn("timeout 20s firewall-cmd", source)

    def test_performance_helpers_do_not_relabel_read_only_source(self):
        for relative in ("performance_test/setup.sh", "performance_test/run_case.sh"):
            source = self.source(relative)
            self.assertIn("--security-opt label=disable", source)
            self.assertNotIn(".py:ro,Z", source)
        self.assertIn("/app/instance/profile_ssh_keys", self.source("performance_test/setup.sh"))

    def test_one_command_setup_runs_deployments_and_validation(self):
        source = self.source("deploy/setup.sh")
        for script in ("bootstrap.sh", "deploy.sh", "deploy_ui.sh", "validate.sh"):
            self.assertIn(script, source)
        self.assertIn("--smoke-test", source)


if __name__ == "__main__":
    unittest.main()
