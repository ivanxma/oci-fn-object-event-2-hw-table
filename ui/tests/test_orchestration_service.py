import unittest
from types import SimpleNamespace
from unittest.mock import patch
from myapp.services.orchestration_service import ContainerOrchestrationService, DeploymentSettings, OrchestrationError, assigned_partitions, validate_deployment

class OrchestrationServiceTest(unittest.TestCase):
    def test_modes(self):
        validate_deployment(processing_mode="FIFO", partitions=1, replicas=1)
        validate_deployment(processing_mode="PARALLEL", partitions=2, replicas=1)
        with self.assertRaises(ValueError): validate_deployment(processing_mode="FIFO", partitions=2, replicas=1)
        with self.assertRaises(ValueError): validate_deployment(processing_mode="PARALLEL", partitions=1, replicas=1)

    def test_explicit_assignment_and_secret_free_container_environment(self):
        self.assertEqual(assigned_partitions("0,2", partition_count=3, mode="PARALLEL"), ["0", "2"])
        with self.assertRaises(ValueError): assigned_partitions("1", partition_count=1, mode="FIFO")
        settings = DeploymentSettings(True, "ocid1.compartment.test", "uk-london-1", "ocid1.subnet.test", "AD-1", "CI.Standard.E4.Flex", 1, 16, "lhr.ocir.io/ns/repo:tag", "ocid1.vaultsecret.test", "db", "3306", "streamuser", "stream_db")
        spec = ContainerOrchestrationService(settings).deployment_spec(mapping={"id": 7, "stream_id": "ocid1.stream.test", "processing_mode": "FIFO"}, stream_partitions=1, partition_assignment="0")
        env = spec["container"]["environment_variables"]
        self.assertFalse(spec["container"]["is_resource_principal_disabled"])
        self.assertEqual(env["CONSUMER_PARTITIONS"], "0")
        self.assertNotIn("DB_CREDENTIAL", env)

    def test_rejects_non_positive_resources(self):
        settings = DeploymentSettings(True, "ocid1.compartment.test", "uk-london-1", "ocid1.subnet.test", "AD-1", "CI.Standard.E4.Flex", 0, 16, "lhr.ocir.io/ns/repo:tag", "ocid1.vaultsecret.test", "db", "3306", "streamuser", "stream_db")
        with self.assertRaisesRegex(ValueError, "greater than zero"):
            ContainerOrchestrationService(settings).deployment_spec(mapping={"stream_id": "ocid1.stream.test", "processing_mode": "FIFO"}, stream_partitions=1, partition_assignment="0")

    def test_list_wraps_oci_lifecycle_failure(self):
        settings = DeploymentSettings(True, "ocid1.compartment.test", "uk-london-1", "ocid1.subnet.test", "AD-1", "CI.Standard.E4.Flex", 1, 16, "lhr.ocir.io/ns/repo:tag", "ocid1.vaultsecret.test", "db", "3306", "streamuser", "stream_db")
        service = ContainerOrchestrationService(settings)
        oci = SimpleNamespace(pagination=SimpleNamespace(list_call_get_all_results=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("lifecycle unavailable"))))
        with patch.object(service, "_client", return_value=(oci, SimpleNamespace(list_container_instances=object()))):
            with self.assertRaisesRegex(OrchestrationError, "Could not list Container Instances: RuntimeError"):
                service.list_deployments()
