import unittest
from types import SimpleNamespace
from unittest.mock import patch
from myapp.services.orchestration_service import ConsumerRuntime, ContainerOrchestrationService, DeploymentSettings, OrchestrationError, assigned_partitions, validate_deployment

class OrchestrationServiceTest(unittest.TestCase):
    def _service(self):
        return ContainerOrchestrationService(DeploymentSettings(True, "ocid1.compartment.test", "uk-london-1", "ocid1.subnet.test", "AD-1", "CI.Standard.E4.Flex", 1, 16, "lhr.ocir.io/ns/repo:tag", "ocid1.vaultsecret.test", "db", "3306", "streamuser", "stream_db", "stream_data", "stream_db"))

    def test_modes(self):
        validate_deployment(processing_mode="FIFO", partitions=1, replicas=1)
        validate_deployment(processing_mode="PARALLEL", partitions=2, replicas=1)
        with self.assertRaises(ValueError): validate_deployment(processing_mode="FIFO", partitions=2, replicas=1)
        with self.assertRaises(ValueError): validate_deployment(processing_mode="PARALLEL", partitions=1, replicas=1)

    def test_explicit_assignment_and_secret_free_container_environment(self):
        self.assertEqual(assigned_partitions("0,2", partition_count=3, mode="PARALLEL"), ["0", "2"])
        with self.assertRaises(ValueError): assigned_partitions("1", partition_count=1, mode="FIFO")
        settings = DeploymentSettings(True, "ocid1.compartment.test", "uk-london-1", "ocid1.subnet.test", "AD-1", "CI.Standard.E4.Flex", 1, 16, "lhr.ocir.io/ns/repo:tag", "ocid1.vaultsecret.test", "db", "3306", "streamuser", "stream_db", "stream_data", "stream_db")
        spec = ContainerOrchestrationService(settings).deployment_spec(mapping={"id": 7, "stream_id": "ocid1.stream.test", "processing_mode": "FIFO"}, stream_partitions=1, partition_assignment="0")
        env = spec["container"]["environment_variables"]
        self.assertFalse(spec["container"]["is_resource_principal_disabled"])
        self.assertEqual(env["CONSUMER_PARTITIONS"], "0")
        self.assertEqual(env["DB_NAME"], "stream_db")
        self.assertEqual(env["STREAM_DATA_DB_NAME"], "stream_data")
        self.assertEqual(env["CONTROL_DATABASE"], "stream_db")
        self.assertEqual(env["WRITER_WORKERS"], "4")
        self.assertNotIn("DB_CREDENTIAL", env)

    def test_form_runtime_overrides_non_secret_values_with_validation(self):
        runtime = ConsumerRuntime.from_form({
            "image_url": "lhr.ocir.io/ns/repo:next", "db_secret_ocid": "ocid1.vaultsecret.new",
            "db_host": "10.0.0.8", "db_port": "3307", "db_user": "stream_user",
            "db_name": "testdb", "stream_data_db_name": "stream_data", "control_database": "stream_db",
            "writer_workers": "8",
        }, self._service().settings)
        spec = self._service().deployment_spec(mapping={"id": 7, "stream_id": "ocid1.stream.test", "processing_mode": "FIFO"}, stream_partitions=1, partition_assignment="0", runtime=runtime)
        self.assertEqual(spec["container"]["image_url"], "lhr.ocir.io/ns/repo:next")
        self.assertEqual(spec["container"]["environment_variables"]["WRITER_WORKERS"], "8")
        with self.assertRaisesRegex(ValueError, "valid OCI Vault secret"):
            ConsumerRuntime.from_form({"db_secret_ocid": "not-a-secret"}, self._service().settings)

    def test_rejects_non_positive_resources(self):
        settings = DeploymentSettings(True, "ocid1.compartment.test", "uk-london-1", "ocid1.subnet.test", "AD-1", "CI.Standard.E4.Flex", 0, 16, "lhr.ocir.io/ns/repo:tag", "ocid1.vaultsecret.test", "db", "3306", "streamuser", "stream_db", "stream_data", "stream_db")
        with self.assertRaisesRegex(ValueError, "greater than zero"):
            ContainerOrchestrationService(settings).deployment_spec(mapping={"stream_id": "ocid1.stream.test", "processing_mode": "FIFO"}, stream_partitions=1, partition_assignment="0")

    def test_list_wraps_oci_lifecycle_failure(self):
        settings = DeploymentSettings(True, "ocid1.compartment.test", "uk-london-1", "ocid1.subnet.test", "AD-1", "CI.Standard.E4.Flex", 1, 16, "lhr.ocir.io/ns/repo:tag", "ocid1.vaultsecret.test", "db", "3306", "streamuser", "stream_db", "stream_data", "stream_db")
        service = ContainerOrchestrationService(settings)
        oci = SimpleNamespace(pagination=SimpleNamespace(list_call_get_all_results=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("lifecycle unavailable"))))
        with patch.object(service, "_client", return_value=(oci, SimpleNamespace(list_container_instances=object()))):
            with self.assertRaisesRegex(OrchestrationError, "Could not list Container Instances: RuntimeError"):
                service.list_deployments()

    def test_create_builds_resource_principal_container_request(self):
        class Details:
            def __init__(self, **kwargs): self.__dict__.update(kwargs)
        class Client:
            def __init__(self): self.details = None
            def create_container_instance(self, details):
                self.details = details
                return SimpleNamespace(data=SimpleNamespace(id="ocid1.containerinstance.test", display_name=details.display_name, lifecycle_state="CREATING"))
        client = Client()
        models = SimpleNamespace(CreateContainerInstanceDetails=Details, CreateContainerInstanceShapeConfigDetails=Details, CreateContainerDetails=Details, CreateContainerVnicDetails=Details)
        oci = SimpleNamespace(container_instances=SimpleNamespace(models=models))
        service = self._service()
        with patch.object(service, "_client", return_value=(oci, client)):
            result = service.create(mapping={"id": 3, "stream_id": "ocid1.stream.test", "processing_mode": "FIFO"}, stream_partitions=1, partition_assignment="0")
        self.assertEqual(result["lifecycle_state"], "CREATING")
        self.assertFalse(client.details.containers[0].is_resource_principal_disabled)
        self.assertEqual(client.details.vnics[0].subnet_id, "ocid1.subnet.test")
        self.assertEqual(client.details.freeform_tags["mapping-id"], "3")

    def test_create_wraps_oci_failure(self):
        service = self._service()
        with patch.object(service, "_client", side_effect=RuntimeError("api unavailable")):
            with self.assertRaisesRegex(OrchestrationError, "Could not create Container Instance: RuntimeError"):
                service.create(mapping={"stream_id": "ocid1.stream.test", "processing_mode": "FIFO"}, stream_partitions=1, partition_assignment="0")
