import unittest
import base64
import json
from types import SimpleNamespace
from unittest.mock import patch

from myapp.services.vault_secret_service import VaultSecretError, VaultSecretService


class VaultSecretServiceTest(unittest.TestCase):
    def test_active_secret_metadata_is_sorted_without_content(self):
        service = VaultSecretService(compartment_id="ocid1.compartment.test", region="uk-london-1")
        records = [
            SimpleNamespace(id="ocid1.vaultsecret.z", secret_name="zebra", lifecycle_state="ACTIVE"),
            SimpleNamespace(id="ocid1.vaultsecret.old", secret_name="old", lifecycle_state="PENDING_DELETION"),
            SimpleNamespace(id="ocid1.vaultsecret.a", secret_name="Alpha", lifecycle_state="ACTIVE"),
        ]
        oci = SimpleNamespace(pagination=SimpleNamespace(list_call_get_all_results=lambda *_args, **_kwargs: SimpleNamespace(data=records)))
        with patch.object(service, "_client", return_value=(oci, SimpleNamespace(list_secrets=object()))):
            found = service.list_active_secrets()
        self.assertEqual([(item.name, item.id) for item in found], [("Alpha", "ocid1.vaultsecret.a"), ("zebra", "ocid1.vaultsecret.z")])
        self.assertFalse(hasattr(found[0], "content"))

    def test_disabled_selection_fails_cleanly(self):
        with self.assertRaisesRegex(VaultSecretError, "disabled"):
            VaultSecretService(compartment_id="x", region="y", enabled=False).list_active_secrets()

    def test_list_error_is_redacted_for_browser_use(self):
        service = VaultSecretService(compartment_id="ocid1.compartment.test", region="uk-london-1")
        oci = SimpleNamespace(pagination=SimpleNamespace(list_call_get_all_results=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("opc-request-id=private endpoint"))))
        with patch.object(service, "_client", return_value=(oci, SimpleNamespace(list_secrets=object()))):
            with self.assertRaisesRegex(VaultSecretError, "read secrets") as raised:
                service.list_active_secrets()
        self.assertNotIn("opc-request-id", str(raised.exception))

    def test_create_database_secret_encodes_json_without_returning_value(self):
        service = VaultSecretService(compartment_id="ocid1.compartment.test", region="uk-london-1")
        created = []
        class Content:
            def __init__(self, **kwargs): self.__dict__.update(kwargs)
        class Details:
            def __init__(self, **kwargs): self.__dict__.update(kwargs)
        client = SimpleNamespace(create_secret=lambda details: (created.append(details) or SimpleNamespace(data=SimpleNamespace(id="ocid1.vaultsecret.new", secret_name="processor-db", lifecycle_state="CREATING"))))
        oci = SimpleNamespace(vault=SimpleNamespace(models=SimpleNamespace(Base64SecretContentDetails=Content, CreateSecretDetails=Details)))
        with patch.object(service, "_management_client", return_value=(oci, client)):
            result = service.create_database_secret(name="processor-db", vault_id="ocid1.vault.test", key_id="ocid1.key.test", host="10.0.0.8", port="3306", user="streamuser", password="secret-value", database="testdb", control_database="stream_db", stream_data_database="stream_data")
        self.assertEqual(result.id, "ocid1.vaultsecret.new")
        payload = json.loads(base64.b64decode(created[0].secret_content.content))
        self.assertEqual(payload, {"host": "10.0.0.8", "port": 3306, "user": "streamuser", "credential": "secret-value", "database": "testdb", "control_database": "stream_db", "stream_data_database": "stream_data"})

    def test_create_database_secret_rejects_invalid_ocids_before_oci_call(self):
        service = VaultSecretService(compartment_id="ocid1.compartment.test", region="uk-london-1")
        with self.assertRaisesRegex(ValueError, "Vault and encryption key"):
            service.create_database_secret(name="processor", vault_id="no", key_id="no", host="db", port="3306", user="u", password="x", database="d", control_database="c", stream_data_database="s")
