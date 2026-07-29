import unittest
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
