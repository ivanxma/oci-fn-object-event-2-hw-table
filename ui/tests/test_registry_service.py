import unittest
from types import SimpleNamespace
from unittest.mock import patch

from myapp.services.registry_service import RegistryService


class RegistryServiceTest(unittest.TestCase):
    def test_image_url_is_built_from_repository_tag(self):
        service = RegistryService(compartment_id="c", region="uk-london-1", namespace="ns", region_key="lhr")
        client = SimpleNamespace(
            list_container_images=lambda **_: SimpleNamespace(data=[SimpleNamespace(id="i", version="v1", digest="sha256:x", lifecycle_state="AVAILABLE")], next_page=None, has_next_page=False)
        )
        with patch.object(service, "_client", return_value=client), patch("oci.pagination.list_call_get_all_results", return_value=SimpleNamespace(data=[SimpleNamespace(id="i", version="v1", digest="sha256:x", lifecycle_state="AVAILABLE")])):
            self.assertEqual(service.list_images("repo")[0]["image_url"], "lhr.ocir.io/ns/repo:v1")

    def test_unavailable_images_are_excluded(self):
        service = RegistryService(compartment_id="c", region="r", namespace="ns", region_key="lhr")
        client = SimpleNamespace(
            list_container_images=lambda **_: SimpleNamespace(data=[SimpleNamespace(id="i", version="v1", digest="d", lifecycle_state="DELETED")], next_page=None, has_next_page=False)
        )
        with patch.object(service, "_client", return_value=client), patch("oci.pagination.list_call_get_all_results", return_value=SimpleNamespace(data=[SimpleNamespace(id="i", version="v1", digest="d", lifecycle_state="DELETED")])):
            self.assertEqual(service.list_images("repo"), [])


if __name__ == "__main__":
    unittest.main()
