import unittest
from types import SimpleNamespace
from unittest.mock import patch

from myapp.services.registry_service import RegistryService


class RegistryServiceTest(unittest.TestCase):
    def test_image_url_is_built_from_repository_tag(self):
        service = RegistryService(compartment_id="c", region="uk-london-1", namespace="ns", region_key="lhr")
        client = SimpleNamespace(
            list_container_images=lambda **_: SimpleNamespace(data=[SimpleNamespace(id="i", version="processor-v1", digest="sha256:x", lifecycle_state="AVAILABLE")], next_page=None, has_next_page=False)
        )
        with patch.object(service, "_client", return_value=client), patch("oci.pagination.list_call_get_all_results", return_value=SimpleNamespace(data=[SimpleNamespace(id="i", version="processor-v1", digest="sha256:x", lifecycle_state="AVAILABLE")])):
            self.assertEqual(service.list_images("repo")[0]["image_url"], "lhr.ocir.io/ns/repo:processor-v1")

    def test_unavailable_images_are_excluded(self):
        service = RegistryService(compartment_id="c", region="r", namespace="ns", region_key="lhr")
        client = SimpleNamespace(
            list_container_images=lambda **_: SimpleNamespace(data=[SimpleNamespace(id="i", version="processor-v1", digest="d", lifecycle_state="DELETED")], next_page=None, has_next_page=False)
        )
        with patch.object(service, "_client", return_value=client), patch("oci.pagination.list_call_get_all_results", return_value=SimpleNamespace(data=[SimpleNamespace(id="i", version="processor-v1", digest="d", lifecycle_state="DELETED")])):
            self.assertEqual(service.list_images("repo"), [])

    def test_ui_release_tags_are_excluded_from_processor_choices(self):
        service = RegistryService(compartment_id="c", region="r", namespace="ns", region_key="lhr")
        rows = [
            SimpleNamespace(id="processor", version="processor-v2", digest="processor-digest", lifecycle_state="AVAILABLE"),
            SimpleNamespace(id="ui", version="ui-v2", digest="ui-digest", lifecycle_state="AVAILABLE"),
            SimpleNamespace(id="unqualified", version="v1", digest="unqualified-digest", lifecycle_state="AVAILABLE"),
        ]
        client = SimpleNamespace(list_container_images=lambda **_: SimpleNamespace(data=rows, next_page=None, has_next_page=False))
        with patch.object(service, "_client", return_value=client), patch(
            "oci.pagination.list_call_get_all_results",
            return_value=SimpleNamespace(data=rows),
        ):
            images = service.list_images("repo")
        self.assertEqual([image["version"] for image in images], ["processor-v2"])

    def test_images_are_newest_first_with_utc_timestamp(self):
        service = RegistryService(compartment_id="c", region="r", namespace="ns", region_key="lhr")
        rows = [
            SimpleNamespace(id="old", version="processor-20260922T093000Z-old", digest="old", lifecycle_state="AVAILABLE"),
            SimpleNamespace(id="new", version="processor-20260923T093000Z-new", digest="new", lifecycle_state="AVAILABLE"),
        ]
        client = SimpleNamespace(list_container_images=lambda **_: None)
        with patch.object(service, "_client", return_value=client), patch(
            "oci.pagination.list_call_get_all_results", return_value=SimpleNamespace(data=rows),
        ):
            images = service.list_images("repo")
        self.assertEqual([image["id"] for image in images], ["new", "old"])
        self.assertEqual(images[0]["release_timestamp"], "2026-09-23 09:30:00 UTC")

    def test_delete_rejects_a_non_release_tag(self):
        service = RegistryService(compartment_id="c", region="r")
        with patch.object(service, "list_images", return_value=[{"id": "other", "version": "scratch"}]):
            with self.assertRaises(ValueError):
                service.delete_image(repository_name="repo", image_id="other")


if __name__ == "__main__":
    unittest.main()
