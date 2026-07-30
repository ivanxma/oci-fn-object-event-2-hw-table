"""Secret-free UI build provenance exposed to authenticated pages."""
from __future__ import annotations

import os


def release_metadata() -> dict[str, str]:
    return {
        "release_version": os.environ.get("RELEASE_VERSION", "dev"),
        "git_sha": os.environ.get("GIT_SHA", "unknown"),
        "source_branch": os.environ.get("SOURCE_BRANCH", "unknown"),
        "build_utc": os.environ.get("BUILD_UTC", "unknown"),
        "image_name": os.environ.get("UI_IMAGE_NAME", "object-storage-heatwave-ui"),
        "image_tag": os.environ.get("UI_IMAGE_TAG", "dev"),
        "image_digest": os.environ.get("IMAGE_DIGEST", ""),
        "config_schema_version": os.environ.get("CONFIG_SCHEMA_VERSION", "2"),
    }


def processor_release_from_image(image_url: str) -> dict[str, str]:
    """Record selected OCI image provenance without reading any secret."""
    image_name, _, image_tag = image_url.rpartition(":")
    return {
        "release_version": image_tag or "unknown",
        "git_sha": "image-tag-only",
        "source_branch": "image-tag-only",
        "build_utc": "unknown",
        "image_name": image_name or image_url,
        "image_tag": image_tag or "unknown",
        "image_digest": "",
        "config_schema_version": os.environ.get("CONFIG_SCHEMA_VERSION", "2"),
    }
