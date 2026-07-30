"""Secret-free processor build provenance."""
from __future__ import annotations

import os


def release_metadata() -> dict[str, str]:
    return {
        "release_version": os.environ.get("RELEASE_VERSION", "dev"),
        "git_sha": os.environ.get("GIT_SHA", "unknown"),
        "source_branch": os.environ.get("SOURCE_BRANCH", "unknown"),
        "build_utc": os.environ.get("BUILD_UTC", "unknown"),
        "image_name": os.environ.get("PROCESSOR_IMAGE_NAME", "object-storage-stream-processor"),
        "image_tag": os.environ.get("PROCESSOR_IMAGE_TAG", "dev"),
        "image_digest": os.environ.get("IMAGE_DIGEST", ""),
        "config_schema_version": os.environ.get("CONFIG_SCHEMA_VERSION", "2"),
    }


def release_stamp() -> str:
    value = release_metadata()
    return "|".join((value["release_version"], value["git_sha"], value["build_utc"], value["config_schema_version"]))[:255]
