"""Invoke the existing Object Storage loader logic from a durable capture."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any


def process_event(event: dict[str, Any]) -> None:
    """Run the shared loader path and let failures remain durable/retryable."""
    loader_dir = Path(__file__).with_name("function_loader")
    if str(loader_dir) not in sys.path:
        sys.path.insert(0, str(loader_dir))
    from func import process_cloud_event  # imported only in the image; keeps local tests light
    process_cloud_event(event)
