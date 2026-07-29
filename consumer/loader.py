"""Invoke the existing Object Storage loader logic from a durable capture."""
from __future__ import annotations

import io
import json
import os
import sys
from pathlib import Path
from typing import Any


def process_event(event: dict[str, Any]) -> None:
    """Run the prior loader handler and convert its non-2xx response to retry."""
    loader_dir = Path(__file__).with_name("function_loader")
    if str(loader_dir) not in sys.path:
        sys.path.insert(0, str(loader_dir))
    from func import handler  # imported only in the image; keeps local tests light
    response = handler(None, io.BytesIO(json.dumps(event, separators=(",", ":")).encode("utf-8")))
    if int(getattr(response, "status_code", 500)) >= 300:
        raise RuntimeError("Object Storage loader returned a non-success response.")
