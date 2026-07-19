#!/usr/bin/env python3
"""Generate deterministic CSV data accepted by the performance target."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    size = parser.add_mutually_exclusive_group(required=True)
    size.add_argument("--rows", type=int)
    size.add_argument("--target-bytes", type=int)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--start-id", type=int, default=1)
    parser.add_argument("--payload-bytes", type=int, default=128)
    args = parser.parse_args()
    if (args.rows is not None and args.rows < 1) or (args.target_bytes is not None and args.target_bytes < 1) or args.payload_bytes < 1:
        raise SystemExit("row, byte, and payload sizes must be positive")
    if args.payload_bytes > 512:
        raise SystemExit("--payload-bytes cannot exceed the target payload column size of 512")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    payload = "x" * args.payload_bytes
    header = b"record_id,event_ts,category,payload,amount\n"
    written = len(header)
    rows = 0
    with args.output.open("wb") as stream:
        stream.write(header)
        while args.rows is None or rows < args.rows:
            if args.target_bytes is not None and written >= args.target_bytes:
                break
            offset = rows
            record_id = args.start_id + offset
            line = f"{record_id},2026-07-19 12:00:00.000000,group-{record_id % 16:02d},{payload},{record_id % 10000}.25\n".encode()
            stream.write(line)
            written += len(line)
            rows += 1
    metadata = {"rows": rows, "bytes": written, "path": str(args.output)}
    if args.metadata:
        args.metadata.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"Generated {rows} rows and {written} bytes at {args.output}")


if __name__ == "__main__":
    main()
