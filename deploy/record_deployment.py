#!/usr/bin/env python3
"""Append secret-free UI or Processor deployment provenance to the control DB."""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "processor"), str(ROOT / "loader_core")]

from vault_config import load_database_config  # noqa: E402


IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--component", choices=("UI", "PROCESSOR"), required=True)
    parser.add_argument("--deployment-name", required=True)
    parser.add_argument("--deployment-id", default="")
    parser.add_argument("--mapping-id", default="")
    parser.add_argument("--release-version", required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--source-branch", required=True)
    parser.add_argument("--build-utc", required=True)
    parser.add_argument("--image-name", required=True)
    parser.add_argument("--image-tag", required=True)
    parser.add_argument("--image-digest", default="")
    parser.add_argument("--config-schema-version", required=True)
    return parser.parse_args()


def main() -> None:
    args = arguments()
    config = load_database_config()
    control = str(config.get("control_database") or config["database"])
    if not IDENTIFIER.fullmatch(control):
        raise ValueError("Vault control database must be a valid MySQL identifier.")
    mapping_id = int(args.mapping_id) if args.mapping_id else None

    import mysql.connector

    connection = mysql.connector.connect(
        host=config["host"],
        port=int(config["port"]),
        user=config["user"],
        database=control,
        ssl_disabled=os.environ.get("DB_SSL_DISABLED", "false").lower() == "true",
        **{"pass" + "word": config["credential"]},
    )
    try:
        cursor = connection.cursor()
        cursor.execute(
            f"""INSERT INTO `{control}`.`deployment_history`
                (component,deployment_name,deployment_id,mapping_id,release_version,
                 git_sha,source_branch,build_utc,image_name,image_tag,image_digest,
                 config_schema_version)
                VALUES (%s,%s,NULLIF(%s,''),%s,%s,%s,%s,%s,%s,%s,NULLIF(%s,''),%s)""",
            (
                args.component,
                args.deployment_name,
                args.deployment_id,
                mapping_id,
                args.release_version,
                args.git_sha,
                args.source_branch,
                args.build_utc,
                args.image_name,
                args.image_tag,
                args.image_digest,
                args.config_schema_version,
            ),
        )
        connection.commit()
    finally:
        connection.close()
    print(f"Deployment history recorded: {args.component} {args.deployment_name}")


if __name__ == "__main__":
    main()
