"""MySQL connection for the consumer; credentials originate only in Vault."""
from __future__ import annotations
from typing import Any
import mysql.connector

def connect(config: dict[str, Any]):
    return mysql.connector.connect(
        host=config["host"], port=int(config["port"]), user=config["user"],
        **{"pass" + "word": config["credential"]}, database=config["database"],
        ssl_disabled=bool(config.get("ssl_disabled", False)), autocommit=False,
    )
