"""Optional server-owned SSH tunnel creation."""

from __future__ import annotations

from sshtunnel import SSHTunnelForwarder


def open_tunnel(profile: dict, key_path) -> SSHTunnelForwarder:
    if not key_path or not key_path.is_file():
        raise ValueError("This SSH profile needs a private key uploaded by a profile manager.")
    tunnel = SSHTunnelForwarder(
        (profile["ssh_host"], profile["ssh_port"]),
        ssh_username=profile["ssh_user"],
        ssh_pkey=str(key_path),
        remote_bind_address=(profile["host"], profile["port"]),
        local_bind_address=("127.0.0.1", 0),
    )
    tunnel.start()
    return tunnel
