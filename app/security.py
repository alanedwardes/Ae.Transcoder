from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlparse


def validate_source_url(src: str) -> None:
    u = urlparse(src)
    if u.scheme not in ("http", "https"):
        raise ValueError("src must be http or https")
    if not u.netloc:
        raise ValueError("src missing host")

    try:
        infos = socket.getaddrinfo(u.hostname, None)
    except Exception:
        raise ValueError("unable to resolve src host")

    for family, _, _, _, sockaddr in infos:
        ip_str = sockaddr[0]
        try:
            ip = ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved:
            raise ValueError("src host resolves to a non-public IP")


