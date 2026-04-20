from __future__ import annotations

import json
import socket
import time
from contextlib import contextmanager
from pathlib import Path


class CachedDnsResolver:
    def __init__(self, cache_path: Path, ttl_seconds: int = 21600):
        self.cache_path = cache_path
        self.ttl_seconds = max(300, ttl_seconds)

    def _load_cache(self) -> dict:
        if not self.cache_path.exists():
            return {}
        try:
            return json.loads(self.cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def _save_cache(self, data: dict) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")

    def _resolve_host_ip(self, host: str) -> str | None:
        try:
            rows = socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)
            if rows:
                return rows[0][4][0]
        except OSError:
            pass

        try:
            rows = socket.getaddrinfo(host, 443, socket.AF_UNSPEC, socket.SOCK_STREAM)
            if rows:
                return rows[0][4][0]
        except OSError:
            return None
        return None

    def get_or_resolve_ip(self, host: str) -> str | None:
        now = int(time.time())
        cache = self._load_cache()
        entry = cache.get(host)
        if isinstance(entry, dict):
            ip = str(entry.get("ip", "")).strip()
            ts = int(entry.get("ts", 0) or 0)
            if ip and (now - ts) <= self.ttl_seconds:
                return ip

        ip = self._resolve_host_ip(host)
        if not ip:
            return None

        cache[host] = {"ip": ip, "ts": now}
        self._save_cache(cache)
        return ip

    @contextmanager
    def route_host_to_ip(self, host: str, ip: str):
        original = socket.getaddrinfo

        def patched_getaddrinfo(target, port, family=0, type=0, proto=0, flags=0):
            if target == host:
                return original(ip, port, family, type, proto, flags)
            return original(target, port, family, type, proto, flags)

        socket.getaddrinfo = patched_getaddrinfo
        try:
            yield
        finally:
            socket.getaddrinfo = original
