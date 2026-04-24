
from __future__ import annotations

"""
Original free replacement for paid AKShare proxy patch packages.

Features
- Monkey-patches requests.Session.request
- Injects per-request proxies only for selected domains
- Supports static proxy pool and/or fetching proxies from an API endpoint
- On proxy failure, can re-fetch a fresh proxy from API and retry automatically
- Failed proxies enter cooldown and are skipped on later retries
- Supports proxy pool from txt/json files
- Optional traffic logging (best-effort HTTP/application-layer estimate)
- Optional logging to console/file

Notes on traffic logging
- request bytes are estimated from the prepared HTTP request line + headers + body size
- response header bytes are estimated from the response status line + headers
- response body bytes at the application layer are measured from response.content when stream=False
- response body bytes on the wire are approximated from Content-Length when provided by server
- this is NOT exact TCP/TLS wire traffic; it does not include TLS handshakes, retransmissions,
  proxy CONNECT overhead, chunk framing, or other lower-layer details

This is an original implementation, not copied from any third-party package.
"""

from dataclasses import dataclass, field
from http.client import RemoteDisconnected
from pathlib import Path
from threading import RLock
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import quote, urlparse

import json
import logging
import random
import re
import time

import requests
from requests import Request, Response
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectTimeout,
    ConnectionError,
    ProxyError,
    ReadTimeout,
    SSLError,
)


LOGGER = logging.getLogger("akshare_proxy_patch_free")
LOGGER.addHandler(logging.NullHandler())


@dataclass
class ProxyRuntimeState:
    proxy_url: str
    source: str = "static"  # static, file, api
    total_success: int = 0
    total_failures: int = 0
    consecutive_failures: int = 0
    last_error: str | None = None
    last_used_at: float = 0.0
    cooldown_until: float = 0.0
    disabled: bool = False
    disabled_reason: str | None = None

    @property
    def in_cooldown(self) -> bool:
        return time.time() < self.cooldown_until


@dataclass
class ProxyPatchConfig:
    proxy_urls: list[str]
    hook_domains: list[str]
    retries: int = 3
    backoff: float = 1.5
    backoff_max: float = 30.0
    timeout: float | tuple[float, float] | None = None
    verify: bool | str | None = None
    disable_env_proxy: bool = True
    extra_headers: dict[str, str] = field(default_factory=dict)
    rotate: str = "health"  # health, cycle, random, first
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504)
    retry_non_idempotent: bool = False
    cooldown_base: float = 20.0
    cooldown_max: float = 300.0
    failure_threshold: int = 1
    ban_after_total_failures: int | None = None
    traffic_log: bool = False
    traffic_log_failures: bool = True
    redact_proxy_auth: bool = True

    # proxy API source
    proxy_api_url: str | None = None
    proxy_api_method: str = "GET"
    proxy_api_headers: dict[str, str] = field(default_factory=dict)
    proxy_api_params: dict[str, Any] = field(default_factory=dict)
    proxy_api_data: Any = None
    proxy_api_json: Any = None
    proxy_api_timeout: float | tuple[float, float] | None = 10.0
    proxy_api_scheme: str = "http"
    proxy_api_username: str | None = None
    proxy_api_password: str | None = None
    proxy_api_verify: bool | str | None = None
    proxy_api_refresh_on_failure: bool = True
    proxy_api_min_interval: float = 0.0
    proxy_api_max_fetch_tries: int = 3


_TRANSIENT_EXCEPTIONS = (
    ProxyError,
    ConnectTimeout,
    ReadTimeout,
    ConnectionError,
    ChunkedEncodingError,
    RemoteDisconnected,
    SSLError,
)

_LOCK = RLock()
_ORIGINAL_REQUEST = None
_CURRENT_CONFIG: ProxyPatchConfig | None = None
_CURRENT_POOL = None
_PATCH_INSTALLED = False


# ---------- general helpers ----------

def _normalize_domains(domains: Iterable[str]) -> list[str]:
    cleaned: list[str] = []
    for domain in domains:
        d = str(domain or "").strip().lower()
        if not d:
            continue
        if d.startswith("http://") or d.startswith("https://"):
            d = urlparse(d).hostname or d
        cleaned.append(d)
    return list(dict.fromkeys(cleaned))


def _match_domain(hostname: str | None, hook_domains: Sequence[str]) -> bool:
    if not hostname:
        return False
    host = hostname.lower()
    for domain in hook_domains:
        if host == domain or host.endswith("." + domain):
            return True
    return False


def _merge_headers(base: Mapping[str, str] | None, extra: Mapping[str, str] | None) -> dict[str, str]:
    merged: dict[str, str] = {}
    if base:
        merged.update({str(k): str(v) for k, v in base.items()})
    if extra:
        merged.update({str(k): str(v) for k, v in extra.items()})
    return merged


def _build_proxy_url(
    *,
    proxy_host: str,
    proxy_port: int | str,
    proxy_scheme: str = "http",
    username: str | None = None,
    password: str | None = None,
) -> str:
    auth = ""
    if username is not None:
        auth = quote(username, safe="")
        if password is not None:
            auth += ":" + quote(password, safe="")
        auth += "@"
    return f"{proxy_scheme}://{auth}{proxy_host}:{proxy_port}"


def _redact_proxy_url(proxy_url: str | None, enabled: bool = True) -> str | None:
    if proxy_url is None or not enabled:
        return proxy_url
    parsed = urlparse(proxy_url)
    if parsed.username is None:
        return proxy_url
    netloc = parsed.hostname or ""
    if parsed.port:
        netloc += f":{parsed.port}"
    return f"{parsed.scheme}://***@{netloc}"


def _human_size(num: int | float | None) -> str:
    if num is None:
        return "-"
    size = float(num)
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024 or unit == "GB":
            if unit == "B":
                return f"{int(size)}{unit}"
            return f"{size:.2f}{unit}"
        size /= 1024.0
    return f"{size:.2f}GB"


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def load_proxy_urls(path: str | Path) -> list[str]:
    """
    Supported formats:
    - txt: one proxy per line, '#' comments allowed
    - json: ["http://...", ...] or {"proxy_urls": [...]} or {"proxies": [...]}
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))

    suffix = p.suffix.lower()
    if suffix == ".txt":
        rows = []
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            rows.append(line)
        if not rows:
            raise ValueError(f"no proxy found in {p}")
        return rows

    if suffix == ".json":
        obj = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(obj, list):
            rows = [str(x).strip() for x in obj if str(x).strip()]
        elif isinstance(obj, dict):
            raw = obj.get("proxy_urls") or obj.get("proxies") or []
            rows = [str(x).strip() for x in raw if str(x).strip()]
        else:
            raise ValueError(f"unsupported json structure in {p}")
        if not rows:
            raise ValueError(f"no proxy found in {p}")
        return rows

    raise ValueError(f"unsupported proxy file type: {suffix}")


def setup_logger(
    *,
    log_file: str | Path | None = None,
    level: str | int = "INFO",
    console: bool = True,
) -> logging.Logger:
    logger = LOGGER
    logger.handlers.clear()
    logger.setLevel(level)
    logger.propagate = False
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if console:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    if log_file:
        fh = logging.FileHandler(str(log_file), encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def _raw_request(method: str, url: str, **kwargs) -> Response:
    """Send a request bypassing our patch to avoid recursion (used for proxy API fetch)."""
    session = requests.Session()
    try:
        if kwargs.pop("disable_env_proxy", False):
            session.trust_env = False
        original = _ORIGINAL_REQUEST
        if original is not None:
            return original(session, method, url, **kwargs)
        return session.request(method, url, **kwargs)
    finally:
        session.close()


# ---------- proxy api helpers ----------

_COMMON_JSON_KEYS = (
    "proxy", "https", "http", "proxy_url", "ip_port", "addr", "endpoint", "data"
)
_IP_PORT_RE = re.compile(r"(?P<endpoint>[A-Za-z0-9\.-]+:\d{2,5})")


def _extract_endpoint_from_obj(obj: Any) -> str | None:
    if obj is None:
        return None
    if isinstance(obj, str):
        text = obj.strip()
        if not text:
            return None
        if "://" in text:
            return text
        m = _IP_PORT_RE.search(text)
        return m.group("endpoint") if m else text
    if isinstance(obj, list):
        for item in obj:
            endpoint = _extract_endpoint_from_obj(item)
            if endpoint:
                return endpoint
        return None
    if isinstance(obj, dict):
        ip = obj.get("ip") or obj.get("host")
        port = obj.get("port")
        if ip and port:
            return f"{ip}:{port}"
        for key in _COMMON_JSON_KEYS:
            if key in obj:
                endpoint = _extract_endpoint_from_obj(obj.get(key))
                if endpoint:
                    return endpoint
        for value in obj.values():
            endpoint = _extract_endpoint_from_obj(value)
            if endpoint:
                return endpoint
    return None


def _parse_proxy_api_response(response: Response) -> str:
    content_type = (response.headers.get("Content-Type") or "").lower()
    text = response.text.strip()

    if "json" in content_type or (text.startswith("{") or text.startswith("[")):
        try:
            obj = response.json()
        except Exception:
            obj = None
        endpoint = _extract_endpoint_from_obj(obj)
        if endpoint:
            return endpoint

    endpoint = _extract_endpoint_from_obj(text)
    if endpoint:
        return endpoint

    raise ValueError(f"cannot parse proxy endpoint from proxy API response: {text[:120]!r}")


def _endpoint_to_proxy_url(
    endpoint: str,
    *,
    scheme: str,
    username: str | None,
    password: str | None,
) -> str:
    raw = str(endpoint).strip()
    if not raw:
        raise ValueError("empty proxy endpoint")
    if "://" in raw:
        return raw
    if ":" not in raw:
        raise ValueError(f"invalid proxy endpoint: {raw}")
    host, port = raw.rsplit(":", 1)
    return _build_proxy_url(
        proxy_host=host,
        proxy_port=port,
        proxy_scheme=scheme,
        username=username,
        password=password,
    )


# ---------- traffic estimation ----------

def _estimate_request_bytes(session: requests.Session, method: str, url: str, kwargs: dict[str, Any]) -> int | None:
    try:
        req = Request(
            method=method.upper(),
            url=url,
            headers=kwargs.get("headers"),
            files=kwargs.get("files"),
            data=kwargs.get("data"),
            json=kwargs.get("json"),
            params=kwargs.get("params"),
            auth=kwargs.get("auth"),
            cookies=kwargs.get("cookies"),
        )
        prep = session.prepare_request(req)
        total = 0
        path = prep.path_url or "/"
        total += len(f"{prep.method} {path} HTTP/1.1\r\n".encode("utf-8", errors="ignore"))
        for k, v in prep.headers.items():
            total += len(f"{k}: {v}\r\n".encode("utf-8", errors="ignore"))
        total += 2
        body = prep.body
        if body is None:
            pass
        elif isinstance(body, bytes):
            total += len(body)
        elif isinstance(body, str):
            total += len(body.encode("utf-8", errors="ignore"))
        else:
            total += len(str(body).encode("utf-8", errors="ignore"))
        return total
    except Exception:
        return None


def _estimate_response_header_bytes(response: Response) -> int | None:
    try:
        version_map = {10: "1.0", 11: "1.1", 20: "2"}
        version_num = getattr(getattr(response.raw, "version", None), "real", None)
        if version_num is None:
            version_num = getattr(response.raw, "version", 11)
        version = version_map.get(version_num, "1.1")
        reason = response.reason or ""
        total = len(f"HTTP/{version} {response.status_code} {reason}\r\n".encode("utf-8", errors="ignore"))
        for k, v in response.headers.items():
            total += len(f"{k}: {v}\r\n".encode("utf-8", errors="ignore"))
        total += 2
        return total
    except Exception:
        return None


def _estimate_response_body_app_bytes(response: Response, *, stream: bool) -> int | None:
    if stream:
        return None
    try:
        return len(response.content)
    except Exception:
        try:
            cached = getattr(response, "_content", None)
            if isinstance(cached, (bytes, bytearray)):
                return len(cached)
        except Exception:
            return None
    return None


def _estimate_response_body_wire_bytes(response: Response, *, fallback_app_bytes: int | None) -> int | None:
    header_val = response.headers.get("Content-Length")
    if header_val is not None:
        parsed = _safe_int(header_val)
        if parsed is not None:
            return parsed
    return fallback_app_bytes


# ---------- pool ----------

class ProxyPool:
    def __init__(
        self,
        proxy_urls: Sequence[str],
        *,
        rotate: str = "health",
        cooldown_base: float = 20.0,
        cooldown_max: float = 300.0,
        failure_threshold: int = 1,
        ban_after_total_failures: int | None = None,
    ):
        cleaned = [str(p).strip() for p in proxy_urls if str(p).strip()]
        self._lock = RLock()
        self._api_lock = RLock()
        self._states: list[ProxyRuntimeState] = [ProxyRuntimeState(proxy_url=p) for p in dict.fromkeys(cleaned)]
        self.rotate = rotate
        self.cooldown_base = float(cooldown_base)
        self.cooldown_max = float(cooldown_max)
        self.failure_threshold = max(1, int(failure_threshold))
        self.ban_after_total_failures = (
            None if ban_after_total_failures is None else max(1, int(ban_after_total_failures))
        )
        self._rr_index = 0
        self._last_api_fetch_at = 0.0

    def _healthy_states(self) -> list[ProxyRuntimeState]:
        now = time.time()
        return [s for s in self._states if not s.disabled and s.cooldown_until <= now]

    def _sorted_by_health(self, states: Sequence[ProxyRuntimeState]) -> list[ProxyRuntimeState]:
        return sorted(
            states,
            key=lambda s: (
                s.consecutive_failures,
                s.total_failures,
                -s.total_success,
                s.last_used_at,
            ),
        )

    def acquire(self, *, exclude: set[str] | None = None) -> ProxyRuntimeState:
        exclude = exclude or set()
        with self._lock:
            available = [s for s in self._healthy_states() if s.proxy_url not in exclude]
            if not available:
                fallback = [s for s in self._states if not s.disabled and s.proxy_url not in exclude]
                if not fallback:
                    fallback = [s for s in self._states if not s.disabled]
                if not fallback:
                    raise RuntimeError("all proxies are disabled; no proxy available")
                chosen = min(fallback, key=lambda s: (s.cooldown_until, s.consecutive_failures, s.total_failures))
                chosen.last_used_at = time.time()
                return chosen

            if self.rotate == "first":
                chosen = available[0]
            elif self.rotate == "random":
                chosen = random.choice(available)
            elif self.rotate == "cycle":
                chosen = list(available)[self._rr_index % len(available)]
                self._rr_index += 1
            else:
                chosen = self._sorted_by_health(available)[0]

            chosen.last_used_at = time.time()
            return chosen

    def mark_success(self, proxy_url: str) -> None:
        with self._lock:
            state = self._find(proxy_url)
            state.total_success += 1
            state.consecutive_failures = 0
            state.last_error = None
            state.cooldown_until = 0.0
            state.last_used_at = time.time()

    def mark_failure(self, proxy_url: str, error: str | None = None) -> None:
        with self._lock:
            state = self._find(proxy_url)
            state.total_failures += 1
            state.consecutive_failures += 1
            state.last_error = error
            state.last_used_at = time.time()

            if self.ban_after_total_failures is not None and state.total_failures >= self.ban_after_total_failures:
                state.disabled = True
                state.disabled_reason = f"too many failures: {state.total_failures}"
                state.cooldown_until = 0.0
                return

            if state.consecutive_failures >= self.failure_threshold:
                fail_level = state.consecutive_failures - self.failure_threshold
                cooldown = min(self.cooldown_max, self.cooldown_base * (2 ** fail_level))
                state.cooldown_until = time.time() + cooldown

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            now = time.time()
            return [
                {
                    "proxy_url": s.proxy_url,
                    "source": s.source,
                    "total_success": s.total_success,
                    "total_failures": s.total_failures,
                    "consecutive_failures": s.consecutive_failures,
                    "last_error": s.last_error,
                    "cooldown_seconds_left": max(0.0, round(s.cooldown_until - now, 3)),
                    "last_used_at": s.last_used_at,
                    "disabled": s.disabled,
                    "disabled_reason": s.disabled_reason,
                }
                for s in self._states
            ]

    def reset_proxy(self, proxy_url: str) -> None:
        with self._lock:
            s = self._find(proxy_url)
            s.consecutive_failures = 0
            s.last_error = None
            s.cooldown_until = 0.0
            s.disabled = False
            s.disabled_reason = None

    def enable_all(self) -> None:
        with self._lock:
            for s in self._states:
                s.consecutive_failures = 0
                s.last_error = None
                s.cooldown_until = 0.0
                s.disabled = False
                s.disabled_reason = None

    def add_or_refresh_proxy(self, proxy_url: str, *, source: str = "api") -> ProxyRuntimeState:
        proxy_url = str(proxy_url).strip()
        if not proxy_url:
            raise ValueError("empty proxy_url")
        with self._lock:
            for s in self._states:
                if s.proxy_url == proxy_url:
                    s.source = source
                    s.disabled = False
                    s.disabled_reason = None
                    s.cooldown_until = 0.0
                    s.last_error = None
                    return s
            state = ProxyRuntimeState(proxy_url=proxy_url, source=source)
            self._states.append(state)
            return state

    def fetch_proxy_from_api(
        self,
        cfg: ProxyPatchConfig,
        *,
        exclude: set[str] | None = None,
        force: bool = False,
        reason: str | None = None,
    ) -> ProxyRuntimeState | None:
        if not cfg.proxy_api_url:
            return None
        exclude = exclude or set()
        now = time.time()
        if not force and cfg.proxy_api_min_interval > 0 and (now - self._last_api_fetch_at) < cfg.proxy_api_min_interval:
            with self._lock:
                available = [s for s in self._healthy_states() if s.proxy_url not in exclude]
                if available:
                    chosen = self._sorted_by_health(available)[0]
                    chosen.last_used_at = time.time()
                    return chosen

        with self._api_lock:
            for fetch_attempt in range(1, max(1, int(cfg.proxy_api_max_fetch_tries)) + 1):
                request_kwargs = {
                    "headers": dict(cfg.proxy_api_headers or {}),
                    "params": dict(cfg.proxy_api_params or {}),
                    "data": cfg.proxy_api_data,
                    "json": cfg.proxy_api_json,
                    "timeout": cfg.proxy_api_timeout,
                    "disable_env_proxy": cfg.disable_env_proxy,
                }
                if cfg.proxy_api_verify is not None:
                    request_kwargs["verify"] = cfg.proxy_api_verify

                LOGGER.info(
                    "proxy api fetch attempt=%s reason=%s url=%s",
                    fetch_attempt,
                    reason or "-",
                    cfg.proxy_api_url,
                )
                response = _raw_request(cfg.proxy_api_method.upper(), cfg.proxy_api_url, **request_kwargs)
                response.raise_for_status()
                endpoint = _parse_proxy_api_response(response)
                proxy_url = _endpoint_to_proxy_url(
                    endpoint,
                    scheme=cfg.proxy_api_scheme,
                    username=cfg.proxy_api_username,
                    password=cfg.proxy_api_password,
                )
                self._last_api_fetch_at = time.time()
                if proxy_url in exclude:
                    LOGGER.warning(
                        "proxy api returned excluded proxy=%s; refetching",
                        _redact_proxy_url(proxy_url, cfg.redact_proxy_auth),
                    )
                    continue
                state = self.add_or_refresh_proxy(proxy_url, source="api")
                LOGGER.info(
                    "proxy api got proxy=%s",
                    _redact_proxy_url(state.proxy_url, cfg.redact_proxy_auth),
                )
                return state
        return None

    def _find(self, proxy_url: str) -> ProxyRuntimeState:
        for s in self._states:
            if s.proxy_url == proxy_url:
                return s
        raise KeyError(f"proxy not found: {proxy_url}")


# ---------- public control helpers ----------

def _should_retry(method: str, status_code: int | None, cfg: ProxyPatchConfig) -> bool:
    if status_code is None:
        return True
    if status_code not in cfg.status_forcelist:
        return False
    if cfg.retry_non_idempotent:
        return True
    return method.upper() in {"GET", "HEAD", "OPTIONS"}


def install_patch(
    proxy_url: str | None = None,
    *,
    proxy_urls: Sequence[str] | None = None,
    proxy_file: str | Path | None = None,
    proxy_host: str | None = None,
    proxy_port: int | str | None = None,
    proxy_scheme: str = "http",
    username: str | None = None,
    password: str | None = None,
    # new: proxy api
    proxy_api_url: str | None = None,
    proxy_api_method: str = "GET",
    proxy_api_headers: Mapping[str, str] | None = None,
    proxy_api_params: Mapping[str, Any] | None = None,
    proxy_api_data: Any = None,
    proxy_api_json: Any = None,
    proxy_api_timeout: float | tuple[float, float] | None = 10.0,
    proxy_api_scheme: str | None = None,
    proxy_api_username: str | None = None,
    proxy_api_password: str | None = None,
    proxy_api_verify: bool | str | None = None,
    proxy_api_refresh_on_failure: bool = True,
    proxy_api_min_interval: float = 0.0,
    proxy_api_max_fetch_tries: int = 3,
    hook_domains: Sequence[str] | None = None,
    retries: int = 3,
    backoff: float = 1.5,
    backoff_max: float = 30.0,
    timeout: float | tuple[float, float] | None = None,
    verify: bool | str | None = None,
    disable_env_proxy: bool = True,
    extra_headers: Mapping[str, str] | None = None,
    rotate: str = "health",
    status_forcelist: Sequence[int] = (429, 500, 502, 503, 504),
    retry_non_idempotent: bool = False,
    cooldown_base: float = 20.0,
    cooldown_max: float = 300.0,
    failure_threshold: int = 1,
    ban_after_total_failures: int | None = None,
    traffic_log: bool = False,
    traffic_log_failures: bool = True,
    redact_proxy_auth: bool = True,
    log_file: str | Path | None = None,
    log_level: str | int = "INFO",
    log_console: bool = True,
) -> None:
    """
    Install a process-wide patch for requests.Session.request.

    Main features:
    - automatic proxy-pool failover on retry
    - failed proxies go into cooldown and later recover automatically
    - supports proxy pool from txt/json file
    - supports proxy API source that returns `ip:port` or full proxy URL
    - if current proxy fails, can fetch a fresh proxy from API and retry automatically
    - supports optional traffic logging to console/file
    """
    global _ORIGINAL_REQUEST, _CURRENT_CONFIG, _CURRENT_POOL, _PATCH_INSTALLED

    setup_logger(log_file=log_file, level=log_level, console=log_console)

    merged_proxy_urls: list[str] = []
    if proxy_file:
        merged_proxy_urls.extend(load_proxy_urls(proxy_file))
    if proxy_urls:
        merged_proxy_urls.extend([str(p).strip() for p in proxy_urls if str(p).strip()])
    if proxy_url:
        merged_proxy_urls.insert(0, str(proxy_url).strip())

    if proxy_host is not None and proxy_port is not None:
        merged_proxy_urls.append(
            _build_proxy_url(
                proxy_host=proxy_host,
                proxy_port=proxy_port,
                proxy_scheme=proxy_scheme,
                username=username,
                password=password,
            )
        )

    cfg = ProxyPatchConfig(
        proxy_urls=list(dict.fromkeys([p for p in merged_proxy_urls if p])),
        hook_domains=_normalize_domains(hook_domains or [
            "eastmoney.com",
            "push2.eastmoney.com",
            "push2his.eastmoney.com",
            "quote.eastmoney.com",
            "data.eastmoney.com",
        ]),
        retries=max(1, int(retries)),
        backoff=float(backoff),
        backoff_max=float(backoff_max),
        timeout=timeout,
        verify=verify,
        disable_env_proxy=disable_env_proxy,
        extra_headers=dict(extra_headers or {}),
        rotate=rotate,
        status_forcelist=tuple(int(x) for x in status_forcelist),
        retry_non_idempotent=retry_non_idempotent,
        cooldown_base=float(cooldown_base),
        cooldown_max=float(cooldown_max),
        failure_threshold=max(1, int(failure_threshold)),
        ban_after_total_failures=(None if ban_after_total_failures is None else max(1, int(ban_after_total_failures))),
        traffic_log=bool(traffic_log),
        traffic_log_failures=bool(traffic_log_failures),
        redact_proxy_auth=bool(redact_proxy_auth),
        proxy_api_url=str(proxy_api_url).strip() if proxy_api_url else None,
        proxy_api_method=str(proxy_api_method or "GET").upper(),
        proxy_api_headers=dict(proxy_api_headers or {}),
        proxy_api_params=dict(proxy_api_params or {}),
        proxy_api_data=proxy_api_data,
        proxy_api_json=proxy_api_json,
        proxy_api_timeout=proxy_api_timeout,
        proxy_api_scheme=proxy_api_scheme or proxy_scheme,
        proxy_api_username=proxy_api_username if proxy_api_username is not None else username,
        proxy_api_password=proxy_api_password if proxy_api_password is not None else password,
        proxy_api_verify=proxy_api_verify,
        proxy_api_refresh_on_failure=bool(proxy_api_refresh_on_failure),
        proxy_api_min_interval=float(proxy_api_min_interval),
        proxy_api_max_fetch_tries=max(1, int(proxy_api_max_fetch_tries)),
    )

    if not cfg.proxy_urls and not cfg.proxy_api_url:
        raise ValueError(
            "Provide proxy_url / proxy_urls / proxy_file / proxy_host+proxy_port, or provide proxy_api_url"
        )

    pool = ProxyPool(
        cfg.proxy_urls,
        rotate=cfg.rotate,
        cooldown_base=cfg.cooldown_base,
        cooldown_max=cfg.cooldown_max,
        failure_threshold=cfg.failure_threshold,
        ban_after_total_failures=cfg.ban_after_total_failures,
    )

    if not cfg.proxy_urls and cfg.proxy_api_url:
        state = pool.fetch_proxy_from_api(cfg, force=True, reason="initial")
        if state is None:
            raise RuntimeError("proxy_api_url was provided but failed to fetch initial proxy")

    LOGGER.info(
        "install patch: proxies=%s domains=%s rotate=%s retries=%s proxy_api=%s",
        len(pool.snapshot()),
        ",".join(cfg.hook_domains),
        cfg.rotate,
        cfg.retries,
        bool(cfg.proxy_api_url),
    )

    with _LOCK:
        _CURRENT_CONFIG = cfg
        _CURRENT_POOL = pool
        if _PATCH_INSTALLED:
            return

        _ORIGINAL_REQUEST = requests.sessions.Session.request

        def patched_request(self, method, url, **kwargs):
            cfg_local = _CURRENT_CONFIG
            pool_local: ProxyPool | None = _CURRENT_POOL
            if cfg_local is None or pool_local is None:
                return _ORIGINAL_REQUEST(self, method, url, **kwargs)

            hostname = urlparse(url).hostname
            if not _match_domain(hostname, cfg_local.hook_domains):
                return _ORIGINAL_REQUEST(self, method, url, **kwargs)

            if cfg_local.disable_env_proxy:
                self.trust_env = False

            request_kwargs = dict(kwargs)
            request_kwargs["headers"] = _merge_headers(kwargs.get("headers"), cfg_local.extra_headers)
            if cfg_local.timeout is not None and "timeout" not in request_kwargs:
                request_kwargs["timeout"] = cfg_local.timeout
            if cfg_local.verify is not None and "verify" not in request_kwargs:
                request_kwargs["verify"] = cfg_local.verify

            last_error = None
            tried_in_this_request: set[str] = set()
            method_upper = str(method).upper()
            stream_mode = bool(request_kwargs.get("stream", False))

            for attempt in range(1, cfg_local.retries + 1):
                try:
                    state = pool_local.acquire(exclude=tried_in_this_request)
                except RuntimeError:
                    if cfg_local.proxy_api_url:
                        state = pool_local.fetch_proxy_from_api(
                            cfg_local,
                            exclude=tried_in_this_request,
                            force=True,
                            reason="pool_exhausted",
                        )
                        if state is None:
                            raise
                    else:
                        raise

                proxy = state.proxy_url
                tried_in_this_request.add(proxy)

                call_kwargs = dict(request_kwargs)
                call_kwargs["proxies"] = {
                    "http": proxy,
                    "https": proxy,
                }

                req_bytes_est = None
                if cfg_local.traffic_log:
                    req_bytes_est = _estimate_request_bytes(self, method_upper, url, call_kwargs)

                LOGGER.info(
                    "request attempt=%s method=%s host=%s proxy=%s%s",
                    attempt,
                    method_upper,
                    hostname,
                    _redact_proxy_url(proxy, cfg_local.redact_proxy_auth),
                    f" req_est={_human_size(req_bytes_est)}" if cfg_local.traffic_log else "",
                )

                started = time.perf_counter()
                try:
                    response: Response = _ORIGINAL_REQUEST(self, method, url, **call_kwargs)
                    elapsed_ms = round((time.perf_counter() - started) * 1000, 2)

                    resp_hdr_est = None
                    resp_body_app = None
                    resp_body_wire = None
                    if cfg_local.traffic_log:
                        resp_hdr_est = _estimate_response_header_bytes(response)
                        resp_body_app = _estimate_response_body_app_bytes(response, stream=stream_mode)
                        resp_body_wire = _estimate_response_body_wire_bytes(response, fallback_app_bytes=resp_body_app)
                        total_app = (req_bytes_est or 0) + (resp_hdr_est or 0) + (resp_body_app or 0)
                        total_wire_est = (req_bytes_est or 0) + (resp_hdr_est or 0) + (resp_body_wire or 0)
                        LOGGER.info(
                            "traffic attempt=%s method=%s host=%s status=%s elapsed_ms=%s proxy=%s req_est=%s resp_hdr_est=%s resp_body_app=%s resp_body_wire=%s total_app=%s total_wire_est=%s encoding=%s",
                            attempt,
                            method_upper,
                            hostname,
                            response.status_code,
                            elapsed_ms,
                            _redact_proxy_url(proxy, cfg_local.redact_proxy_auth),
                            _human_size(req_bytes_est),
                            _human_size(resp_hdr_est),
                            _human_size(resp_body_app),
                            _human_size(resp_body_wire),
                            _human_size(total_app),
                            _human_size(total_wire_est),
                            response.headers.get("Content-Encoding", "identity"),
                        )

                    if attempt < cfg_local.retries and _should_retry(method_upper, response.status_code, cfg_local):
                        pool_local.mark_failure(proxy, f"HTTP {response.status_code}")
                        LOGGER.warning(
                            "retry status=%s proxy=%s attempt=%s/%s",
                            response.status_code,
                            _redact_proxy_url(proxy, cfg_local.redact_proxy_auth),
                            attempt,
                            cfg_local.retries,
                        )
                        if cfg_local.proxy_api_url and cfg_local.proxy_api_refresh_on_failure:
                            try:
                                pool_local.fetch_proxy_from_api(
                                    cfg_local,
                                    exclude=tried_in_this_request,
                                    force=True,
                                    reason=f"http_{response.status_code}",
                                )
                            except Exception as api_exc:
                                LOGGER.warning("proxy api refresh failed after status retry trigger: %s", api_exc)
                        response.close()
                        sleep_s = min(
                            cfg_local.backoff_max,
                            cfg_local.backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.7),
                        )
                        time.sleep(sleep_s)
                        continue

                    pool_local.mark_success(proxy)
                    LOGGER.info(
                        "success status=%s proxy=%s attempt=%s",
                        response.status_code,
                        _redact_proxy_url(proxy, cfg_local.redact_proxy_auth),
                        attempt,
                    )
                    return response
                except _TRANSIENT_EXCEPTIONS as exc:
                    elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
                    last_error = exc
                    pool_local.mark_failure(proxy, f"{type(exc).__name__}: {exc}")
                    log_msg = (
                        f"exception={type(exc).__name__} proxy={_redact_proxy_url(proxy, cfg_local.redact_proxy_auth)} "
                        f"attempt={attempt}/{cfg_local.retries} elapsed_ms={elapsed_ms}"
                    )
                    if cfg_local.traffic_log and cfg_local.traffic_log_failures:
                        log_msg += f" req_est={_human_size(req_bytes_est)}"
                    LOGGER.warning(log_msg)

                    if cfg_local.proxy_api_url and cfg_local.proxy_api_refresh_on_failure:
                        try:
                            pool_local.fetch_proxy_from_api(
                                cfg_local,
                                exclude=tried_in_this_request,
                                force=True,
                                reason=f"{type(exc).__name__}",
                            )
                        except Exception as api_exc:
                            LOGGER.warning("proxy api refresh failed after exception: %s", api_exc)

                    if attempt >= cfg_local.retries:
                        raise
                    sleep_s = min(
                        cfg_local.backoff_max,
                        cfg_local.backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.7),
                    )
                    time.sleep(sleep_s)

            if last_error is not None:
                raise last_error
            return _ORIGINAL_REQUEST(self, method, url, **kwargs)

        requests.sessions.Session.request = patched_request
        _PATCH_INSTALLED = True


def uninstall_patch() -> None:
    global _ORIGINAL_REQUEST, _CURRENT_CONFIG, _CURRENT_POOL, _PATCH_INSTALLED
    with _LOCK:
        _CURRENT_CONFIG = None
        _CURRENT_POOL = None
        if _PATCH_INSTALLED and _ORIGINAL_REQUEST is not None:
            requests.sessions.Session.request = _ORIGINAL_REQUEST
        _ORIGINAL_REQUEST = None
        _PATCH_INSTALLED = False
        LOGGER.info("patch uninstalled")


def update_patch(**kwargs) -> None:
    uninstall_patch()
    install_patch(**kwargs)


def is_installed() -> bool:
    return _PATCH_INSTALLED


def current_config() -> ProxyPatchConfig | None:
    return _CURRENT_CONFIG


def proxy_pool_stats() -> list[dict[str, Any]]:
    if _CURRENT_POOL is None:
        return []
    return _CURRENT_POOL.snapshot()


def reset_proxy(proxy_url: str) -> None:
    if _CURRENT_POOL is None:
        raise RuntimeError("patch is not installed")
    _CURRENT_POOL.reset_proxy(proxy_url)


def enable_all_proxies() -> None:
    if _CURRENT_POOL is None:
        raise RuntimeError("patch is not installed")
    _CURRENT_POOL.enable_all()


def refresh_proxy_from_api(force: bool = True, reason: str | None = "manual") -> str | None:
    if _CURRENT_POOL is None or _CURRENT_CONFIG is None:
        raise RuntimeError("patch is not installed")
    state = _CURRENT_POOL.fetch_proxy_from_api(_CURRENT_CONFIG, force=force, reason=reason)
    return None if state is None else state.proxy_url


if __name__ == "__main__":
    install_patch(
        proxy_urls=[
            "http://127.0.0.1:7890",
            "http://127.0.0.1:7891",
        ],
        hook_domains=[
            "eastmoney.com",
            "push2.eastmoney.com",
            "push2his.eastmoney.com",
            "quote.eastmoney.com",
            "data.eastmoney.com",
        ],
        retries=5,
        traffic_log=True,
    )
    print(proxy_pool_stats())
