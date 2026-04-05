import hashlib
import json
import os
import platform
import socket
import time
import urllib.error
import urllib.request
import uuid
from typing import Dict, Optional, Tuple

try:
    import winreg
except Exception:
    winreg = None

_DIRECT_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def machine_fingerprint() -> str:
    machine_guid = ""
    if winreg is not None:
        try:
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Cryptography") as key:
                machine_guid = str(winreg.QueryValueEx(key, "MachineGuid")[0] or "")
        except Exception:
            machine_guid = ""
    raw = "|".join(
        [
            platform.system(),
            platform.machine(),
            platform.version(),
            socket.gethostname(),
            machine_guid,
            str(uuid.getnode()),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _normalize_server_url(server_url: str) -> str:
    server_url = str(server_url or "").strip().rstrip("/")
    if server_url and "://" not in server_url:
        server_url = "http://" + server_url
    return server_url


def _post_json(url: str, payload: Dict, timeout: int = 8) -> Dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with _DIRECT_OPENER.open(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _handle_http_error(exc: urllib.error.HTTPError) -> Tuple[bool, str, Optional[Dict]]:
    try:
        data = json.loads(exc.read().decode("utf-8"))
        return False, str(data.get("detail") or f"HTTP {exc.code}"), None
    except Exception:
        return False, f"HTTP {exc.code}", None


def activate(
    server_url: str,
    license_key: str,
    app_version: str = "",
    custom_name: str = "",
    machine_token: str = "",
) -> Tuple[bool, str, Optional[Dict]]:
    server_url = _normalize_server_url(server_url)
    if not server_url:
        return False, "授权服务器地址为空", None
    if not license_key:
        return False, "授权码为空", None
    payload = {
        "license_key": license_key.strip(),
        "machine_id": machine_fingerprint(),
        "app_version": app_version,
        "device_name": custom_name or socket.gethostname(),
        "machine_token": str(machine_token or "").strip(),
    }
    try:
        data = _post_json(f"{server_url}/api/activate", payload)
    except urllib.error.HTTPError as exc:
        return _handle_http_error(exc)
    except Exception as exc:
        return False, f"连接失败: {exc}", None
    ok = bool(data.get("ok"))
    msg = str(data.get("message") or ("激活成功" if ok else "激活失败"))
    return ok, msg, data


def heartbeat(
    server_url: str,
    license_key: str,
    app_version: str = "",
    machine_token: str = "",
) -> Tuple[bool, str, Optional[Dict]]:
    server_url = _normalize_server_url(server_url)
    if not server_url or not license_key:
        return False, "缺少服务器地址或授权码", None
    payload = {
        "license_key": license_key.strip(),
        "machine_id": machine_fingerprint(),
        "app_version": app_version,
        "machine_token": str(machine_token or "").strip(),
    }
    try:
        data = _post_json(f"{server_url}/api/heartbeat", payload)
    except urllib.error.HTTPError as exc:
        return _handle_http_error(exc)
    except Exception as exc:
        return False, f"连接失败: {exc}", None
    ok = bool(data.get("ok"))
    msg = str(data.get("message") or ("授权正常" if ok else "授权失败"))
    return ok, msg, data


def save_license_cache(path: str, data: Dict) -> None:
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def load_license_cache(path: str) -> Dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def now_ts() -> int:
    return int(time.time())


def resolve_remote_permanent_id(
    server_url: str,
    license_key: str,
    unique_id: str,
    display_name: str = "",
    app_version: str = "",
    machine_token: str = "",
) -> Tuple[bool, str, Optional[Dict]]:
    server_url = _normalize_server_url(server_url)
    if not server_url or not license_key or not unique_id:
        return False, "缺少服务器地址/授权码/unique_id", None
    payload = {
        "license_key": license_key.strip(),
        "machine_id": machine_fingerprint(),
        "unique_id": unique_id.strip(),
        "display_name": display_name or "",
        "app_version": app_version or "",
        "machine_token": str(machine_token or "").strip(),
    }
    try:
        data = _post_json(f"{server_url}/api/permanent-ids/resolve", payload, timeout=10)
    except urllib.error.HTTPError as exc:
        return _handle_http_error(exc)
    except Exception as exc:
        return False, f"连接失败: {exc}", None
    return bool(data.get("ok")), str(data.get("message") or "ok"), data


def relay_start_room(
    server_url: str,
    license_key: str,
    room_unique_id: str,
    app_version: str = "",
    machine_token: str = "",
    proxy: str = "",
    proxy_route_mode: str = "direct",
    sign_api_base: str = "",
    sign_api_key: str = "",
    use_sign_api_key: bool = False,
    ssl_insecure: bool = True,
) -> Tuple[bool, str, Optional[Dict]]:
    server_url = _normalize_server_url(server_url)
    if not server_url or not license_key or not room_unique_id:
        return False, "missing server_url/license_key/room_unique_id", None
    payload = {
        "license_key": license_key.strip(),
        "machine_id": machine_fingerprint(),
        "room_unique_id": room_unique_id.strip().lstrip("@"),
        "app_version": app_version or "",
        "machine_token": str(machine_token or "").strip(),
        "proxy": proxy or "",
        "proxy_route_mode": proxy_route_mode or "direct",
        "sign_api_base": sign_api_base or "",
        "sign_api_key": sign_api_key or "",
        "use_sign_api_key": bool(use_sign_api_key),
        "ssl_insecure": bool(ssl_insecure),
    }
    try:
        data = _post_json(f"{server_url}/api/relay/rooms/start", payload, timeout=15)
    except urllib.error.HTTPError as exc:
        return _handle_http_error(exc)
    except Exception as exc:
        return False, f"connect failed: {exc}", None
    return bool(data.get("ok")), str(data.get("message") or "ok"), data


def relay_stop_room(server_url: str, license_key: str, room_unique_id: str, app_version: str = "", machine_token: str = "") -> Tuple[bool, str, Optional[Dict]]:
    server_url = _normalize_server_url(server_url)
    if not server_url or not license_key or not room_unique_id:
        return False, "missing server_url/license_key/room_unique_id", None
    payload = {
        "license_key": license_key.strip(),
        "machine_id": machine_fingerprint(),
        "room_unique_id": room_unique_id.strip().lstrip("@"),
        "app_version": app_version or "",
        "machine_token": str(machine_token or "").strip(),
    }
    try:
        data = _post_json(f"{server_url}/api/relay/rooms/stop", payload, timeout=10)
    except urllib.error.HTTPError as exc:
        return _handle_http_error(exc)
    except Exception as exc:
        return False, f"connect failed: {exc}", None
    return bool(data.get("ok")), str(data.get("message") or "ok"), data


def relay_list_rooms(server_url: str, license_key: str, app_version: str = "", machine_token: str = "") -> Tuple[bool, str, Optional[Dict]]:
    server_url = _normalize_server_url(server_url)
    if not server_url or not license_key:
        return False, "missing server_url/license_key", None
    payload = {
        "license_key": license_key.strip(),
        "machine_id": machine_fingerprint(),
        "app_version": app_version or "",
        "machine_token": str(machine_token or "").strip(),
    }
    try:
        data = _post_json(f"{server_url}/api/relay/rooms/list", payload, timeout=10)
    except urllib.error.HTTPError as exc:
        return _handle_http_error(exc)
    except Exception as exc:
        return False, f"connect failed: {exc}", None
    return bool(data.get("ok")), str(data.get("message") or "ok"), data


def relay_pull_events(
    server_url: str,
    license_key: str,
    after_id: int = 0,
    limit: int = 100,
    room_unique_id: str = "",
    app_version: str = "",
    machine_token: str = "",
) -> Tuple[bool, str, Optional[Dict]]:
    server_url = _normalize_server_url(server_url)
    if not server_url or not license_key:
        return False, "missing server_url/license_key", None
    payload = {
        "license_key": license_key.strip(),
        "machine_id": machine_fingerprint(),
        "app_version": app_version or "",
        "machine_token": str(machine_token or "").strip(),
        "room_unique_id": room_unique_id.strip().lstrip("@") if room_unique_id else "",
        "after_id": int(after_id or 0),
        "limit": int(limit or 100),
    }
    try:
        data = _post_json(f"{server_url}/api/relay/events/pull", payload, timeout=15)
    except urllib.error.HTTPError as exc:
        return _handle_http_error(exc)
    except Exception as exc:
        return False, f"connect failed: {exc}", None
    return bool(data.get("ok")), str(data.get("message") or "ok"), data


def pull_remote_permanent_ids(server_url: str, license_key: str, app_version: str = "", machine_token: str = "") -> Tuple[bool, str, Optional[Dict]]:
    server_url = _normalize_server_url(server_url)
    if not server_url or not license_key:
        return False, "缺少服务器地址或授权码", None
    payload = {
        "license_key": license_key.strip(),
        "machine_id": machine_fingerprint(),
        "app_version": app_version or "",
        "machine_token": str(machine_token or "").strip(),
    }
    try:
        data = _post_json(f"{server_url}/api/permanent-ids/pull", payload, timeout=12)
    except urllib.error.HTTPError as exc:
        return _handle_http_error(exc)
    except Exception as exc:
        return False, f"连接失败: {exc}", None
    return bool(data.get("ok")), str(data.get("message") or "ok"), data


def push_remote_permanent_ids(server_url: str, license_key: str, items: list[dict], app_version: str = "", machine_token: str = "") -> Tuple[bool, str, Optional[Dict]]:
    server_url = _normalize_server_url(server_url)
    if not server_url or not license_key:
        return False, "缺少服务器地址或授权码", None
    payload = {
        "license_key": license_key.strip(),
        "machine_id": machine_fingerprint(),
        "app_version": app_version or "",
        "machine_token": str(machine_token or "").strip(),
        "items": items or [],
    }
    try:
        data = _post_json(f"{server_url}/api/permanent-ids/push", payload, timeout=15)
    except urllib.error.HTTPError as exc:
        return _handle_http_error(exc)
    except Exception as exc:
        return False, f"连接失败: {exc}", None
    return bool(data.get("ok")), str(data.get("message") or "ok"), data


def sync_remote_permanent_ids(server_url: str, license_key: str, items: list[dict], app_version: str = "", machine_token: str = "") -> Tuple[bool, str, Optional[Dict]]:
    server_url = _normalize_server_url(server_url)
    if not server_url or not license_key:
        return False, "缺少服务器地址或授权码", None
    payload = {
        "license_key": license_key.strip(),
        "machine_id": machine_fingerprint(),
        "app_version": app_version or "",
        "machine_token": str(machine_token or "").strip(),
        "items": items or [],
    }
    try:
        data = _post_json(f"{server_url}/api/permanent-ids/sync", payload, timeout=20)
    except urllib.error.HTTPError as exc:
        return _handle_http_error(exc)
    except Exception as exc:
        return False, f"连接失败: {exc}", None
    return bool(data.get("ok")), str(data.get("message") or "ok"), data
