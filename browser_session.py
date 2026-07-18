from __future__ import annotations

import os
import queue
import threading
from dataclasses import dataclass
from typing import Any, Dict


SENSITIVE_BROWSER_COOKIE_NAMES = frozenset({
    "sessionid",
    "sessionid_ss",
    "sid_guard",
    "sid_tt",
    "sid_tt_ss",
    "tt-target-idc",
    "uid_tt",
    "uid_tt_ss",
})


class BrowserSessionError(RuntimeError):
    pass


@dataclass(frozen=True)
class BrowserAuthState:
    cookies: Dict[str, str]
    user_agent: str
    page_url: str
    warning: str = ""


def build_live_room_url(unique_id: str) -> str:
    uid = str(unique_id or "").strip().lstrip("@").split("/", 1)[0]
    if not uid:
        raise ValueError("直播间主播 ID 不能为空")
    return f"https://www.tiktok.com/@{uid}/live"


def filter_sensitive_browser_cookies(cookies: Dict[str, str]) -> Dict[str, str]:
    return {
        str(name): str(value)
        for name, value in dict(cookies or {}).items()
        if str(name).lower() not in SENSITIVE_BROWSER_COOKIE_NAMES
    }


class BrowserSessionManager:
    """Own one visible persistent Chrome session from a dedicated thread."""

    def __init__(self, profile_dir: str):
        self.profile_dir = os.path.abspath(profile_dir)
        self._commands: queue.Queue = queue.Queue()
        self._start_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._closed = False
        self._ready = threading.Event()
        self._startup_error = ""

    def open_room(self, unique_id: str, proxy_server: str = "", timeout: float = 75.0) -> BrowserAuthState:
        return self._request("open_room", unique_id, proxy_server, timeout=timeout)

    def get_auth_state(self, timeout: float = 15.0) -> BrowserAuthState:
        return self._request("auth_state", timeout=timeout)

    def close_room(self, unique_id: str, timeout: float = 15.0) -> None:
        self._request("close_room", unique_id, timeout=timeout)

    def stop(self, timeout: float = 20.0) -> None:
        thread = self._thread
        if thread is None or not thread.is_alive():
            self._closed = True
            return
        try:
            self._request("stop", timeout=timeout)
        finally:
            self._closed = True
            thread.join(timeout=max(0.1, timeout))

    def _ensure_started(self) -> None:
        with self._start_lock:
            if self._closed:
                raise BrowserSessionError("浏览器会话已经关闭")
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run, name="tiktok-browser-session", daemon=True)
                self._thread.start()
        if not self._ready.wait(timeout=15.0):
            raise BrowserSessionError("浏览器会话初始化超时")
        if self._startup_error:
            raise BrowserSessionError(self._startup_error)

    def _request(self, command: str, *args: Any, timeout: float) -> Any:
        self._ensure_started()
        response: queue.Queue = queue.Queue(maxsize=1)
        self._commands.put((command, args, response))
        try:
            ok, value = response.get(timeout=max(0.1, timeout))
        except queue.Empty as exc:
            raise BrowserSessionError(f"浏览器命令超时: {command}") from exc
        if not ok:
            raise BrowserSessionError(str(value))
        return value

    def _run(self) -> None:
        playwright = None
        try:
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
            from playwright.sync_api import sync_playwright
            os.makedirs(self.profile_dir, exist_ok=True)
            if os.name != "nt":
                os.chmod(self.profile_dir, 0o700)
            playwright = sync_playwright().start()
        except Exception as exc:
            self._startup_error = f"无法初始化 Playwright，请先安装 requirements.txt: {exc}"
            self._ready.set()
            return

        context = None
        pages: Dict[str, Any] = {}
        active_proxy = ""
        self._ready.set()
        try:
            while True:
                command, args, response = self._commands.get()
                if command == "stop":
                    response.put((True, None))
                    break
                try:
                    if command == "open_room":
                        unique_id, requested_proxy = args
                        requested_proxy = str(requested_proxy or "").strip()
                        if context is None:
                            context = self._launch_context(playwright, requested_proxy)
                            active_proxy = requested_proxy
                        elif requested_proxy != active_proxy:
                            raise BrowserSessionError("浏览器已使用另一套代理，请先停止监听后再切换代理")

                        uid = str(unique_id or "").strip().lstrip("@")
                        page = pages.get(uid)
                        if page is None or page.is_closed():
                            page = context.new_page()
                            pages[uid] = page
                        warning = ""
                        try:
                            page.goto(build_live_room_url(uid), wait_until="domcontentloaded", timeout=45_000)
                        except PlaywrightTimeoutError:
                            warning = "直播页面加载超时，但浏览器会话已保留，可等待页面继续加载"
                        page.bring_to_front()
                        page.wait_for_timeout(1_000)
                        response.put((True, self._auth_state(context, page, warning)))
                    elif command == "auth_state":
                        if context is None:
                            raise BrowserSessionError("浏览器尚未打开直播间")
                        page = next((p for p in pages.values() if not p.is_closed()), None)
                        response.put((True, self._auth_state(context, page)))
                    elif command == "close_room":
                        uid = str(args[0] or "").strip().lstrip("@")
                        page = pages.pop(uid, None)
                        if page is not None and not page.is_closed():
                            page.close()
                        response.put((True, None))
                    else:
                        raise BrowserSessionError(f"未知浏览器命令: {command}")
                except Exception as exc:
                    response.put((False, exc))
        except Exception as exc:
            self._fail_pending(str(exc))
        finally:
            try:
                if context is not None:
                    context.close()
            except Exception:
                pass
            try:
                if playwright is not None:
                    playwright.stop()
            except Exception:
                pass

    def _launch_context(self, playwright: Any, proxy_server: str):
        launch_kwargs: Dict[str, Any] = {
            "user_data_dir": self.profile_dir,
            "headless": False,
            "no_viewport": True,
        }
        if proxy_server:
            launch_kwargs["proxy"] = {"server": proxy_server}
        try:
            return playwright.chromium.launch_persistent_context(channel="chrome", **launch_kwargs)
        except Exception as chrome_exc:
            try:
                return playwright.chromium.launch_persistent_context(**launch_kwargs)
            except Exception as bundled_exc:
                raise BrowserSessionError(
                    "无法启动 Chrome。请安装 Google Chrome，或执行 `python -m playwright install chromium`。"
                    f" Chrome={chrome_exc}; Chromium={bundled_exc}"
                ) from bundled_exc

    @staticmethod
    def _auth_state(context: Any, page: Any, warning: str = "") -> BrowserAuthState:
        cookies = filter_sensitive_browser_cookies({
            str(item.get("name", "")): str(item.get("value", ""))
            for item in context.cookies("https://www.tiktok.com")
            if item.get("name")
        })
        user_agent = ""
        page_url = ""
        if page is not None and not page.is_closed():
            user_agent = str(page.evaluate("() => navigator.userAgent") or "")
            page_url = str(page.url or "")
        return BrowserAuthState(cookies=cookies, user_agent=user_agent, page_url=page_url, warning=warning)

    def _fail_pending(self, message: str) -> None:
        while True:
            try:
                _command, _args, response = self._commands.get_nowait()
            except queue.Empty:
                return
            response.put((False, message))
