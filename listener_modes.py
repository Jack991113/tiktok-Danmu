from __future__ import annotations


PUBLIC_LISTEN_MODE_OPTIONS = (
    ("API 接口", "api"),
    ("本机直连", "local"),
    ("浏览器会话", "browser"),
)
PUBLIC_LISTEN_MODES = tuple(value for _label, value in PUBLIC_LISTEN_MODE_OPTIONS)


def resolve_sign_settings(
    mode: str,
    configured_base: str,
    configured_key: str,
    use_configured_key: bool,
    default_base: str,
) -> tuple[str, str]:
    normalized_mode = str(mode or "local").strip().lower()
    fallback_base = str(default_base or "").strip().rstrip("/")
    if normalized_mode != "api":
        return fallback_base, ""

    base = str(configured_base or fallback_base).strip().rstrip("/")
    key = str(configured_key or "").strip() if use_configured_key else ""
    return base, key
