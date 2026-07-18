import re
from typing import Iterable, Tuple


_ASCII_ALNUM_RE = re.compile(r"^[A-Za-z0-9]+$")


def classify_print_content(
    content: str,
    *,
    numeric_enabled: bool,
    keyword_enabled: bool,
    keywords: Iterable[str],
    min_length: int,
    max_length: int,
) -> Tuple[bool, str]:
    text = str(content or "").strip()
    if not text or len(text) < min_length or len(text) > max_length:
        return False, ""

    if numeric_enabled and text.isascii() and text.isdigit():
        return True, "numeric"

    if numeric_enabled and _ASCII_ALNUM_RE.fullmatch(text):
        has_digit = any(ch.isdigit() for ch in text)
        has_letter = any(ch.isalpha() for ch in text)
        if has_digit and has_letter:
            return True, "alphanumeric"

    if keyword_enabled:
        for keyword in keywords:
            if keyword and keyword in text:
                return True, f"keyword:{keyword}"

    return False, ""


def has_required_permanent_number(rule_hit: str, permanent_id: object) -> bool:
    if str(rule_hit or "") != "alphanumeric":
        return True
    return bool(str(permanent_id or "").strip())
