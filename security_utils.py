import base64
import ctypes
import hashlib
import hmac
import os
import secrets
from ctypes import wintypes


_DPAPI_PREFIX = "dpapi:"
_B64_PREFIX = "b64:"
_PASSWORD_PREFIX = "pbkdf2_sha256"
_PASSWORD_ITERATIONS = 240000
_DPAPI_ENTROPY = b"SenNailsSecureLocal"


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]


def _blob_from_bytes(value: bytes) -> _DataBlob:
    if not value:
        return _DataBlob(0, None)
    buffer = ctypes.create_string_buffer(value)
    return _DataBlob(len(value), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)))


def _bytes_from_blob(blob: _DataBlob) -> bytes:
    if not blob.cbData or not blob.pbData:
        return b""
    return ctypes.string_at(blob.pbData, blob.cbData)


def _dpapi_encrypt(raw: bytes) -> bytes:
    if os.name != "nt":
        raise RuntimeError("DPAPI is only available on Windows")
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    in_blob = _blob_from_bytes(raw)
    entropy_blob = _blob_from_bytes(_DPAPI_ENTROPY)
    out_blob = _DataBlob()
    ok = crypt32.CryptProtectData(
        ctypes.byref(in_blob),
        ctypes.c_wchar_p("SenNails"),
        ctypes.byref(entropy_blob),
        None,
        None,
        0,
        ctypes.byref(out_blob),
    )
    if not ok:
        raise ctypes.WinError()
    try:
        return _bytes_from_blob(out_blob)
    finally:
        if out_blob.pbData:
            kernel32.LocalFree(out_blob.pbData)


def _dpapi_decrypt(raw: bytes) -> bytes:
    if os.name != "nt":
        raise RuntimeError("DPAPI is only available on Windows")
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    in_blob = _blob_from_bytes(raw)
    entropy_blob = _blob_from_bytes(_DPAPI_ENTROPY)
    out_blob = _DataBlob()
    ok = crypt32.CryptUnprotectData(
        ctypes.byref(in_blob),
        None,
        ctypes.byref(entropy_blob),
        None,
        None,
        0,
        ctypes.byref(out_blob),
    )
    if not ok:
        raise ctypes.WinError()
    try:
        return _bytes_from_blob(out_blob)
    finally:
        if out_blob.pbData:
            kernel32.LocalFree(out_blob.pbData)


def protect_secret(value: str) -> str:
    text = str(value or "")
    if not text:
        return ""
    raw = text.encode("utf-8")
    try:
        if os.name == "nt":
            return _DPAPI_PREFIX + base64.b64encode(_dpapi_encrypt(raw)).decode("ascii")
    except Exception:
        pass
    return _B64_PREFIX + base64.b64encode(raw).decode("ascii")


def unprotect_secret(value: str) -> str:
    text = str(value or "")
    if not text:
        return ""
    try:
        if text.startswith(_DPAPI_PREFIX):
            raw = base64.b64decode(text[len(_DPAPI_PREFIX) :].encode("ascii"))
            return _dpapi_decrypt(raw).decode("utf-8")
        if text.startswith(_B64_PREFIX):
            raw = base64.b64decode(text[len(_B64_PREFIX) :].encode("ascii"))
            return raw.decode("utf-8")
    except Exception:
        return ""
    return text


def is_password_hash(value: str) -> bool:
    return str(value or "").startswith(_PASSWORD_PREFIX + "$")


def hash_password(password: str, salt: str = "") -> str:
    text = str(password or "")
    if not text:
        return ""
    raw_salt = bytes.fromhex(salt) if salt else secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", text.encode("utf-8"), raw_salt, _PASSWORD_ITERATIONS)
    return f"{_PASSWORD_PREFIX}${_PASSWORD_ITERATIONS}${raw_salt.hex()}${digest.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    text = str(password or "")
    payload = str(encoded or "")
    if not text or not payload:
        return False
    if not is_password_hash(payload):
        return hmac.compare_digest(text, payload)
    try:
        _prefix, iterations_text, salt_hex, digest_hex = payload.split("$", 3)
        iterations = max(100000, int(iterations_text))
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            text.encode("utf-8"),
            bytes.fromhex(salt_hex),
            iterations,
        )
        return hmac.compare_digest(digest.hex(), digest_hex)
    except Exception:
        return False
