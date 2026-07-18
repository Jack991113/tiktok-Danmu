import unittest

from listener_modes import PUBLIC_LISTEN_MODE_OPTIONS, PUBLIC_LISTEN_MODES, resolve_sign_settings


DEFAULT_BASE = "https://tiktok.eulerstream.com"


class ListenerModeTests(unittest.TestCase):
    def test_public_build_has_three_explicit_modes(self):
        self.assertEqual(PUBLIC_LISTEN_MODES, ("api", "local", "browser"))
        self.assertEqual(
            PUBLIC_LISTEN_MODE_OPTIONS,
            (("API 接口", "api"), ("本机直连", "local"), ("浏览器会话", "browser")),
        )

    def test_api_mode_uses_configured_base_and_enabled_key(self):
        self.assertEqual(
            resolve_sign_settings(
                "api",
                "https://sign.example.com/",
                "secret-key",
                True,
                DEFAULT_BASE,
            ),
            ("https://sign.example.com", "secret-key"),
        )

    def test_api_mode_respects_disabled_key(self):
        self.assertEqual(
            resolve_sign_settings("api", "https://sign.example.com", "saved-key", False, DEFAULT_BASE),
            ("https://sign.example.com", ""),
        )

    def test_local_mode_ignores_saved_api_configuration(self):
        self.assertEqual(
            resolve_sign_settings("local", "https://sign.example.com", "saved-key", True, DEFAULT_BASE),
            (DEFAULT_BASE, ""),
        )

    def test_browser_mode_ignores_saved_api_configuration(self):
        self.assertEqual(
            resolve_sign_settings("browser", "https://sign.example.com", "saved-key", True, DEFAULT_BASE),
            (DEFAULT_BASE, ""),
        )


if __name__ == "__main__":
    unittest.main()
