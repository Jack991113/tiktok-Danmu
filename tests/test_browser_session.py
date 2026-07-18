import unittest

from browser_session import build_live_room_url, filter_sensitive_browser_cookies


class BrowserSessionTests(unittest.TestCase):
    def test_build_live_room_url(self):
        self.assertEqual(
            build_live_room_url("@creator"),
            "https://www.tiktok.com/@creator/live",
        )

    def test_empty_room_id_is_rejected(self):
        with self.assertRaises(ValueError):
            build_live_room_url("")

    def test_sensitive_login_cookies_are_not_exported(self):
        filtered = filter_sensitive_browser_cookies({
            "sessionid": "secret-session",
            "sessionid_ss": "secret-session-ss",
            "tt-target-idc": "useast2a",
            "ttwid": "anonymous-browser-cookie",
        })

        self.assertEqual(filtered, {"ttwid": "anonymous-browser-cookie"})


if __name__ == "__main__":
    unittest.main()
