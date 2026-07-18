import unittest

from browser_session import build_live_room_url


class BrowserSessionTests(unittest.TestCase):
    def test_build_live_room_url(self):
        self.assertEqual(
            build_live_room_url("@creator"),
            "https://www.tiktok.com/@creator/live",
        )

    def test_empty_room_id_is_rejected(self):
        with self.assertRaises(ValueError):
            build_live_room_url("")


if __name__ == "__main__":
    unittest.main()
