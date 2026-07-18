import os
import unittest

from app import App


class SettingsSecurityTests(unittest.TestCase):
    def test_sensitive_settings_are_not_stored_as_plaintext(self):
        payload = {
            "sign_api_key": "secret-sign-key",
            "custom_name": "host",
        }

        encoded = App._encode_settings_payload(object(), payload)

        self.assertNotEqual(encoded["sign_api_key"], payload["sign_api_key"])
        if os.name == "nt":
            self.assertTrue(encoded["sign_api_key"].startswith("dpapi:"))
        self.assertEqual(encoded["custom_name"], "host")
        self.assertEqual(
            App._decode_settings_payload(object(), encoded)["sign_api_key"],
            payload["sign_api_key"],
        )

    def test_plaintext_legacy_key_is_still_readable(self):
        decoded = App._decode_settings_payload(object(), {"sign_api_key": "legacy-key"})
        self.assertEqual(decoded["sign_api_key"], "legacy-key")


if __name__ == "__main__":
    unittest.main()
