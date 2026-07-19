import unittest

from compat_sdk_smoke import expected_sdk_versions, verify_sdk_versions


class CompatibilitySDKVersionTest(unittest.TestCase):
    def test_exact_pins_are_accepted(self) -> None:
        expected = expected_sdk_versions()
        self.assertEqual(verify_sdk_versions(expected), expected)

    def test_mismatched_version_is_rejected(self) -> None:
        expected = expected_sdk_versions()
        expected["exa-py"] = "0.0.0-mismatch"
        with self.assertRaisesRegex(AssertionError, r"exa-py version .* expected 2\.16\.0"):
            verify_sdk_versions(expected)


if __name__ == "__main__":
    unittest.main()
