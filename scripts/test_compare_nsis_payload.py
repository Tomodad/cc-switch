import unittest
from compare_nsis_payload import compare_payload, UNKNOWN, NSIS


class PayloadComparisonTests(unittest.TestCase):
    def setUp(self):
        self.build = b'MZ\x00prefix' + UNKNOWN + b'\x00body' + NSIS + b'tail'
        self.payload = self.build.replace(UNKNOWN, NSIS, 1)

    def test_expected_marker_change(self):
        result = compare_payload(self.build, self.payload)
        self.assertTrue(result['normalized_equal'])
        self.assertEqual(result['build_executable_sha256'], result['normalized_payload_sha256'])
        self.assertNotEqual(result['build_executable_sha256'], result['payload_executable_sha256'])

    def test_unrelated_change_rejected(self):
        for offset in (0, len(self.payload) - 1):
            changed = bytearray(self.payload)
            changed[offset] ^= 1
            with self.assertRaises(ValueError):
                compare_payload(self.build, bytes(changed))

    def test_length_change_rejected(self):
        for changed in (self.payload + b'\x00', self.payload[:-1]):
            with self.assertRaises(ValueError):
                compare_payload(self.build, changed)

    def test_missing_marker_rejected(self):
        with self.assertRaises(ValueError):
            compare_payload(b'MZplain', b'MZplain')

    def test_unchanged_marker_rejected(self):
        with self.assertRaises(ValueError):
            compare_payload(self.build, self.build)

    def test_only_first_marker_may_change(self):
        build = self.build + UNKNOWN
        compare_payload(build, build.replace(UNKNOWN, NSIS, 1))
        with self.assertRaises(ValueError):
            compare_payload(build, build.replace(UNKNOWN, NSIS))
        with self.assertRaises(ValueError):
            compare_payload(build, self.build + NSIS)


if __name__ == '__main__':
    unittest.main()
