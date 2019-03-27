import unittest

from pup.utils import fact_extract


class TestFormatHelpers(unittest.TestCase):

    def test_bytes_to_gb(self):
        gb = fact_extract.bytes_to_gb(987654321)
        self.assertEquals(gb, "1.0 GB")

    def test_ipv4_ipv6_addresses(self):
        ip_addresses = ['8.8.8.8', '3ffe:1900:4545:3:200:f8ff:fe21:67cf']
        result = fact_extract.ipv4_ipv6_addresses(ip_addresses)
        self.assertIn('8.8.8.8', result['ipv4_addresses'])
        self.assertIn('3ffe:1900:4545:3:200:f8ff:fe21:67cf', result['ipv6_addresses'])

    def test_strip_bad_display_name(self):
        values = {"account": "12345", "metadata": {"display_name": "a", "non_empty_key": "non_empty_value"}}
        stripped_metadata = fact_extract._remove_bad_display_name(values["metadata"])

        assert stripped_metadata == {"non_empty_key": "non_empty_value"}

    def test_strip_nones(self):
        values = {"account": "12345", "metadata": {"empty_key": "", "non_empty_key": "non_empty_value"}}
        stripped_metadata = fact_extract._remove_nones(values["metadata"])
        print(stripped_metadata)

        assert stripped_metadata == {"non_empty_key": "non_empty_value"}
