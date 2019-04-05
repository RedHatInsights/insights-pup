import unittest

from pup.utils import fact_extract


class TestFormatHelpers(unittest.TestCase):

    def test_strip_bad_display_name(self):
        values = {"account": "12345", "metadata": {"display_name": "a", "non_empty_key": "non_empty_value"}}
        stripped_metadata = fact_extract._remove_bad_display_name(values["metadata"])

        assert stripped_metadata == {"non_empty_key": "non_empty_value"}

    def test_strip_empties(self):
        values = {"account": "12345", "metadata": {"empty_key": "", "another_empty": [], "non_empty_key": "non_empty_value"}}
        stripped_metadata = fact_extract._remove_empties(values["metadata"])
        print(stripped_metadata)

        assert stripped_metadata == {"non_empty_key": "non_empty_value"}
