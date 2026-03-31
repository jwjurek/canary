import json
from pathlib import Path
import unittest

from scrapers.parsers import BN_TITLE_RE, LB_TITLE_RE, parse_detail_specs, parse_spec_table

FIXTURE_DIR = Path(__file__).parent / "fixtures"


class ParserContractTests(unittest.TestCase):
    def test_bluenile_spec_table_contract(self):
        rows = json.loads((FIXTURE_DIR / "bluenile_spec_rows.json").read_text())
        specs = parse_spec_table(rows)
        self.assertEqual(specs["stock"], "12345678")
        self.assertEqual(specs["shape"], "Oval")
        self.assertEqual(specs["intensity"], "Fancy Vivid")
        self.assertEqual(specs["clarity"], "VVS2")

    def test_leibish_spec_table_contract(self):
        rows = json.loads((FIXTURE_DIR / "leibish_spec_rows.json").read_text())
        specs = parse_detail_specs(rows)
        self.assertEqual(specs["carat"], "2.15")
        self.assertEqual(specs["shape"], "Cushion")
        self.assertEqual(specs["intensity"], "Fancy Intense")
        self.assertEqual(specs["table_pct"], "62")

    def test_title_regex_contracts(self):
        bn = "GIA 5.06 Carat Fancy Vivid Yellow-VVS2 Round Cut Diamond"
        lb = "1.15 Carat Fancy Intense Yellow Radiant Diamond FL GIA"
        self.assertIsNotNone(BN_TITLE_RE.search(bn))
        self.assertIsNotNone(LB_TITLE_RE.search(lb))


if __name__ == "__main__":
    unittest.main()
