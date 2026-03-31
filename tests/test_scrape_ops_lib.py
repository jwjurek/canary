import importlib.util
import unittest

if importlib.util.find_spec("pandas") is not None:
    import pandas as pd
else:
    pd = None


@unittest.skipIf(pd is None, "pandas not installed")
class ScrapeOpsLibTests(unittest.TestCase):
    def test_normalize_cert_and_listing_key(self):
        from scripts.scrape_ops_lib import build_listing_key, normalize_cert

        self.assertEqual(normalize_cert("GIA 123456789"), "123456789")
        self.assertEqual(normalize_cert("abc"), "")
        row = {"cert_lab": "gia", "cert_number": "1234567", "vendor": "BlueNile", "sku_or_stock": "x", "source_url": "u"}
        self.assertEqual(build_listing_key(row), "CERT:GIA:1234567")

    def test_summarize_run_basic_rates(self):
        from scripts.scrape_ops_lib import summarize_run

        df = pd.DataFrame(
            [
                {"price_usd": 1000, "carat": 1.0, "cert_number": "1234567"},
                {"price_usd": None, "carat": 1.2, "cert_number": ""},
            ]
        )
        s = summarize_run(df)
        self.assertEqual(s["rows"], 2)
        self.assertEqual(s["price_present_rate"], 0.5)
        self.assertEqual(s["carat_present_rate"], 1.0)

    def test_detect_mispriced_flags_large_drop(self):
        from scripts.scrape_ops_lib import detect_mispriced

        records = []
        for i in range(1, 8):
            records.append({"listing_key": "CERT:GIA:1234567", "price_usd": 10000 + i * 10, "scraped_at": f"2026-01-0{i}T00:00:00Z", "vendor": "BlueNile", "source_url": "u"})
        records.append({"listing_key": "CERT:GIA:1234567", "price_usd": 6500, "scraped_at": "2026-01-20T00:00:00Z", "vendor": "BlueNile", "source_url": "u"})
        hist = pd.DataFrame(records)

        flagged = detect_mispriced(hist, z_thresh=2.0, min_points=5, undervalued_drop_pct=0.2)
        self.assertEqual(len(flagged), 1)
        self.assertTrue(bool(flagged.iloc[0]["is_mispriced"]))
        self.assertTrue(bool(flagged.iloc[0]["is_potentially_undervalued"]))


if __name__ == "__main__":
    unittest.main()
