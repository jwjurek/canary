import unittest

import importlib.util

if importlib.util.find_spec("pandas") is not None:
    import pandas as pd
    from pathlib import Path
    from tempfile import TemporaryDirectory
    from scripts.run_scrape_cycle import (
        _copy_import_vendor_csv,
        build_phase_progress,
        compute_active_vendor_count,
        compute_expansion_metrics,
        evaluate_phase12_exit,
    )
else:
    pd = None
    _copy_import_vendor_csv = None
    build_phase_progress = None
    compute_active_vendor_count = None
    compute_expansion_metrics = None
    evaluate_phase12_exit = None


@unittest.skipIf(pd is None, "pandas not installed")
class RunScrapeCycleMetricsTests(unittest.TestCase):
    def test_compute_active_vendor_count(self):
        batch = pd.DataFrame(
            [
                {"vendor": "BlueNile"},
                {"vendor": "Leibish"},
                {"vendor": "Leibish"},
                {"vendor": ""},
                {"vendor": None},
            ]
        )
        self.assertEqual(compute_active_vendor_count(batch), 2)

    def test_compute_expansion_metrics_against_baseline(self):
        history = pd.DataFrame(
            [
                {"run_id": "20260101T000000Z", "listing_key": "A", "cert_number": "1"},
                {"run_id": "20260101T000000Z", "listing_key": "B", "cert_number": "2"},
                {"run_id": "20260102T000000Z", "listing_key": "A", "cert_number": "1"},
                {"run_id": "20260102T000000Z", "listing_key": "B", "cert_number": "2"},
                {"run_id": "20260102T000000Z", "listing_key": "C", "cert_number": "3"},
                {"run_id": "20260102T000000Z", "listing_key": "D", "cert_number": "4"},
                {"run_id": "20260102T000000Z", "listing_key": "E", "cert_number": "5"},
                {"run_id": "20260102T000000Z", "listing_key": "F", "cert_number": "6"},
            ]
        )

        metrics = compute_expansion_metrics(history, "20260102T000000Z")
        self.assertEqual(metrics["baseline_run_id"], "20260101T000000Z")
        self.assertEqual(metrics["current_unique_listings"], 6)
        self.assertEqual(metrics["baseline_unique_listings"], 2)
        self.assertEqual(metrics["current_unique_certs"], 6)
        self.assertEqual(metrics["baseline_unique_certs"], 2)
        self.assertEqual(metrics["listing_coverage_multiple"], 3.0)
        self.assertEqual(metrics["cert_coverage_multiple"], 3.0)

    def test_build_phase_progress_tracks_each_phase_independently(self):
        expansion = {"listing_coverage_multiple": 3.2, "cert_coverage_multiple": 2.1}
        progress = build_phase_progress(
            active_vendor_count=3,
            min_active_vendors=3,
            expansion=expansion,
            min_coverage_multiple=3.0,
        )
        self.assertTrue(progress["phase_1_acquisition_hardening"]["ready"])
        self.assertFalse(progress["phase_2_dataset_defensibility"]["ready"])
        self.assertTrue(progress["phase_2_dataset_defensibility"]["checks"]["meets_listing_coverage"])
        self.assertFalse(progress["phase_2_dataset_defensibility"]["checks"]["meets_cert_coverage"])

    def test_evaluate_phase12_exit_not_ready_when_dominated_by_one_vendor(self):
        batch = pd.DataFrame(
            [{"vendor": "BlueNile"} for _ in range(9)] + [{"vendor": "Leibish"}]
        )
        per_vendor = {
            "bluenile": {"rows": 9, "cert_present_rate": 0.9, "parse_success_rate": 0.95},
            "leibish": {"rows": 1, "cert_present_rate": 0.8, "parse_success_rate": 0.9},
        }
        expansion = {"listing_coverage_multiple": 3.5, "cert_coverage_multiple": 3.1}
        result = evaluate_phase12_exit(
            batch_df=batch,
            per_vendor_quality=per_vendor,
            active_vendor_count=2,
            min_active_vendors=2,
            expansion=expansion,
            min_coverage_multiple=3.0,
            max_vendor_share=0.60,
            min_weighted_cert_present_rate=0.70,
            min_weighted_parse_success_rate=0.80,
        )
        self.assertFalse(result["ready"])
        self.assertFalse(result["checks"]["max_dominant_vendor_share"])

    def test_evaluate_phase12_exit_ready_with_balanced_quality_and_coverage(self):
        batch = pd.DataFrame(
            [{"vendor": "BlueNile"} for _ in range(4)]
            + [{"vendor": "Leibish"} for _ in range(3)]
            + [{"vendor": "Ritani"} for _ in range(3)]
        )
        per_vendor = {
            "bluenile": {"rows": 4, "cert_present_rate": 0.9, "parse_success_rate": 0.92},
            "leibish": {"rows": 3, "cert_present_rate": 0.75, "parse_success_rate": 0.85},
            "ritani": {"rows": 3, "cert_present_rate": 0.8, "parse_success_rate": 0.83},
        }
        expansion = {"listing_coverage_multiple": 3.2, "cert_coverage_multiple": 3.0}
        result = evaluate_phase12_exit(
            batch_df=batch,
            per_vendor_quality=per_vendor,
            active_vendor_count=3,
            min_active_vendors=3,
            expansion=expansion,
            min_coverage_multiple=3.0,
            max_vendor_share=0.60,
            min_weighted_cert_present_rate=0.70,
            min_weighted_parse_success_rate=0.80,
        )
        self.assertTrue(result["ready"])

    def test_copy_import_vendor_csv_sets_vendor_when_missing(self):
        with TemporaryDirectory() as td:
            import_dir = Path(td) / "imports"
            out_dir = Path(td) / "out"
            import_dir.mkdir(parents=True, exist_ok=True)
            source_path = import_dir / "brilliance.csv"
            pd.DataFrame(
                [
                    {"source_url": "u1", "price_usd": 1000, "carat": 1.0},
                    {"source_url": "u2", "price_usd": 1200, "carat": 1.1},
                ]
            ).to_csv(source_path, index=False)

            out_csv = out_dir / "import:brilliance.csv"
            _copy_import_vendor_csv("brilliance", import_dir, out_csv)
            copied = pd.read_csv(out_csv)
            self.assertEqual(len(copied), 2)
            self.assertTrue((copied["vendor"] == "brilliance").all())


if __name__ == "__main__":
    unittest.main()
