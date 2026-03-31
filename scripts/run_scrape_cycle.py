import argparse
import asyncio
import importlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from scripts.scrape_ops_lib import build_listing_key, detect_mispriced, summarize_run


def utc_run_id():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _copy_import_vendor_csv(vendor_alias, import_dir, out_csv):
    import_path = Path(import_dir) / f"{vendor_alias}.csv"
    if not import_path.exists():
        raise FileNotFoundError(
            f"Import vendor file not found: {import_path}. "
            "Expected <import-dir>/<vendor_alias>.csv."
        )
    df = pd.read_csv(import_path)
    if df.empty:
        raise ValueError(f"Import vendor CSV is empty: {import_path}")
    if "vendor" not in df.columns:
        df["vendor"] = vendor_alias
    else:
        df["vendor"] = (
            df["vendor"].fillna("").astype(str).str.strip().replace("", vendor_alias)
        )
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)


async def run_vendor(
    vendor,
    raw_dir,
    artifacts_root,
    max_cards=None,
    max_pages=2,
    retries=2,
    import_dir="data/raw/vendor_imports",
):
    out_csv = raw_dir / f"{vendor}.csv"
    artifacts_dir = artifacts_root / vendor
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    if vendor.startswith("import:"):
        vendor_alias = vendor.split(":", 1)[1].strip()
        if not vendor_alias:
            raise ValueError("Import vendor alias is required, e.g. import:brilliance")
        _copy_import_vendor_csv(vendor_alias, import_dir, out_csv)
        return {
            "vendor": vendor,
            "status": "ok",
            "attempts": 1,
            "outfile": str(out_csv),
            "mode": "import",
            "import_alias": vendor_alias,
        }
    if vendor == "bluenile":
        mod = importlib.import_module("scrapers.blue_nile_scraper")
        fn = lambda: mod.scrape(outfile=str(out_csv), headless=True, use_chrome=True, max_cards=max_cards, artifacts_dir=str(artifacts_dir))
    elif vendor == "leibish":
        mod = importlib.import_module("scrapers.leibish_scraper")
        fn = lambda: mod.scrape(max_pages=max_pages, outfile=str(out_csv), headless=True, use_chrome=True, max_cards=max_cards, artifacts_dir=str(artifacts_dir))
    else:
        raise ValueError(f"Unsupported vendor: {vendor}")

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            await fn()
            if out_csv.exists() and out_csv.stat().st_size > 0:
                return {"vendor": vendor, "status": "ok", "attempts": attempt, "outfile": str(out_csv)}
        except Exception as e:
            last_err = str(e)
    return {"vendor": vendor, "status": "failed", "attempts": retries, "error": last_err or "no output"}


def append_history(run_id, raw_dir, history_path):
    frames = []
    scraped_at = datetime.now(timezone.utc).isoformat()
    for csv_path in raw_dir.glob("*.csv"):
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            continue
        if df.empty:
            continue
        df["listing_key"] = df.apply(build_listing_key, axis=1)
        df["run_id"] = run_id
        df["scraped_at"] = scraped_at
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    batch = pd.concat(frames, ignore_index=True)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    if history_path.exists():
        existing = pd.read_csv(history_path)
        combined = pd.concat([existing, batch], ignore_index=True)
    else:
        combined = batch
    combined.to_csv(history_path, index=False)
    return batch


def compute_growth_metrics(history_df, run_id):
    if history_df.empty or "run_id" not in history_df.columns:
        return {"previous_run_id": None, "previous_rows": 0, "current_rows": 0, "delta_rows": 0, "delta_pct": 0.0}

    current = history_df[history_df["run_id"] == run_id]
    previous_runs = [r for r in history_df["run_id"].dropna().astype(str).unique().tolist() if r != run_id]
    if not previous_runs:
        return {
            "previous_run_id": None,
            "previous_rows": 0,
            "current_rows": int(len(current)),
            "delta_rows": int(len(current)),
            "delta_pct": 1.0 if len(current) > 0 else 0.0,
        }

    prev_run = sorted(previous_runs)[-1]
    prev = history_df[history_df["run_id"] == prev_run]
    prev_rows = int(len(prev))
    cur_rows = int(len(current))
    delta = cur_rows - prev_rows
    delta_pct = (delta / prev_rows) if prev_rows else (1.0 if cur_rows > 0 else 0.0)
    return {
        "previous_run_id": prev_run,
        "previous_rows": prev_rows,
        "current_rows": cur_rows,
        "delta_rows": int(delta),
        "delta_pct": float(delta_pct),
    }


def compute_expansion_metrics(history_df, run_id):
    if history_df.empty or "run_id" not in history_df.columns:
        return {
            "baseline_run_id": None,
            "current_unique_listings": 0,
            "baseline_unique_listings": 0,
            "listing_coverage_multiple": 0.0,
            "current_unique_certs": 0,
            "baseline_unique_certs": 0,
            "cert_coverage_multiple": 0.0,
        }

    all_runs = sorted(history_df["run_id"].dropna().astype(str).unique().tolist())
    if not all_runs:
        return {
            "baseline_run_id": None,
            "current_unique_listings": 0,
            "baseline_unique_listings": 0,
            "listing_coverage_multiple": 0.0,
            "current_unique_certs": 0,
            "baseline_unique_certs": 0,
            "cert_coverage_multiple": 0.0,
        }

    baseline_run = all_runs[0]
    current = history_df[history_df["run_id"] == run_id].copy()
    baseline = history_df[history_df["run_id"] == baseline_run].copy()

    current_unique_listings = int(current["listing_key"].dropna().astype(str).nunique()) if "listing_key" in current.columns else 0
    baseline_unique_listings = int(baseline["listing_key"].dropna().astype(str).nunique()) if "listing_key" in baseline.columns else 0

    current_unique_certs = int(current["cert_number"].dropna().astype(str).str.strip().replace("", pd.NA).dropna().nunique()) if "cert_number" in current.columns else 0
    baseline_unique_certs = int(baseline["cert_number"].dropna().astype(str).str.strip().replace("", pd.NA).dropna().nunique()) if "cert_number" in baseline.columns else 0

    listing_multiple = (current_unique_listings / baseline_unique_listings) if baseline_unique_listings else 0.0
    cert_multiple = (current_unique_certs / baseline_unique_certs) if baseline_unique_certs else 0.0

    return {
        "baseline_run_id": baseline_run,
        "current_unique_listings": current_unique_listings,
        "baseline_unique_listings": baseline_unique_listings,
        "listing_coverage_multiple": float(listing_multiple),
        "current_unique_certs": current_unique_certs,
        "baseline_unique_certs": baseline_unique_certs,
        "cert_coverage_multiple": float(cert_multiple),
    }


def compute_active_vendor_count(batch_df):
    if batch_df.empty or "vendor" not in batch_df.columns:
        return 0
    return int(batch_df["vendor"].dropna().astype(str).str.strip().replace("", pd.NA).dropna().nunique())


def build_phase_progress(active_vendor_count, min_active_vendors, expansion, min_coverage_multiple):
    phase1_checks = {
        "active_vendor_count": int(active_vendor_count),
        "min_active_vendors": int(min_active_vendors),
        "meets_source_diversity": bool(active_vendor_count >= min_active_vendors),
    }
    phase2_checks = {
        "listing_coverage_multiple": float(expansion.get("listing_coverage_multiple", 0.0)),
        "cert_coverage_multiple": float(expansion.get("cert_coverage_multiple", 0.0)),
        "min_coverage_multiple": float(min_coverage_multiple),
        "meets_listing_coverage": bool(expansion.get("listing_coverage_multiple", 0.0) >= min_coverage_multiple),
        "meets_cert_coverage": bool(expansion.get("cert_coverage_multiple", 0.0) >= min_coverage_multiple),
    }
    return {
        "phase_1_acquisition_hardening": {
            "ready": phase1_checks["meets_source_diversity"],
            "checks": phase1_checks,
        },
        "phase_2_dataset_defensibility": {
            "ready": phase2_checks["meets_listing_coverage"] and phase2_checks["meets_cert_coverage"],
            "checks": phase2_checks,
        },
    }


def evaluate_phase12_exit(
    batch_df,
    per_vendor_quality,
    active_vendor_count,
    min_active_vendors,
    expansion,
    min_coverage_multiple,
    max_vendor_share,
    min_weighted_cert_present_rate,
    min_weighted_parse_success_rate,
):
    if batch_df.empty or "vendor" not in batch_df.columns:
        vendor_distribution = {}
        dominant_vendor = None
        dominant_vendor_share = 0.0
    else:
        vc = batch_df["vendor"].dropna().astype(str).str.strip().replace("", pd.NA).dropna().value_counts()
        total = int(vc.sum())
        vendor_distribution = {str(k): int(v) for k, v in vc.to_dict().items()}
        if total > 0 and not vc.empty:
            dominant_vendor = str(vc.index[0])
            dominant_vendor_share = float(vc.iloc[0] / total)
        else:
            dominant_vendor = None
            dominant_vendor_share = 0.0

    weighted_rows = 0
    weighted_cert_total = 0.0
    weighted_parse_total = 0.0
    for metrics in per_vendor_quality.values():
        rows = int(metrics.get("rows", 0) or 0)
        cert_rate = float(metrics.get("cert_present_rate", 0.0) or 0.0)
        parse_rate = float(metrics.get("parse_success_rate", 0.0) or 0.0)
        weighted_rows += rows
        weighted_cert_total += rows * cert_rate
        weighted_parse_total += rows * parse_rate

    weighted_cert_present_rate = (weighted_cert_total / weighted_rows) if weighted_rows else 0.0
    weighted_parse_success_rate = (weighted_parse_total / weighted_rows) if weighted_rows else 0.0

    checks = {
        "min_active_vendors": bool(active_vendor_count >= min_active_vendors),
        "min_listing_coverage_multiple": bool(expansion.get("listing_coverage_multiple", 0.0) >= min_coverage_multiple),
        "min_cert_coverage_multiple": bool(expansion.get("cert_coverage_multiple", 0.0) >= min_coverage_multiple),
        "max_dominant_vendor_share": bool(dominant_vendor_share <= max_vendor_share),
        "min_weighted_cert_present_rate": bool(weighted_cert_present_rate >= min_weighted_cert_present_rate),
        "min_weighted_parse_success_rate": bool(weighted_parse_success_rate >= min_weighted_parse_success_rate),
    }

    return {
        "ready": all(checks.values()),
        "checks": checks,
        "metrics": {
            "active_vendor_count": int(active_vendor_count),
            "listing_coverage_multiple": float(expansion.get("listing_coverage_multiple", 0.0)),
            "cert_coverage_multiple": float(expansion.get("cert_coverage_multiple", 0.0)),
            "dominant_vendor": dominant_vendor,
            "dominant_vendor_share": float(dominant_vendor_share),
            "weighted_cert_present_rate": float(weighted_cert_present_rate),
            "weighted_parse_success_rate": float(weighted_parse_success_rate),
            "vendor_distribution": vendor_distribution,
        },
        "thresholds": {
            "min_active_vendors": int(min_active_vendors),
            "min_coverage_multiple": float(min_coverage_multiple),
            "max_vendor_share": float(max_vendor_share),
            "min_weighted_cert_present_rate": float(min_weighted_cert_present_rate),
            "min_weighted_parse_success_rate": float(min_weighted_parse_success_rate),
        },
    }


def main(args):
    run_id = utc_run_id()
    run_root = Path(args.output_root) / run_id
    raw_dir = run_root / "raw"
    artifacts_root = run_root / "artifacts"
    raw_dir.mkdir(parents=True, exist_ok=True)
    artifacts_root.mkdir(parents=True, exist_ok=True)

    vendors = [v.strip().lower() for v in args.vendors.split(",") if v.strip()]

    results = asyncio.run(
        _run_all(
            vendors,
            raw_dir,
            artifacts_root,
            args.max_cards,
            args.max_pages,
            args.retries,
            args.import_dir,
        )
    )

    per_vendor = {}
    for vendor in vendors:
        csv_path = raw_dir / f"{vendor}.csv"
        if csv_path.exists() and csv_path.stat().st_size > 0:
            try:
                df = pd.read_csv(csv_path)
            except Exception:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()
        per_vendor[vendor] = summarize_run(df)

    history_path = Path(args.history_path)
    batch = append_history(run_id, raw_dir, history_path)
    history_df = pd.read_csv(history_path) if history_path.exists() else pd.DataFrame()
    mispriced = detect_mispriced(
        history_df,
        z_thresh=args.misprice_z,
        min_points=args.min_points,
        undervalued_drop_pct=args.undervalued_drop_pct,
    )

    out_candidates = run_root / "mispriced_candidates.csv"
    if not mispriced.empty:
        cols = [c for c in ["vendor", "listing_key", "source_url", "price_usd", "median", "z_score", "drop_pct_vs_median", "is_potentially_undervalued", "scraped_at"] if c in mispriced.columns]
        mispriced[cols].to_csv(out_candidates, index=False)
    else:
        pd.DataFrame().to_csv(out_candidates, index=False)

    summary = {
        "run_id": run_id,
        "vendors": vendors,
        "vendor_results": results,
        "vendor_quality": per_vendor,
        "batch_rows": int(len(batch)) if isinstance(batch, pd.DataFrame) else 0,
        "history_rows": int(len(history_df)) if isinstance(history_df, pd.DataFrame) else 0,
        "mispriced_candidates": int(len(mispriced)),
        "output_dir": str(run_root),
        "history_path": str(history_path),
    }
    growth = compute_growth_metrics(history_df, run_id)
    expansion = compute_expansion_metrics(history_df, run_id)
    active_vendor_count = compute_active_vendor_count(batch if isinstance(batch, pd.DataFrame) else pd.DataFrame())
    summary["growth_metrics"] = growth
    summary["expansion_metrics"] = expansion
    summary["active_vendor_count"] = active_vendor_count
    summary["growth_target"] = {"min_new_rows": int(args.min_new_rows), "require_growth": bool(args.require_growth)}
    summary["source_target"] = {
        "min_active_vendors": int(args.min_active_vendors),
        "require_min_active_vendors": bool(args.require_min_active_vendors),
    }
    summary["coverage_target"] = {
        "min_coverage_multiple": float(args.min_coverage_multiple),
        "require_coverage_multiple": bool(args.require_coverage_multiple),
    }
    summary["phase_progress"] = build_phase_progress(
        active_vendor_count=active_vendor_count,
        min_active_vendors=args.min_active_vendors,
        expansion=expansion,
        min_coverage_multiple=args.min_coverage_multiple,
    )
    summary["phase12_exit"] = evaluate_phase12_exit(
        batch_df=batch if isinstance(batch, pd.DataFrame) else pd.DataFrame(),
        per_vendor_quality=per_vendor,
        active_vendor_count=active_vendor_count,
        min_active_vendors=args.min_active_vendors,
        expansion=expansion,
        min_coverage_multiple=args.min_coverage_multiple,
        max_vendor_share=args.max_vendor_share,
        min_weighted_cert_present_rate=args.min_weighted_cert_present_rate,
        min_weighted_parse_success_rate=args.min_weighted_parse_success_rate,
    )

    if args.require_growth and growth["delta_rows"] < args.min_new_rows:
        summary["status"] = "failed_growth_gate"
    elif args.require_min_active_vendors and active_vendor_count < args.min_active_vendors:
        summary["status"] = "failed_source_gate"
    elif args.require_coverage_multiple and (
        expansion["listing_coverage_multiple"] < args.min_coverage_multiple
        or expansion["cert_coverage_multiple"] < args.min_coverage_multiple
    ):
        summary["status"] = "failed_coverage_gate"
    elif args.require_phase12_exit and not summary["phase12_exit"]["ready"]:
        summary["status"] = "failed_phase12_exit_gate"
    else:
        summary["status"] = "ok"

    with open(run_root / "run_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))

    if summary["status"] != "ok":
        if summary["status"] == "failed_growth_gate":
            raise SystemExit(
                f"Growth gate failed: delta_rows={growth['delta_rows']} < min_new_rows={args.min_new_rows}. "
                "Dataset did not grow meaningfully for this run."
            )
        if summary["status"] == "failed_source_gate":
            raise SystemExit(
                f"Source gate failed: active_vendor_count={active_vendor_count} < min_active_vendors={args.min_active_vendors}."
            )
        if summary["status"] == "failed_phase12_exit_gate":
            raise SystemExit(
                "Phase 1/2 exit gate failed: one or more exit checks did not pass. "
                f"checks={summary['phase12_exit']['checks']}"
            )
        raise SystemExit(
            "Coverage gate failed: "
            f"listing_coverage_multiple={expansion['listing_coverage_multiple']:.3f}, "
            f"cert_coverage_multiple={expansion['cert_coverage_multiple']:.3f}, "
            f"required>={args.min_coverage_multiple:.3f}."
        )


async def _run_all(vendors, raw_dir, artifacts_root, max_cards, max_pages, retries, import_dir):
    results = []
    for vendor in vendors:
        result = await run_vendor(
            vendor,
            raw_dir,
            artifacts_root,
            max_cards=max_cards,
            max_pages=max_pages,
            retries=retries,
            import_dir=import_dir,
        )
        results.append(result)
    return results


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Run a timestamped multi-vendor scrape cycle with history + mispricing detection.")
    ap.add_argument("--vendors", default="bluenile,leibish")
    ap.add_argument(
        "--import-dir",
        default="data/raw/vendor_imports",
        help="Directory for offline vendor CSV imports used with vendor tokens like import:brilliance.",
    )
    ap.add_argument("--output-root", default="data/runs")
    ap.add_argument("--history-path", default="data/history/listings_history.csv")
    ap.add_argument("--max-cards", type=int, default=50)
    ap.add_argument("--max-pages", type=int, default=2)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--misprice-z", type=float, default=2.5)
    ap.add_argument("--min-points", type=int, default=5)
    ap.add_argument("--undervalued-drop-pct", type=float, default=0.2)
    ap.add_argument("--min-new-rows", type=int, default=25, help="Minimum additional rows versus previous run to count as meaningful growth.")
    ap.add_argument("--require-growth", action="store_true", help="Fail the run if growth target is not met.")
    ap.add_argument("--min-active-vendors", type=int, default=3, help="Minimum number of active vendors present in this run's batch.")
    ap.add_argument("--require-min-active-vendors", action="store_true", help="Fail the run if active vendor count is below --min-active-vendors.")
    ap.add_argument("--min-coverage-multiple", type=float, default=3.0, help="Minimum  coverage multiple versus baseline run for both unique listings and unique certs.")
    ap.add_argument("--require-coverage-multiple", action="store_true", help="Fail the run if listing/cert coverage multiples are below --min-coverage-multiple.")
    ap.add_argument("--max-vendor-share", type=float, default=0.60, help="Maximum allowable dominant vendor share for Phase 1/2 exit readiness.")
    ap.add_argument("--min-weighted-cert-present-rate", type=float, default=0.70, help="Minimum weighted certificate presence rate for Phase 1/2 exit readiness.")
    ap.add_argument("--min-weighted-parse-success-rate", type=float, default=0.80, help="Minimum weighted parse success rate for Phase 1/2 exit readiness.")
    ap.add_argument("--require-phase12-exit", action="store_true", help="Fail the run unless all Phase 1/2 exit checks pass.")
    args = ap.parse_args()
    main(args)
