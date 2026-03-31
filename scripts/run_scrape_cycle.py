import argparse
import asyncio
import importlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from scripts.scrape_ops_lib import build_listing_key, detect_mispriced, summarize_run


def utc_run_id():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


async def run_vendor(vendor, raw_dir, artifacts_root, max_cards=None, max_pages=2, retries=2):
    out_csv = raw_dir / f"{vendor}.csv"
    artifacts_dir = artifacts_root / vendor
    artifacts_dir.mkdir(parents=True, exist_ok=True)

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


def main(args):
    run_id = utc_run_id()
    run_root = Path(args.output_root) / run_id
    raw_dir = run_root / "raw"
    artifacts_root = run_root / "artifacts"
    raw_dir.mkdir(parents=True, exist_ok=True)
    artifacts_root.mkdir(parents=True, exist_ok=True)

    vendors = [v.strip().lower() for v in args.vendors.split(",") if v.strip()]

    results = asyncio.run(
        _run_all(vendors, raw_dir, artifacts_root, args.max_cards, args.max_pages, args.retries)
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
    summary["growth_metrics"] = growth
    summary["growth_target"] = {"min_new_rows": int(args.min_new_rows), "require_growth": bool(args.require_growth)}

    if args.require_growth and growth["delta_rows"] < args.min_new_rows:
        summary["status"] = "failed_growth_gate"
    else:
        summary["status"] = "ok"

    with open(run_root / "run_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))

    if summary["status"] != "ok":
        raise SystemExit(
            f"Growth gate failed: delta_rows={growth['delta_rows']} < min_new_rows={args.min_new_rows}. "
            "Dataset did not grow meaningfully for this run."
        )


async def _run_all(vendors, raw_dir, artifacts_root, max_cards, max_pages, retries):
    results = []
    for vendor in vendors:
        result = await run_vendor(vendor, raw_dir, artifacts_root, max_cards=max_cards, max_pages=max_pages, retries=retries)
        results.append(result)
    return results


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Run a timestamped multi-vendor scrape cycle with history + mispricing detection.")
    ap.add_argument("--vendors", default="bluenile,leibish")
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
    args = ap.parse_args()
    main(args)
