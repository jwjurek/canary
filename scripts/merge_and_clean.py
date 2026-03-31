import argparse
import json
import os
import sys
from importlib.util import find_spec
from pathlib import Path

# Ensure repo root is importable when running as a script.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scrapers.sources import MIN_ACTIVE_SOURCES, assert_min_active_sources


np = None
pd = None

def ensure_runtime_deps():
    global np, pd
    if np is not None and pd is not None:
        return
    missing = [m for m in ("numpy", "pandas") if find_spec(m) is None]
    if missing:
        raise SystemExit(
            "Missing required runtime dependencies: " + ", ".join(missing) +
            ". Install with: python3 -m pip install -r requirements.txt"
        )
    import numpy as _np
    import pandas as _pd

    np = _np
    pd = _pd

CANON_COLS = [
    "vendor",
    "source_url",
    "sku_or_stock",
    "date_seen",
    "price_usd",
    "carat",
    "fancy_color_primary",
    "fancy_color_modifier",
    "intensity",
    "clarity",
    "shape",
    "polish",
    "symmetry",
    "fluorescence",
    "length_mm",
    "width_mm",
    "depth_mm",
    "table_pct",
    "depth_pct",
    "girdle",
    "culet",
    "l_w_ratio",
    "measurements",
    "cert_lab",
    "cert_number",
    "is_natural",
    "is_lab_grown",
    "in_stock",
]


def load_any(path):
    ensure_runtime_deps()
    if path.endswith(".csv"):
        return pd.read_csv(path)
    if path.endswith(".json"):
        return pd.read_json(path, lines=True)
    raise ValueError(f"Unsupported: {path}")


def normalize(df):
    ensure_runtime_deps()
    df = df.copy()
    for c in CANON_COLS:
        if c not in df.columns:
            df[c] = np.nan
    df["date_seen"] = pd.to_datetime(df["date_seen"], errors="coerce").dt.date
    for c in ["price_usd", "carat", "length_mm", "width_mm", "depth_mm", "table_pct", "depth_pct", "l_w_ratio"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["intensity"] = df["intensity"].astype(str).str.strip().str.title()
    df["clarity"] = df["clarity"].astype(str).str.strip().str.upper()
    df["shape"] = df["shape"].astype(str).str.strip().str.title()
    df["cert_lab"] = df["cert_lab"].astype(str).str.strip().str.upper()
    return df


def _cert_key(df):
    cert_num = df["cert_number"].astype(str).str.extract(r"(\d{6,10})")[0]
    cert_lab = df["cert_lab"].replace({"": np.nan, "NAN": np.nan})
    return cert_lab.fillna("UNK") + "|" + cert_num.fillna("")


def _geom_bucket(df):
    # tolerance-aware geometry key
    c = (df["carat"] / 0.01).round() * 0.01
    l = (df["length_mm"] / 0.02).round() * 0.02
    w = (df["width_mm"] / 0.02).round() * 0.02
    d = (df["depth_mm"] / 0.02).round() * 0.02
    return (
        c.astype(str)
        + "|"
        + df["intensity"].astype(str)
        + "|"
        + df["clarity"].astype(str)
        + "|"
        + df["shape"].astype(str)
        + "|"
        + l.astype(str)
        + "x"
        + w.astype(str)
        + "x"
        + d.astype(str)
    )


def dedupe(df):
    df = df.copy()
    n0 = len(df)
    df["_cert_key"] = _cert_key(df)
    has_cert = df["_cert_key"].str.split("|").str[-1].astype(str).str.len() > 0

    cert_df = df[has_cert].sort_values(["_cert_key", "date_seen"], ascending=[True, False])
    cert_df = cert_df.drop_duplicates(subset=["_cert_key"], keep="first")

    nocert_df = df[~has_cert].copy()
    nocert_df["_geom_key"] = _geom_bucket(nocert_df)
    nocert_df = nocert_df.sort_values(["_geom_key", "date_seen"], ascending=[True, False])
    nocert_df = nocert_df.drop_duplicates(subset=["_geom_key"], keep="first")

    out = pd.concat([cert_df, nocert_df], ignore_index=True)
    out = out.drop(columns=[c for c in ["_cert_key", "_geom_key"] if c in out.columns])
    print(f"Deduped: {n0} -> {len(out)}")
    return out


def build_quality_report(df):
    required = ["price_usd", "carat", "intensity", "clarity", "shape", "vendor", "source_url"]
    completeness = {c: float(df[c].notna().mean()) for c in required}
    parse_ok = (df["price_usd"].notna() & df["carat"].notna()).mean()
    report = {
        "row_count": int(len(df)),
        "vendor_counts": df["vendor"].fillna("Unknown").value_counts(dropna=False).to_dict(),
        "required_completeness": completeness,
        "price_carat_parse_success_rate": float(parse_ok),
        "missing_price_rows": int(df["price_usd"].isna().sum()),
        "missing_carat_rows": int(df["carat"].isna().sum()),
    }
    return report


def main(paths, out, quality_report_path=None, enforce_source_policy=False):
    ensure_runtime_deps()
    if enforce_source_policy:
        assert_min_active_sources(MIN_ACTIVE_SOURCES)

    frames = [normalize(load_any(p)) for p in paths if os.path.exists(p)]
    if not frames:
        raise SystemExit("No input files found.")

    df = pd.concat(frames, ignore_index=True, sort=False)
    df = df[CANON_COLS]
    df = dedupe(df)

    os.makedirs(os.path.dirname(out), exist_ok=True)
    df.to_csv(out, index=False)
    print("Wrote", out, "rows:", len(df))

    if quality_report_path:
        report = build_quality_report(df)
        os.makedirs(os.path.dirname(quality_report_path), exist_ok=True)
        with open(quality_report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print("Wrote quality report", quality_report_path)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument(
        "--inputs",
        nargs="+",
        default=["data/raw/bluenile.csv", "data/raw/leibish.csv", "data/raw/jamesallen.csv", "data/raw/ritani.csv"],
    )
    ap.add_argument("--quality-report", default="data/clean/yellow_unified_quality.json")
    ap.add_argument("--enforce-source-policy", action="store_true")
    args = ap.parse_args()
    main(args.inputs, args.out, quality_report_path=args.quality_report, enforce_source_policy=args.enforce_source_policy)
