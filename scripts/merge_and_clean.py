import argparse, os, pandas as pd, numpy as np
CANON_COLS = ["vendor","source_url","sku_or_stock","date_seen","price_usd","carat","fancy_color_primary","fancy_color_modifier","intensity",
              "clarity","shape","polish","symmetry","fluorescence","length_mm","width_mm","depth_mm","table_pct","depth_pct","girdle","culet","l_w_ratio","measurements",
              "cert_lab","cert_number","is_natural","is_lab_grown","in_stock"]
def load_any(path):
    if path.endswith(".csv"): return pd.read_csv(path)
    if path.endswith(".json"): return pd.read_json(path, lines=True)
    raise ValueError(f"Unsupported: {path}")
def normalize(df):
    df = df.copy()
    for c in CANON_COLS:
        if c not in df.columns:
            df[c] = np.nan
    df["date_seen"] = pd.to_datetime(df["date_seen"], errors="coerce").dt.date
    for c in ["price_usd","carat","length_mm","width_mm","depth_mm","table_pct","depth_pct","l_w_ratio"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["intensity"] = df["intensity"].astype(str).str.strip().str.title()
    return df
def dedupe(df):
    df = df.copy()
    n0 = len(df)
    df["cert_number_norm"] = df["cert_number"].astype(str).str.extract(r"(\d{6,10})")[0]
    has_cert = df["cert_number_norm"].notna()
    df_cert = df[has_cert].sort_values(["cert_number_norm","date_seen"], ascending=[True, False])
    df_cert = df_cert.drop_duplicates(subset=["cert_number_norm"], keep="first")
    df = pd.concat([df_cert, df[~has_cert]], ignore_index=True)
    key = (df["carat"].round(2).astype(str) + "|" + df["intensity"].astype(str) + "|" + df["clarity"].astype(str) + "|" + df["shape"].astype(str) + "|" +
           df["length_mm"].round(2).astype(str) + "x" + df["width_mm"].round(2).astype(str) + "x" + df["depth_mm"].round(2).astype(str))
    df = df.loc[~key.duplicated(keep="first")].drop(columns=["cert_number_norm"])
    print(f"Deduped: {n0} -> {len(df)}"); return df
def main(paths, out):
    frames = [normalize(load_any(p)) for p in paths if os.path.exists(p)]
    if not frames: raise SystemExit("No input files found.")
    df = pd.concat(frames, ignore_index=True, sort=False)
    df = df[CANON_COLS]; df = dedupe(df); df.to_csv(out, index=False)
    print("Wrote", out, "rows:", len(df))
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--inputs", nargs="+", default=["data/raw/bluenile.csv","data/raw/leibish.csv","data/raw/jamesallen.csv","data/raw/ritani.csv"])
    args = ap.parse_args(); main(args.inputs, args.out)
