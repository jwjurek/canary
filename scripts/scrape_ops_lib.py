import pandas as pd


def normalize_cert(value):
    if pd.isna(value):
        return ""
    s = str(value)
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits if 6 <= len(digits) <= 12 else ""


def build_listing_key(row):
    cert = normalize_cert(row.get("cert_number"))
    if cert:
        lab = str(row.get("cert_lab", "")).strip().upper() or "UNK"
        return f"CERT:{lab}:{cert}"
    sku = str(row.get("sku_or_stock", "")).strip()
    if sku:
        return f"SKU:{row.get('vendor','UNK')}:{sku}"
    return f"URL:{row.get('vendor','UNK')}:{row.get('source_url','')}"


def summarize_run(df):
    if df.empty:
        return {
            "rows": 0,
            "price_present_rate": 0.0,
            "carat_present_rate": 0.0,
            "parse_success_rate": 0.0,
            "cert_present_rate": 0.0,
        }
    price_ok = df["price_usd"].notna().mean() if "price_usd" in df.columns else 0.0
    carat_ok = df["carat"].notna().mean() if "carat" in df.columns else 0.0
    cert_ok = df["cert_number"].astype(str).str.len().gt(0).mean() if "cert_number" in df.columns else 0.0
    return {
        "rows": int(len(df)),
        "price_present_rate": float(price_ok),
        "carat_present_rate": float(carat_ok),
        "parse_success_rate": float((price_ok + carat_ok) / 2),
        "cert_present_rate": float(cert_ok),
    }


def detect_mispriced(history_df, z_thresh=2.5, min_points=5, undervalued_drop_pct=0.2):
    if history_df.empty:
        return history_df.copy()

    df = history_df.copy()
    df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")
    df = df.dropna(subset=["price_usd", "listing_key", "scraped_at"]).copy()
    if df.empty:
        return df

    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    df = df.dropna(subset=["scraped_at"])
    if df.empty:
        return df

    stats = df.groupby("listing_key")["price_usd"].agg(["count", "median", "std"]).reset_index()
    stats = stats[stats["count"] >= min_points].copy()
    if stats.empty:
        return df.iloc[0:0].copy()

    latest_idx = df.sort_values("scraped_at").groupby("listing_key").tail(1).index
    latest = df.loc[latest_idx].copy()
    latest = latest.merge(stats, on="listing_key", how="inner")
    if latest.empty:
        return latest

    latest["std"] = latest["std"].replace(0, pd.NA)
    latest["z_score"] = (latest["price_usd"] - latest["median"]) / latest["std"]
    latest["drop_pct_vs_median"] = (latest["median"] - latest["price_usd"]) / latest["median"]
    latest["is_mispriced"] = latest["z_score"].abs() >= z_thresh
    latest["is_potentially_undervalued"] = latest["drop_pct_vs_median"] >= undervalued_drop_pct
    return latest.sort_values(["is_potentially_undervalued", "z_score"], ascending=[False, True])
