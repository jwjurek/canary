import argparse
import json
import os
from importlib.util import find_spec


np = None
pd = None
shap = None
xgb = None
ColumnTransformer = None
mean_absolute_error = None
r2_score = None
GroupKFold = None
Pipeline = None
OneHotEncoder = None


def ensure_runtime_deps():
    global np, pd, shap, xgb, ColumnTransformer, mean_absolute_error, r2_score, GroupKFold, Pipeline, OneHotEncoder
    if np is not None:
        return
    needed = ["numpy", "pandas", "shap", "xgboost", "sklearn"]
    missing = [m for m in needed if find_spec(m) is None]
    if missing:
        raise SystemExit(
            "Missing required runtime dependencies: " + ", ".join(missing) +
            ". Install with: python3 -m pip install -r requirements.txt"
        )
    import numpy as _np
    import pandas as _pd
    import shap as _shap
    import xgboost as _xgb
    from sklearn.compose import ColumnTransformer as _ColumnTransformer
    from sklearn.metrics import mean_absolute_error as _mae, r2_score as _r2
    from sklearn.model_selection import GroupKFold as _GroupKFold
    from sklearn.pipeline import Pipeline as _Pipeline
    from sklearn.preprocessing import OneHotEncoder as _OneHotEncoder

    np = _np
    pd = _pd
    shap = _shap
    xgb = _xgb
    ColumnTransformer = _ColumnTransformer
    mean_absolute_error = _mae
    r2_score = _r2
    GroupKFold = _GroupKFold
    Pipeline = _Pipeline
    OneHotEncoder = _OneHotEncoder


PHYSICAL_CATS = ["intensity", "clarity", "shape", "polish", "symmetry", "fluorescence", "cert_lab"]
PHYSICAL_NUMS = ["carat", "length_mm", "width_mm", "depth_mm", "table_pct", "depth_pct", "l_w_ratio"]
KNOTS = [1.0, 1.5, 2.0, 3.0, 4.0, 5.0]


def add_knot_features(df):
    df = df.copy()
    for k in KNOTS:
        df[f"carat_over_{k}"] = (df["carat"] - k).clip(lower=0.0)
    df["carat_sq"] = df["carat"] ** 2
    return df


def build_pipeline(cat_levels=None):
    encoder_kwargs = {"handle_unknown": "ignore", "sparse_output": False}
    if cat_levels is not None:
        encoder_kwargs["categories"] = cat_levels
    pre = ColumnTransformer(
        [
            ("cat", OneHotEncoder(**encoder_kwargs), PHYSICAL_CATS),
            ("num", "passthrough", PHYSICAL_NUMS + ["carat_sq"] + [f"carat_over_{k}" for k in KNOTS]),
        ]
    )
    model = xgb.XGBRegressor(
        n_estimators=1200,
        learning_rate=0.03,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        objective="reg:squarederror",
        random_state=42,
        tree_method="hist",
    )
    return Pipeline([("pre", pre), ("model", model)])


def infer_constraints(feature_names):
    return [1 if name.startswith("num__carat_over_") or name in ["num__carat", "num__carat_sq"] else 0 for name in feature_names]


def fit_with_constraint_retry(pipe, x_train, y_train):
    try:
        pipe.fit(x_train, y_train)
    except xgb.core.XGBoostError as e:
        msg = str(e)
        if "monotone constraint" not in msg.lower():
            raise
        fn_fold = pipe.named_steps["pre"].fit(x_train).get_feature_names_out()
        cons_fold = infer_constraints(fn_fold)
        pipe.named_steps["model"].set_params(monotone_constraints="(" + ",".join(map(str, cons_fold)) + ")")
        pipe.fit(x_train, y_train)


def baseline_predict(train_df, test_df):
    # Simple benchmark: median log_price by 0.25ct bucket + intensity; fallback global median
    tr = train_df.copy()
    te = test_df.copy()
    tr["carat_bucket"] = (tr["carat"] / 0.25).round() * 0.25
    te["carat_bucket"] = (te["carat"] / 0.25).round() * 0.25
    grouped = tr.groupby(["carat_bucket", "intensity"], dropna=False)["log_price"].median().to_dict()
    global_med = float(tr["log_price"].median())

    preds = []
    for _, r in te.iterrows():
        preds.append(grouped.get((r["carat_bucket"], r["intensity"]), global_med))
    return np.array(preds)


def evaluate_holdouts(df, pipe, features):
    def _prep(frame):
        frame = frame.copy()
        for col in PHYSICAL_CATS:
            frame[col] = pd.Series(frame[col]).fillna("<MISSING>").astype(str)
        return frame

    results = {}
    # Vendor holdout
    vendor_metrics = []
    for vendor in sorted(df["vendor"].dropna().unique()):
        tr = df[df["vendor"] != vendor]
        te = df[df["vendor"] == vendor]
        if len(tr) < 50 or len(te) < 10:
            continue
        x_tr = _prep(tr[features])
        x_te = _prep(te[features])
        fit_with_constraint_retry(pipe, x_tr, tr["log_price"].values)
        pred = pipe.predict(x_te)
        base = baseline_predict(tr, te)
        vendor_metrics.append(
            {
                "vendor": vendor,
                "xgb_mae_log": float(mean_absolute_error(te["log_price"].values, pred)),
                "baseline_mae_log": float(mean_absolute_error(te["log_price"].values, base)),
            }
        )
    results["vendor_holdout"] = vendor_metrics

    # Time holdout (last 20% by date_seen)
    with_dates = df[df["date_seen"].notna()].copy()
    if len(with_dates) >= 100:
        with_dates = with_dates.sort_values("date_seen")
        split = int(len(with_dates) * 0.8)
        tr, te = with_dates.iloc[:split], with_dates.iloc[split:]
        if len(te) >= 20:
            x_tr = _prep(tr[features])
            x_te = _prep(te[features])
            fit_with_constraint_retry(pipe, x_tr, tr["log_price"].values)
            pred = pipe.predict(x_te)
            base = baseline_predict(tr, te)
            results["time_holdout"] = {
                "xgb_mae_log": float(mean_absolute_error(te["log_price"].values, pred)),
                "baseline_mae_log": float(mean_absolute_error(te["log_price"].values, base)),
                "cutoff_date": str(te["date_seen"].min()),
            }
    return results


def build_uncertainty_policy(df, oof_resid_abs):
    sparse_features = ["polish", "symmetry", "fluorescence", "table_pct", "depth_pct", "length_mm", "width_mm", "depth_mm"]
    sparse_count = df[sparse_features].isna().sum(axis=1)
    p90 = float(np.percentile(oof_resid_abs, 90))
    p75 = float(np.percentile(oof_resid_abs, 75))
    return {
        "sparse_feature_fields": sparse_features,
        "reject_if_missing_feature_count_ge": 4,
        "high_uncertainty_abs_log_error_threshold": p90,
        "medium_uncertainty_abs_log_error_threshold": p75,
        "policy": "Reject predictions when sparse-feature count >= threshold; otherwise attach uncertainty band.",
        "sparse_feature_missing_distribution": {
            "p50": float(np.percentile(sparse_count, 50)),
            "p90": float(np.percentile(sparse_count, 90)),
        },
    }


def main(data_path, out_dir, quality_report_path, require_quality_report=True, min_improvement=0.0):
    ensure_runtime_deps()
    os.makedirs(out_dir, exist_ok=True)

    if require_quality_report:
        if not quality_report_path or not os.path.exists(quality_report_path):
            raise SystemExit("Missing required quality report. Run merge_and_clean.py with --quality-report first.")
        with open(quality_report_path, "r", encoding="utf-8") as f:
            quality = json.load(f)
        if quality.get("price_carat_parse_success_rate", 0.0) < 0.9:
            raise SystemExit("Quality gate failed: price_carat_parse_success_rate < 0.9")

    df = pd.read_csv(data_path)
    df = df[df["is_lab_grown"] != True]
    df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")
    df["carat"] = pd.to_numeric(df["carat"], errors="coerce")
    df["date_seen"] = pd.to_datetime(df["date_seen"], errors="coerce")
    df = df.dropna(subset=["price_usd", "carat"])
    df = add_knot_features(df)
    df["log_price"] = np.log(df["price_usd"])

    features = PHYSICAL_CATS + PHYSICAL_NUMS + ["carat_sq"] + [f"carat_over_{k}" for k in KNOTS]
    groups = df["vendor"].fillna("Unknown")
    X = df[features]
    y = df["log_price"].values

    # Freeze categorical levels from full dataset so fold-level one-hot feature spaces remain stable.
    cat_levels = []
    for col in PHYSICAL_CATS:
        vals = pd.Series(X[col]).fillna("<MISSING>").astype(str)
        cat_levels.append(sorted(vals.unique().tolist()))

    X_model = X.copy()
    for col in PHYSICAL_CATS:
        X_model[col] = pd.Series(X_model[col]).fillna("<MISSING>").astype(str)

    pipe = build_pipeline(cat_levels=cat_levels)
    fn = pipe.named_steps["pre"].fit(X_model).get_feature_names_out()
    cons = infer_constraints(fn)
    pipe.named_steps["model"].set_params(monotone_constraints="(" + ",".join(map(str, cons)) + ")")

    n_groups = len(np.unique(groups))
    gkf = GroupKFold(n_splits=min(5, n_groups))
    maes, r2s, base_maes = [], [], []
    oof_resid_abs = []

    for tr_idx, te_idx in gkf.split(X_model, y, groups):
        tr_df = df.iloc[tr_idx]
        te_df = df.iloc[te_idx]
        fit_with_constraint_retry(pipe, X_model.iloc[tr_idx], y[tr_idx])
        pred = pipe.predict(X_model.iloc[te_idx])
        base = baseline_predict(tr_df, te_df)
        maes.append(mean_absolute_error(y[te_idx], pred))
        base_maes.append(mean_absolute_error(y[te_idx], base))
        r2s.append(r2_score(y[te_idx], pred))
        oof_resid_abs.extend(np.abs(y[te_idx] - pred).tolist())

    cv_mae = float(np.mean(maes))
    base_cv_mae = float(np.mean(base_maes))
    improvement = base_cv_mae - cv_mae
    if improvement < min_improvement:
        raise SystemExit(
            f"Model gate failed: improvement {improvement:.4f} < required {min_improvement:.4f} vs baseline MAE(log)."
        )

    holdout = evaluate_holdouts(df, pipe, features)

    fit_with_constraint_retry(pipe, X_model, y)
    metrics = {
        "cv_mae_log_mean": cv_mae,
        "cv_r2_mean": float(np.mean(r2s)),
        "baseline_cv_mae_log_mean": base_cv_mae,
        "cv_mae_improvement_vs_baseline": improvement,
        "n_samples": int(len(df)),
        "n_groups": int(n_groups),
        "feature_count": int(len(fn)),
        "holdout": holdout,
    }
    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    explainer = shap.TreeExplainer(pipe.named_steps["model"])
    X_trans = pipe.named_steps["pre"].transform(X_model)
    shap_values = explainer(X_trans, check_additivity=False)
    shap_abs = np.abs(shap_values.values).mean(axis=0)
    importances = pd.DataFrame({"feature": fn, "mean_abs_shap": shap_abs}).sort_values("mean_abs_shap", ascending=False)
    importances.to_csv(os.path.join(out_dir, "feature_importances_shap.csv"), index=False)

    uncertainty = build_uncertainty_policy(df, np.array(oof_resid_abs))
    with open(os.path.join(out_dir, "uncertainty_policy.json"), "w", encoding="utf-8") as f:
        json.dump(uncertainty, f, indent=2)

    pipe.named_steps["model"].save_model(os.path.join(out_dir, "xgb_model.json"))
    import joblib

    joblib.dump(pipe.named_steps["pre"], os.path.join(out_dir, "preprocessor.pkl"))
    print("Saved to", out_dir)
    print("CV metrics:", metrics)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--quality-report", default="data/clean/yellow_unified_quality.json")
    ap.add_argument("--allow-missing-quality-report", action="store_true")
    ap.add_argument("--min-improvement", type=float, default=0.0)
    args = ap.parse_args()
    main(
        args.data,
        args.out,
        quality_report_path=args.quality_report,
        require_quality_report=not args.allow_missing_quality_report,
        min_improvement=args.min_improvement,
    )
