import argparse, os, json, numpy as np, pandas as pd
from sklearn.model_selection import GroupKFold
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, r2_score
import xgboost as xgb, shap
PHYSICAL_CATS = ["intensity","clarity","shape","polish","symmetry","fluorescence","cert_lab"]
PHYSICAL_NUMS = ["carat","length_mm","width_mm","depth_mm","table_pct","depth_pct","l_w_ratio"]
KNOTS = [1.0,1.5,2.0,3.0,4.0,5.0]
def add_knot_features(df):
    df = df.copy()
    for k in KNOTS: df[f"carat_over_{k}"] = (df["carat"] - k).clip(lower=0.0)
    df["carat_sq"] = df["carat"]**2; return df
def build_pipeline():
    pre = ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), PHYSICAL_CATS),
                             ("num", "passthrough", PHYSICAL_NUMS + ["carat_sq"] + [f"carat_over_{k}" for k in KNOTS])])
    model = xgb.XGBRegressor(n_estimators=1200, learning_rate=0.03, max_depth=6, subsample=0.8, colsample_bytree=0.8,
                             reg_lambda=1.0, objective="reg:squarederror", random_state=42, tree_method="hist")
    return Pipeline([("pre", pre), ("model", model)])
def infer_constraints(feature_names):
    cons = []
    for name in feature_names:
        cons.append(1 if name.startswith("num__carat_over_") or name in ["num__carat","num__carat_sq"] else 0)
    return cons
def main(data_path, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    df = pd.read_csv(data_path)
    df = df[df["is_lab_grown"] != True]
    df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")
    df["carat"] = pd.to_numeric(df["carat"], errors="coerce")
    df = df.dropna(subset=["price_usd","carat"])
    df = add_knot_features(df); df["log_price"] = np.log(df["price_usd"])
    features = PHYSICAL_CATS + PHYSICAL_NUMS + ["carat_sq"] + [f"carat_over_{k}" for k in KNOTS]
    groups = df["vendor"].fillna("Unknown"); X = df[features]; y = df["log_price"].values
    pipe = build_pipeline()
    fn = pipe.named_steps["pre"].fit(X).get_feature_names_out()
    cons = infer_constraints(fn); pipe.named_steps["model"].set_params(monotone_constraints="(" + ",".join(map(str, cons)) + ")")
    n_groups = len(np.unique(groups))
    gkf = GroupKFold(n_splits=min(5, n_groups)); maes, r2s = [], []
    for tr, te in gkf.split(X, y, groups):
        pipe.fit(X.iloc[tr], y[tr]); pred = pipe.predict(X.iloc[te])
        maes.append(mean_absolute_error(y[te], pred)); r2s.append(r2_score(y[te], pred))
    pipe.fit(X, y)
    metrics = {"cv_mae_log_mean": float(np.mean(maes)), "cv_r2_mean": float(np.mean(r2s)),
               "n_samples": int(len(df)), "n_groups": int(n_groups), "feature_count": int(len(fn))}
    with open(os.path.join(out_dir, "metrics.json"), "w") as f: json.dump(metrics, f, indent=2)
    explainer = shap.TreeExplainer(pipe.named_steps["model"])
    X_trans = pipe.named_steps["pre"].transform(X); shap_values = explainer(X_trans, check_additivity=False)
    shap_abs = np.abs(shap_values.values).mean(axis=0)
    importances = pd.DataFrame({"feature": fn, "mean_abs_shap": shap_abs}).sort_values("mean_abs_shap", ascending=False)
    importances.to_csv(os.path.join(out_dir, "feature_importances_shap.csv"), index=False)
    pipe.named_steps["model"].save_model(os.path.join(out_dir, "xgb_model.json"))
    import joblib; joblib.dump(pipe.named_steps["pre"], os.path.join(out_dir, "preprocessor.pkl"))
    print("Saved to", out_dir); print("CV metrics:", metrics)
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True); ap.add_argument("--out", required=True)
    args = ap.parse_args(); main(args.data, args.out)
