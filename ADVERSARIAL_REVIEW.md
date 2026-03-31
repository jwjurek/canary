# Adversarial Review: Canary Yellow Diamond Pricing Repository

Date: 2026-03-30
Reviewer stance: skeptical principal engineer

## Executive verdict

The current repository is **not a credible production path** to a defensible canary/yellow diamond pricing system. It is a fragile scraping demo plus an optimistic modeling script. It can generate numbers, but it cannot yet generate trustworthy prices.

## Critical findings

1. **Single-source dependence disguised as multi-vendor strategy.** Two "scrapers" are placeholders that always output empty CSVs, leaving effectively Blue Nile + Leibish only, and Leibish dominates volume. GroupKFold-by-vendor with ~2 active groups is not meaningful robustness.
2. **No legal/compliance envelope.** There is no robots/TOS policy, no rate-limit protocol, no legal risk register, no provenance retention beyond a URL and scrape date.
3. **Brittle extraction contracts.** Scrapers rely on CSS class substring selectors and page text regexes with no DOM contract tests; one frontend refresh can silently corrupt fields.
4. **Asking-price modeling without transaction anchoring.** The model trains on listing prices only, no sold comps, no bid/ask spread handling, no stale listing control.
5. **Dedupe logic can collapse distinct stones and retain false duplicates.** Attribute-key dedupe on rounded dimensions, shape, clarity, intensity, carat is collision-prone and not certificate-verified for large portions of data.
6. **No data quality gate before training.** Pipeline drops NaNs in only price/carat and proceeds; no schema validation report, outlier quarantine, label-noise estimation, or vendor-shift diagnostics.
7. **Model output packaging is incomplete for inference safety.** Saves model + preprocessor separately with no integrated prediction entrypoint, no versioned feature contract, and no drift monitor.

## Scraper/data acquisition audit

### Coverage reality
- README claims multi-vendor collection, but Ritani and James Allen scrapers are explicit non-viable placeholders writing empty CSVs. That means no actual expansion capacity demonstrated.
- Blue Nile query hard-filters to FI/FV and one listing URL; no true pagination implementation despite `max_pages` argument.

### Robustness and extraction fidelity
- Blue Nile relies on selector fragments like `[class*='wideJewelGridItemContainer']`, `[class*='price--']`, and `[class*='cui-info-table-rows']`. These are implementation-detail classes likely to churn.
- Leibish product link harvesting filters anchors by `/products/` + text containing "carat", which is heuristic and can miss products or capture irrelevant links.
- Both scrapers parse critical fields from human-readable title/slug regex fallbacks. That's a red flag for structured data integrity.
- Exception handling is mostly `except Exception: pass` in cookie and detail flows, causing silent data loss.

### Legality / risk / operational safety
- No robots.txt checks, no Terms-of-Service acceptance workflow, no request budget policy, no anti-bot escalation policy.
- No source reliability scoring (retailer, aggregator, marketplace, verified-cert source) despite mixing potentially heterogeneous listing semantics.
- No capture of HTML snapshots or response hashes for auditability/reproducibility.

### Provenance and maintainability
- Schema includes only coarse provenance fields (`vendor`, `source_url`, `date_seen`), with no extraction method version, selector version, parser confidence, or raw payload reference.
- No unit tests for `parse_spec_table`, `parse_detail_specs`, title regexes, or measurement parsing edge-cases.
- `max_pages` in Blue Nile scrape signature is effectively ignored by logic, signaling maintenance drift between API and implementation.

## Data/model credibility audit

### Data realism risks
- Target is 750–1,000 stones, but summary reports 1,496 rows mostly from one vendor; this is not market-representative coverage.
- Missingness is severe for key gemological fields on Leibish per summary text; yet model includes those features and relies on one-hot + passthrough with sparse factual support.
- No treatment of repeated listings over time, stale inventory, withdrawn stones, or price revisions.

### Deduplication quality
- Cert dedupe extracts 6–10 digit substrings from `cert_number`, which risks false matches and ignores certificate lab namespace.
- Non-cert dedupe key uses rounded numeric and categorical concatenation; can merge non-identical stones and retain duplicates with slight rounding perturbations.

### Modeling defensibility
- Uses GroupKFold with `n_splits=min(5, n_groups)`; with 2 vendors, it's effectively 2-fold, unstable and easy to over-read.
- Monotonic constraints only on carat-derived features; no economic sanity constraints on clarity/intensity interactions.
- Evaluates on log-price MAE/R² but does not report calibration error in original dollars, tail risk, or uncertainty intervals.
- SHAP is computed on transformed training data only; no holdout explanation or stability check.
- No baseline comparison (e.g., price-per-carat stratified medians) to justify XGBoost complexity.

## Phased go-forward plan

### Phase 0 (1–2 weeks): credibility stopgap
- Freeze modeling claims.
- Implement legal/compliance checklist per source (robots, TOS, contact/legal signoff).
- Add extraction contract tests with fixture HTML snapshots for each source.
- Add row-level provenance columns: `scraper_version`, `selector_version`, `parse_confidence`, `raw_artifact_id`.

### Phase 1 (2–4 weeks): acquisition hardening
- Replace brittle class selectors with semantic anchors / structured JSON-LD where available.
- Build incremental crawler with retry taxonomy and explicit error classes.
- Persist raw HTML + parsed JSON sidecar for every listing.
- Add source health dashboard (success rate, field completeness, selector break detection).

### Phase 2 (2–4 weeks): dataset defensibility
- Introduce certificate-centric identity graph (lab + cert + measurements tolerance).
- Add dedupe precision/recall audit set manually labeled on sampled pairs.
- Define model-ready contract with mandatory fields and completeness thresholds.
- Add vendor/time stratified validation slices and leakage checks.

### Phase 3 (3–6 weeks): model validity
- Establish strong baselines first, then gradient boosting.
- Add uncertainty estimation (quantile model or conformal intervals).
- Evaluate in dollar space and business metrics (median absolute dollar error by carat buckets).
- Run temporal backtests and vendor holdout stress tests.

### Phase 4 (ongoing): productionization
- Single packaged artifact (preprocess + model + schema validator).
- Continuous drift monitoring and retraining triggers.
- Human-review loop for low-confidence predictions.

## Top 10 actions if inheriting tomorrow

1. Remove/flag non-viable scrapers from the "supported" pipeline and fail CI if active source count <3.
2. Add legal compliance gate before any scraper can run in CI/cron.
3. Introduce parser contract tests with frozen HTML fixtures for both active sources.
4. Implement robust pagination and deterministic crawl frontier; kill misleading `max_pages` dead args.
5. Store raw page artifacts and extraction logs keyed by listing id.
6. Redesign dedupe around certificate-first matching with lab-aware keys and tolerance-based geometric matching.
7. Add data quality report (completeness, cardinality, outliers, parse-failure rate) as required pre-train artifact.
8. Build simple benchmark models and require beating them on vendor/time holdouts.
9. Add uncertainty outputs and reject-option policy for sparse-feature stones.
10. Rewrite README/SUMMARY claims to reflect actual capability and explicit limitations.

## Final judgment

**Reset, not continue.**

Do not scale this code as-is. Keep only the skeleton and rebuild around compliance, provenance, robust acquisition, and validation discipline. Right now the system can produce a model file, but not a pricing system you can defend in front of legal, data science, or commercial leadership.
