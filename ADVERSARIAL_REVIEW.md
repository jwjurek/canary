# Adversarial Review: Canary Yellow Diamond Pricing Repository

Date: 2026-03-30
Reviewer stance: skeptical principal engineer

## Status update (2026-03-31)

The repository has materially improved since this review was first written. Specifically:

- Parser contract tests were added (fixture-based checks for Blue Nile/Leibish parsing contracts).
- Blue Nile and Leibish scrapers now support artifact capture (`--artifacts-dir`) and extraction logs.
- Dedupe moved to certificate-first with lab-aware keys plus tolerance-based geometry fallback.
- Pre-train quality-report gating was added.
- Baseline comparison, holdout metrics, and uncertainty-policy outputs were added to training.
- A monotone-constraint retry guard was added to mitigate fold-time constraint-size mismatches.

These are real improvements, but they do **not** close legal/compliance gaps, source-diversity risk, or transaction-ground-truth weaknesses.

## Executive verdict

The current repository is **not a credible production path** to a defensible canary/yellow diamond pricing system. It is a fragile scraping demo plus an optimistic modeling script. It can generate numbers, but it cannot yet generate trustworthy prices.

## Critical findings

1. **Single-source dependence disguised as multi-vendor strategy.** Two "scrapers" are placeholders that always output empty CSVs, leaving effectively Blue Nile + Leibish only, and Leibish dominates volume. GroupKFold-by-vendor with ~2 active groups is not meaningful robustness.
2. **No legal/compliance envelope.** There is no robots/TOS policy, no rate-limit protocol, no legal risk register, no provenance retention beyond a URL and scrape date.
3. **Brittle extraction contracts (partially mitigated).** Parser contract tests now exist, but extraction still relies on fragile CSS class substring selectors and title/slug regex fallbacks.
4. **Asking-price modeling without transaction anchoring.** The model trains on listing prices only, no sold comps, no bid/ask spread handling, no stale listing control.
5. **Dedupe remains heuristic-heavy for non-certified stones.** Cert-first/lab-aware matching is an improvement, but no pairwise precision/recall audit exists for no-cert geometry buckets.
6. **Quality gate exists but is minimal.** The quality-report requirement is progress, but gating is still mostly parse/completeness thresholding and not a full data validation regime.
7. **Model output packaging is incomplete for inference safety.** Saves model + preprocessor separately with no integrated prediction entrypoint, no versioned feature contract, and no drift monitor.

## Scraper/data acquisition audit

### Coverage reality
- README claims multi-vendor collection, but Ritani and James Allen scrapers are explicit non-viable placeholders writing empty CSVs. That means no actual expansion capacity demonstrated.
- Blue Nile query still hard-filters to FI/FV and one listing URL; deterministic frontier/artifact capture improved reliability, but source breadth remains narrow.

### Robustness and extraction fidelity
- Blue Nile relies on selector fragments like `[class*='wideJewelGridItemContainer']`, `[class*='price--']`, and `[class*='cui-info-table-rows']`. These are implementation-detail classes likely to churn.
- Leibish product link harvesting filters anchors by `/products/` + text containing "carat", which is heuristic and can miss products or capture irrelevant links.
- Both scrapers parse critical fields from human-readable title/slug regex fallbacks. That's a red flag for structured data integrity.
- Exception handling is mostly `except Exception: pass` in cookie and detail flows, causing silent data loss.

### Legality / risk / operational safety
- No robots.txt checks, no Terms-of-Service acceptance workflow, no request budget policy, no anti-bot escalation policy.
- No source reliability scoring (retailer, aggregator, marketplace, verified-cert source) despite mixing potentially heterogeneous listing semantics.
- HTML/spec sidecar artifact capture now exists for active scrapers; provenance is improved but still lacks a formal lineage/versioning contract.

### Provenance and maintainability
- Schema includes only coarse provenance fields (`vendor`, `source_url`, `date_seen`), with no extraction method version, selector version, parser confidence, or raw payload reference.
- Contract tests now cover `parse_spec_table`, `parse_detail_specs`, and title regex parsing.
- Blue Nile CLI was corrected to remove misleading dead pagination argument and uses deterministic frontier handling.

## Data/model credibility audit

### Data realism risks
- Target is 750–1,000 stones, but summary reports 1,496 rows mostly from one vendor; this is not market-representative coverage.
- Missingness is severe for key gemological fields on Leibish per summary text; yet model includes those features and relies on one-hot + passthrough with sparse factual support.
- No treatment of repeated listings over time, stale inventory, withdrawn stones, or price revisions.

### Deduplication quality
- Cert dedupe now includes lab-aware normalization; this reduces collisions but still needs empirical dedupe QA.
- Non-cert dedupe still uses tolerance-bucket heuristics and can mis-merge edge cases without an adjudicated match set.

### Modeling defensibility
- Uses GroupKFold with `n_splits=min(5, n_groups)`; with 2 effective vendors, robustness claims remain limited.
- Monotonic constraints only on carat-derived features; no economic sanity constraints on clarity/intensity interactions.
- Adds baseline comparison and holdout metrics, but still over-relies on listing-price labels and limited vendor diversity.
- SHAP is computed on transformed training data only; no holdout explanation or stability check.
- Baseline predictor and model-vs-baseline gate now exist (positive step).

## Implementation status vs prior top-10 actions

| Action | Status | Notes |
|---|---|---|
| 1. Source policy / min active sources | **Partially done** | Source registry and minimum-source gate exist, but operational CI enforcement is still manual. |
| 2. Legal compliance gate | **Not done** | Still no robots/TOS/legal signoff framework in code. |
| 3. Parser contract tests | **Done** | Fixture-based parser tests added. |
| 4. Deterministic frontier / dead args | **Done** | Blue Nile dead pagination arg removed; deterministic frontier behavior implemented. |
| 5. Raw artifacts + extraction logs | **Done** | Active scrapers emit artifacts/logs with `--artifacts-dir`. |
| 6. Certificate-first dedupe redesign | **Partially done** | Lab-aware cert matching + geometry fallback implemented; dedupe QA benchmarking still missing. |
| 7. Required data-quality artifact | **Done (basic)** | Quality report required by training unless explicitly bypassed. |
| 8. Baseline + holdout requirements | **Done (basic)** | Baseline and holdout metrics added with a minimum-improvement gate. |
| 9. Uncertainty + reject policy | **Done (basic)** | OOF residual-based uncertainty policy and sparse-feature reject rule added. |
| 10. Rewrite external claims/docs | **Not done** | README/SUMMARY still lag reality in places. |

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

**Continue with controlled scope (not production rollout).**

Do not treat this as production-ready. The repo moved from “fragile prototype” to “structured prototype with gates,” which is enough to continue iterative hardening. It is still not defensible for production pricing without legal/compliance controls, source expansion, and stronger ground-truth validation.
