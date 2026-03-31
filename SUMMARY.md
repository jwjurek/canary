# Yellow Diamonds Pricing Tool — Development Summary

## Objective

Build an XGBoost pricing model for natural yellow diamonds, calibrated to data scraped from online vendors. Target: 750–1,000+ stones with monotone constraints on carat-related features and GroupKFold cross-validation by vendor.

---

## Work Completed

### 1. Pipeline Bug Fixes

Five bugs were fixed in the data pipeline before any scraping:

| File | Bug | Fix |
|---|---|---|
| `merge_and_clean.py` | `dedupe()` treated all NaN cert numbers as duplicates, collapsing uncertified rows to 1 | Split certified/uncertified before dedup; only deduplicate on cert_number for rows that have one |
| `merge_and_clean.py` | `normalize()` called before missing columns were added, crashing on `.astype(str)` | Moved column backfill into `normalize()` |
| `train_model.py` | Saved model was from the last CV fold, not a full-data refit | Added `pipe.fit(X, y)` after the CV loop |
| `train_model.py` | `sparse=False` deprecated in scikit-learn ≥1.4 | Changed to `sparse_output=False` |
| `train_model.py` | Pipeline was fit twice on full data before CV (once to get feature names, once with constraints) | Use `preprocessor.fit(X)` alone for feature names |

### 2. Scrapers Built

#### Blue Nile (working — 56 rows)
- Rewrote with current DOM selectors: cards in `[class*="wideJewelGridItemContainer"]`, H3 title with structured text (e.g., "GIA 5.06 Carat Fancy Vivid Yellow-VVS2 Round Cut Diamond"), price in `div[class*="price--"]`
- Detail pages: click "Show More" to reveal full spec table in `[class*="cui-info-table-rows"]`, extracting stock number, shape, color, clarity, carat, fluorescence, depth%, table%, polish, symmetry, girdle, culet, intensity, measurements
- Scroll-to-load pagination on list page

#### Leibish (working — 1,440 rows)
- Built from scratch for their Shopify-based site
- Paginated collection at `/collections/yellow-diamonds?page=N` (52 pages, ~28 items/page)
- Detail page specs in `.details-block` `<p>` tags (Weight, Shape, Intensity, Color, Clarity, Fluorescence, Polish, Table, Measurements)
- Prices extracted from `<span>` elements matching `"USD <number>"`

#### Ritani (dropped)
- Colored diamonds page returns "NO RESULTS FOUND" when filtered to natural + yellow
- Only lab-grown colored diamonds in inventory

#### James Allen (dropped)
- Automated access immediately blocked by bot detection / CAPTCHA
- "your activity and behavior on this site made us think that you are a bot"

### 3. Data Collected

**Combined: 1,496 raw rows (1,412 with both price and carat)**

| Metric | Blue Nile | Leibish |
|---|---|---|
| Rows | 56 | 1,440 |
| Price range | $44,540 – $71,260 | $520 – $1,213,780 |
| Carat range | 1.50 – 4.52 | 0.10 – 25.13 |
| Intensities | FI (34), FV (22) | Fancy (384), FI (324), FV (204), FD (177), FL (144), +others |
| Shapes | 10 shapes | 21 shapes |
| Price+carat completeness | 100% | 94% |
| Polish completeness | 80% | 75% |
| Measurements completeness | 80% | 93% |

**Known gaps:**
- Leibish: symmetry (0%) and depth_pct (0%) — these fields aren't shown on their site
- Leibish: fluorescence only 40% populated
- Blue Nile: small sample (56), only FI/FV intensity, narrow carat range (1.5–4.5)
- ~84 Leibish rows (6%) failed detail page loads — have URL but no specs

---

## Next Steps

### Phase 1: Data Pipeline (immediate)

1. **Run `merge_and_clean.py`** to merge Blue Nile + Leibish, deduplicate by cert number and by physical attributes
2. **Assess data quality** post-merge — distribution of missing values, outlier detection, vendor overlap
3. **Filter to model-ready subset** — rows with price, carat, intensity, clarity, shape at minimum

### Phase 2: Model Training

4. **Run `train_model.py`** on cleaned data — XGBoost with monotone carat constraints, GroupKFold by vendor
5. **Evaluate**: CV MAE (log-price), R², residual analysis by vendor/shape/carat bucket
6. **Check for vendor bias** — Leibish dominates (96% of data); Blue Nile prices may cluster differently
7. **SHAP analysis** — verify carat is dominant, check if intensity/clarity rankings are sensible

### Phase 3: Model Refinement

8. **Hyperparameter tuning** — learning rate, depth, n_estimators grid search within GroupKFold
9. **Feature engineering** — consider log(price_per_carat) as target instead of log(price); add carat × intensity interactions
10. **Handle missing features** — imputation strategy for polish, symmetry, fluorescence; or drop from model and re-evaluate

### Phase 4: Expand Data Acquisition

The current dataset is dominated by Leibish (96%). Adding vendors would improve:
- **Vendor diversity** for more robust GroupKFold CV
- **Price calibration** across different market positions
- **Coverage** of shapes/intensities underrepresented in current data

#### Candidate vendors to investigate:

| Vendor | Why promising | Potential challenges |
|---|---|---|
| **Brilliant Earth** | Large inventory, structured pages, GIA-certified | May require API reverse-engineering or Playwright; possible bot protection |
| **Angara** | Carries fancy yellow diamonds, clear product pages | Smaller inventory, may not have enough FI/FV |
| **Worthy** (auction data) | Real transaction prices (not asking prices) | Data format differs; may need different scraping approach |
| **Rapaport / PriceScope** | Industry pricing references | Paywalled or require account; may not be scrapable |
| **StoneAlgo / Rare Carat** | Aggregator sites with structured data | Aggregated data may overlap with existing vendors |
| **Langerman Diamonds** | Specialist in fancy color | Smaller catalog, European pricing |

#### Alternative data expansion strategies:

1. **Re-run existing scrapers periodically** — prices change, inventory rotates. Running weekly would capture new listings and track price movements over time, adding temporal signal.
2. **Broaden intensity filter on Blue Nile** — currently limited to FI/FV. Adding Fancy Light/Fancy/Fancy Deep would increase Blue Nile yield and match Leibish's broader coverage.
3. **Leibish sub-collections** — their site also has `/collections/fancy-intense-yellow-diamonds`, `/collections/fancy-vivid-yellow-diamonds`, etc. These may surface diamonds not in the main yellow collection.
4. **Lab-grown yellow diamonds** as negative examples — could improve the model's ability to distinguish natural pricing patterns, though this changes the problem scope.
5. **Historical data** — if Wayback Machine snapshots of these vendors exist, they could provide temporal depth. However, extraction would be fragile.
6. **Manual GIA report lookup** — for diamonds with cert numbers, the GIA website provides verified specs. This could fill gaps in polish, symmetry, depth%, and confirm data accuracy.
