# Yellow Diamonds Valuation Project (Clean Build)

Collect ~750–1,000 **natural** yellow diamonds (Fancy Intense / Fancy Vivid) from multiple vendors,
deduplicate by certificate, and train an XGBoost model with **monotone constraints** and **GroupKFold-by-vendor** CV.

## Quickstart (macOS, Python 3.12)
```bash
python3.12 -m venv .venv && source .venv/bin/activate
.venv/bin/python -m pip install --upgrade pip setuptools wheel
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m playwright install chromium

mkdir -p data/raw
.venv/bin/python -m scrapers.blue_nile_scraper --max_cards 20 --use-chrome

.venv/bin/python scripts/merge_and_clean.py --out data/clean/yellow_unified.csv
.venv/bin/python scripts/train_model.py --data data/clean/yellow_unified.csv --out models

# Repeated multi-vendor scrape cycle with history + potential mispricing flags
.venv/bin/python scripts/run_scrape_cycle.py --vendors bluenile,leibish --max_cards 50 --max_pages 2

# Optional: enforce meaningful growth versus previous run
.venv/bin/python scripts/run_scrape_cycle.py --vendors bluenile,leibish --require-growth --min-new-rows 100
```
