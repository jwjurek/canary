# Vendor Expansion Plan (Phase 1/2)

Current directly scraped vendors: **BlueNile, Leibish**.

To move beyond 2 vendors quickly, use a dual-track approach:

1. **Direct scraper track** (longer lead time): add native scrapers where access is viable.
2. **CSV import track** (short lead time): ingest normalized vendor exports via `import:<alias>` in `run_scrape_cycle.py`.

## Priority onboarding order

1. Brilliance (import first, scraper later)
2. Adiamor (import first, scraper later)
3. Whiteflash / Brilliant Earth (import first, scraper feasibility review)
4. Specialty dealers with yellow inventory depth (import feed agreements)

## Exit-oriented targets

- At least 3 active vendors per run.
- Dominant vendor share <= 60%.
- 3x+ listing and cert coverage vs baseline.
- Weighted parse success >= 0.80 and weighted cert presence >= 0.70.

## Operational notes

- Treat import feeds as first-class sources with the same schema and provenance fields.
- Keep a per-vendor onboarding checklist (legal/TOS, field mapping, dedupe behavior).
- Promote import vendor to native scraper only after stable field mapping and repeatability are proven.
