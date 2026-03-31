"""Blue Nile FI/FV yellow diamonds scraper."""
import argparse
import asyncio
import csv
import json
import os
import re
from importlib.util import find_spec
from pathlib import Path

from ._utils import clean_text, l_w_ratio, parse_float, parse_measurements, parse_price, today


def ensure_playwright():
    if find_spec("playwright") is None:
        raise SystemExit("Missing required dependency: playwright. Install with: python3 -m pip install -r requirements.txt && python3 -m playwright install chromium")


LIST_URL = (
    "https://www.bluenile.com/diamonds/fancy-color-diamonds/all"
    "?CaratTo=20&Color=yellow&FancyIntensity=FI,FV&Sort=Price+desc"
)
CARD_SEL = "[class*='wideJewelGridItemContainer']"
HEADERS = [
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

TITLE_RE = re.compile(
    r"(?P<lab>GIA|IGI)?\s*"
    r"(?P<carat>\d+(?:\.\d+)?)\s*Carat\s+"
    r"(?P<intensity>Fancy\s+(?:Vivid|Intense|Deep|Light|Dark|Yellow)(?:\s+(?:Vivid|Intense|Deep|Light|Dark|Yellow))*)\s+"
    r"(?P<color>\w+)[-–]\s*"
    r"(?P<clarity>FL|IF|VVS1|VVS2|VS1|VS2|SI1|SI2|I1|I2|I3)\s+"
    r"(?P<shape>.+?)\s+(?:Cut\s+)?Diamond",
    re.IGNORECASE,
)


def _write_artifact(artifacts_dir, key, suffix, payload):
    if not artifacts_dir:
        return
    os.makedirs(artifacts_dir, exist_ok=True)
    path = Path(artifacts_dir) / f"{key}{suffix}"
    if isinstance(payload, (dict, list)):
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    else:
        path.write_text(str(payload), encoding="utf-8")


async def accept_cookies(page):
    for sel in ["button:has-text('Accept')", "button:has-text('I Agree')", "button:has-text('Got it')", "text=Accept All"]:
        try:
            if await page.locator(sel).first.is_visible():
                await page.locator(sel).first.click()
                break
        except Exception:
            pass


async def scroll_to_load(page, max_scrolls=20, pause=0.8):
    last = 0
    stable = 0
    for _ in range(max_scrolls):
        cards = await page.locator(CARD_SEL).count()
        if cards > last:
            last = cards
            stable = 0
        else:
            stable += 1
        if stable >= 2:
            break
        await page.mouse.wheel(0, 22000)
        await page.wait_for_timeout(int(pause * 1000))
    return last


def parse_spec_table(rows_text):
    specs = {}
    patterns = [
        ("stock", re.compile(r"Stock\s*Number\s*(.+)", re.I)),
        ("shape", re.compile(r"^Shape\s*(.+)", re.I)),
        ("color", re.compile(r"^Color\s*(.+)", re.I)),
        ("clarity", re.compile(r"^Clarity\s*(.+)", re.I)),
        ("carat", re.compile(r"Carat\s*Weight\s*(.+)", re.I)),
        ("fluorescence", re.compile(r"Fluorescence\s*(.+)", re.I)),
        ("lw_ratio", re.compile(r"Length/Width\s*Ratio\s*(.+)", re.I)),
        ("depth_pct", re.compile(r"Depth\s*%\s*(.+)", re.I)),
        ("table_pct", re.compile(r"Table\s*%\s*(.+)", re.I)),
        ("polish", re.compile(r"^Polish\s*(.+)", re.I)),
        ("symmetry", re.compile(r"^Symmetry\s*(.+)", re.I)),
        ("girdle", re.compile(r"^Girdle\s*(.+)", re.I)),
        ("culet", re.compile(r"^Culet\s*(.+)", re.I)),
        ("intensity", re.compile(r"^Intensity\s*(.+)", re.I)),
        ("measurements", re.compile(r"Measurements?\s*(.+)", re.I)),
    ]
    for text in rows_text:
        text = text.strip()
        for key, pat in patterns:
            m = pat.match(text)
            if m:
                specs[key] = m.group(1).strip()
                break
    return specs


async def scrape_detail(browser, url, pw_timeout_exc, artifacts_dir=None):
    d = await browser.new_page()
    specs = {}
    try:
        await d.goto(url, wait_until="domcontentloaded")
        await accept_cookies(d)
        try:
            await d.wait_for_selector("[class*='cui-info-table']", timeout=12000)
        except pw_timeout_exc:
            pass
        try:
            btn = d.locator("text=Show More").first
            if await btn.is_visible():
                await btn.click()
                await d.wait_for_timeout(800)
        except Exception:
            pass
        rows_text = await d.evaluate(
            """() => {
            const container = document.querySelector('[class*="cui-info-table-rows"]');
            if (!container) return [];
            return [...container.children].map(r => r.textContent.trim());
        }"""
        )
        specs = parse_spec_table(rows_text)
        stock = specs.get("stock") or "unknown"
        _write_artifact(artifacts_dir, f"bluenile_{stock}", ".detail.html", await d.content())
        _write_artifact(artifacts_dir, f"bluenile_{stock}", ".detail.specs.json", specs)
    except Exception as e:
        print(f"  [BN] Detail error {url}: {e}")
    finally:
        await d.close()
    return specs


async def scrape(outfile="data/raw/bluenile.csv", headless=False, use_chrome=True, max_cards=None, artifacts_dir=None):
    ensure_playwright()
    from playwright.async_api import TimeoutError as PWTimeout
    from playwright.async_api import async_playwright

    rows = []
    extraction_log = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(channel="chrome" if use_chrome else None, headless=headless, args=["--disable-gpu"])
        page = await browser.new_page()
        await page.goto(LIST_URL, wait_until="domcontentloaded")
        await accept_cookies(page)
        try:
            await page.wait_for_selector(CARD_SEL, timeout=20000)
        except pw_timeout_exc:
            await scroll_to_load(page, max_scrolls=3)
            await page.wait_for_selector(CARD_SEL, timeout=20000)

        loaded = await scroll_to_load(page, max_scrolls=30)
        print(f"[BN] Cards after scroll: {loaded}")

        card_data = await page.evaluate(
            """(sel) => {
            const cards = document.querySelectorAll(sel);
            return [...cards].map(card => {
                const link = card.querySelector('a');
                const h3 = card.querySelector('h3');
                const priceDiv = card.querySelector('[class*="price--"]');
                return {
                    href: link ? link.href : '',
                    title: h3 ? h3.textContent.trim() : '',
                    price: priceDiv ? priceDiv.textContent.trim() : ''
                };
            });
        }""",
            CARD_SEL,
        )

        _write_artifact(artifacts_dir, "bluenile_list", ".html", await page.content())
        _write_artifact(artifacts_dir, "bluenile_list", ".cards.json", card_data)

        # Deterministic crawl frontier
        unique = {c.get("href"): c for c in card_data if c.get("href")}
        frontier = [unique[k] for k in sorted(unique.keys())]
        if max_cards:
            frontier = frontier[:max_cards]

        print(f"[BN] Extracted {len(frontier)} cards from page")
        for i, cd in enumerate(frontier, start=1):
            url = cd.get("href", "")
            title = cd.get("title", "")
            price = parse_price(cd.get("price", ""))

            tm = TITLE_RE.search(title)
            cert_lab = (tm.group("lab") or "") if tm else ""
            carat = parse_float(tm.group("carat")) if tm else None
            intensity = clean_text(tm.group("intensity")) if tm else ""
            clarity = clean_text(tm.group("clarity")) if tm else ""
            shape = clean_text(tm.group("shape")) if tm else ""

            specs = await scrape_detail(browser, url, PWTimeout, artifacts_dir=artifacts_dir)

            if specs.get("clarity"):
                clarity = specs["clarity"]
            if specs.get("intensity"):
                intensity = specs["intensity"]
            if specs.get("shape"):
                shape = specs["shape"]
            if specs.get("carat"):
                carat = parse_float(specs["carat"]) or carat
            if specs.get("stock"):
                stock = specs["stock"]
            else:
                m = re.search(r"(\d+)", url.split("/")[-1])
                stock = m.group(1) if m else f"bn_{i}"

            meas = specs.get("measurements", "")
            l, w, dep = parse_measurements(meas)
            lw = l_w_ratio(l, w)

            row = {
                "vendor": "BlueNile",
                "source_url": url,
                "sku_or_stock": stock,
                "date_seen": today(),
                "price_usd": price,
                "carat": carat,
                "fancy_color_primary": "Yellow",
                "fancy_color_modifier": "",
                "intensity": intensity,
                "clarity": clarity,
                "shape": shape,
                "polish": specs.get("polish", ""),
                "symmetry": specs.get("symmetry", ""),
                "fluorescence": specs.get("fluorescence", ""),
                "length_mm": l,
                "width_mm": w,
                "depth_mm": dep,
                "table_pct": parse_float(specs.get("table_pct")),
                "depth_pct": parse_float(specs.get("depth_pct")),
                "girdle": specs.get("girdle", ""),
                "culet": specs.get("culet", ""),
                "l_w_ratio": lw or parse_float(specs.get("lw_ratio")),
                "measurements": clean_text(meas),
                "cert_lab": cert_lab or "GIA",
                "cert_number": stock,
                "is_natural": True,
                "is_lab_grown": False,
                "in_stock": True,
            }
            rows.append(row)
            extraction_log.append(
                {
                    "vendor": "BlueNile",
                    "listing_key": stock,
                    "source_url": url,
                    "parse_ok": bool(price and carat),
                    "parsed_fields": sorted([k for k, v in row.items() if v not in ("", None)]),
                }
            )
            print(f"  [{i}/{len(frontier)}] {carat}ct {intensity} {shape} {clarity} ${price}")

        with open(outfile, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=HEADERS)
            w.writeheader()
            w.writerows(rows)

        _write_artifact(artifacts_dir, "bluenile", ".extraction_log.json", extraction_log)
        print(f"[BN] Wrote {len(rows)} rows to {outfile}")
        await browser.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--outfile", type=str, default="data/raw/bluenile.csv")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--use-chrome", action="store_true")
    ap.add_argument("--max_cards", type=int, default=None)
    ap.add_argument("--artifacts-dir", type=str, default=None)
    args = ap.parse_args()
    asyncio.run(
        scrape(
            args.outfile,
            headless=args.headless,
            use_chrome=args.use_chrome,
            max_cards=args.max_cards,
            artifacts_dir=args.artifacts_dir,
        )
    )
