"""Leibish yellow diamonds scraper."""
import argparse
import asyncio
import csv
from importlib.util import find_spec
import json
import os
import re
from pathlib import Path

from ._utils import clean_text, l_w_ratio, parse_float, parse_measurements, parse_price, today


def ensure_playwright():
    if find_spec("playwright") is None:
        raise SystemExit("Missing required dependency: playwright. Install with: python3 -m pip install -r requirements.txt && python3 -m playwright install chromium")


LIST_URL = "https://www.leibish.com/collections/yellow-diamonds?page={page}"
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
    r"(?P<carat>\d+(?:\.\d+)?)\s*Carat\s+"
    r"(?P<intensity>Fancy\s+(?:Vivid|Intense|Deep|Light|Dark|Yellow)(?:\s+(?:Vivid|Intense|Deep|Light|Dark|Yellow))*)\s+"
    r"(?P<shape>\w+(?:\s+\w+)?)\s+Diamond\s+"
    r"(?P<clarity>FL|IF|VVS1|VVS2|VS1|VS2|SI1|SI2|I1|I2|I3)\s+"
    r"(?P<lab>GIA|IGI)?",
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
    for sel in ["button:has-text('Accept')", "button:has-text('I Agree')", "button:has-text('Got it')"]:
        try:
            btn = page.locator(sel).first
            if await btn.is_visible():
                await btn.click()
                break
        except Exception:
            pass


def parse_detail_specs(texts):
    specs = {}
    patterns = [
        ("carat", re.compile(r"Weight:\s*(.+?)(?:Ct|ct|$)", re.I)),
        ("shape", re.compile(r"Shape:\s*(.+)", re.I)),
        ("intensity", re.compile(r"Intensity:\s*(.+)", re.I)),
        ("color", re.compile(r"Main\s*Color:\s*(.+)", re.I)),
        ("modifier", re.compile(r"Secondary\s*Color:\s*(.+)", re.I)),
        ("clarity", re.compile(r"Clarity:\s*(.+)", re.I)),
        ("fluorescence", re.compile(r"Fluorescence:\s*(.+)", re.I)),
        ("polish", re.compile(r"Polish:\s*(.+)", re.I)),
        ("symmetry", re.compile(r"Symmetry:\s*(.+)", re.I)),
        ("table_pct", re.compile(r"Table:\s*(.+?)%?\s*$", re.I)),
        ("depth_pct", re.compile(r"Depth:\s*(.+?)%?\s*$", re.I)),
        ("measurements", re.compile(r"Measurements?:\s*(.+)", re.I)),
        ("girdle", re.compile(r"Girdle:\s*(.+)", re.I)),
        ("culet", re.compile(r"Culet:\s*(.+)", re.I)),
    ]
    for text in texts:
        text = text.strip()
        for key, pat in patterns:
            m = pat.match(text)
            if m:
                specs[key] = m.group(1).strip()
                break
    return specs


async def get_product_links(page):
    links = await page.evaluate(
        """() => {
        const links = document.querySelectorAll('a[href*="/products/"]');
        const seen = new Set();
        const results = [];
        for (const a of links) {
            const href = a.href;
            if (!seen.has(href) && /carat/i.test(a.textContent)) {
                seen.add(href);
                results.push(href);
            }
        }
        return results;
    }"""
    )
    return sorted(set(links))


async def scrape_detail(browser, url, artifacts_dir=None):
    d = await browser.new_page()
    result = {"price": None, "specs": {}}
    try:
        await d.goto(url, wait_until="domcontentloaded")
        await d.wait_for_timeout(2500)

        price_text = await d.evaluate(
            """() => {
            const spans = document.querySelectorAll('span');
            for (const s of spans) {
                const t = s.textContent.trim();
                if (/^USD\s+[\d,]+$/.test(t)) return t;
            }
            return '';
        }"""
        )
        result["price"] = parse_price(price_text)

        spec_texts = await d.evaluate(
            """() => {
            const block = document.querySelector('.details-block');
            if (!block) return [];
            return [...block.querySelectorAll('p')].map(p => p.textContent.trim());
        }"""
        )
        result["specs"] = parse_detail_specs(spec_texts)

        report_text = await d.evaluate(
            """() => {
            const els = document.querySelectorAll('[class*="mb-1"]');
            for (const el of els) {
                const t = el.textContent.trim();
                if (/Report Type/i.test(t)) return t;
            }
            return '';
        }"""
        )
        if "GIA" in report_text.upper():
            result["specs"]["cert_lab"] = "GIA"
        elif "IGI" in report_text.upper():
            result["specs"]["cert_lab"] = "IGI"

        slug = url.split("/products/")[-1]
        _write_artifact(artifacts_dir, f"leibish_{slug}", ".detail.html", await d.content())
        _write_artifact(artifacts_dir, f"leibish_{slug}", ".detail.specs.json", result)
    except Exception as e:
        print(f"  [LB] Detail error {url}: {e}")
    finally:
        await d.close()
    return result


async def scrape(max_pages=52, outfile="data/raw/leibish.csv", headless=False, use_chrome=True, max_cards=None, artifacts_dir=None):
    ensure_playwright()
    from playwright.async_api import async_playwright

    rows = []
    extraction_log = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(channel="chrome" if use_chrome else None, headless=headless, args=["--disable-gpu"])

        all_links = []
        for page_num in range(1, max_pages + 1):
            page = await browser.new_page()
            url = LIST_URL.format(page=page_num)
            try:
                await page.goto(url, wait_until="domcontentloaded")
                if page_num == 1:
                    await accept_cookies(page)
                await page.wait_for_timeout(2200)
                links = await get_product_links(page)
                _write_artifact(artifacts_dir, f"leibish_list_page_{page_num}", ".html", await page.content())
                _write_artifact(artifacts_dir, f"leibish_list_page_{page_num}", ".links.json", links)
                if not links:
                    print(f"[LB] Page {page_num}: no products, stopping pagination")
                    await page.close()
                    break
                all_links.extend(links)
                print(f"[LB] Page {page_num}: {len(links)} products (total: {len(all_links)})")
            except Exception as e:
                print(f"[LB] Page {page_num} error: {e}")
            finally:
                await page.close()
            if max_cards and len(set(all_links)) >= max_cards:
                break

        frontier = sorted(set(all_links))
        if max_cards:
            frontier = frontier[:max_cards]

        print(f"[LB] Scraping {len(frontier)} detail pages...")
        for i, link in enumerate(frontier, start=1):
            detail = await scrape_detail(browser, link, artifacts_dir=artifacts_dir)
            specs = detail["specs"]
            price = detail["price"]

            tm = TITLE_RE.search(link.split("/products/")[-1].replace("-", " "))

            carat = parse_float(specs.get("carat")) or (parse_float(tm.group("carat")) if tm else None)
            intensity = specs.get("intensity", (tm.group("intensity") if tm else "") or "")
            clarity = specs.get("clarity", (tm.group("clarity") if tm else "") or "")
            shape = specs.get("shape", (tm.group("shape") if tm else "") or "")
            cert_lab = specs.get("cert_lab", (tm.group("lab") if tm else "") or "")
            modifier = specs.get("modifier", "")
            if modifier and modifier.lower() in ("no overtone", "none", "n/a"):
                modifier = ""

            meas = specs.get("measurements", "")
            l, w, dep = parse_measurements(meas)
            lw = l_w_ratio(l, w)

            slug = link.split("/products/")[-1] if "/products/" in link else f"lb_{i}"

            row = {
                "vendor": "Leibish",
                "source_url": link,
                "sku_or_stock": slug,
                "date_seen": today(),
                "price_usd": price,
                "carat": carat,
                "fancy_color_primary": specs.get("color", "Yellow"),
                "fancy_color_modifier": modifier,
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
                "l_w_ratio": lw,
                "measurements": clean_text(meas),
                "cert_lab": cert_lab,
                "cert_number": "",
                "is_natural": True,
                "is_lab_grown": False,
                "in_stock": True,
            }
            rows.append(row)
            extraction_log.append(
                {
                    "vendor": "Leibish",
                    "listing_key": slug,
                    "source_url": link,
                    "parse_ok": bool(price and carat),
                    "parsed_fields": sorted([k for k, v in row.items() if v not in ("", None)]),
                }
            )
            print(f"  [{i}/{len(frontier)}] {carat}ct {intensity} {shape} {clarity} ${price}")

        with open(outfile, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=HEADERS)
            w.writeheader()
            w.writerows(rows)

        _write_artifact(artifacts_dir, "leibish", ".extraction_log.json", extraction_log)
        print(f"[LB] Wrote {len(rows)} rows to {outfile}")
        await browser.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max_pages", type=int, default=52)
    ap.add_argument("--outfile", type=str, default="data/raw/leibish.csv")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--use-chrome", action="store_true")
    ap.add_argument("--max_cards", type=int, default=None)
    ap.add_argument("--artifacts-dir", type=str, default=None)
    args = ap.parse_args()
    asyncio.run(
        scrape(
            args.max_pages,
            args.outfile,
            headless=args.headless,
            use_chrome=args.use_chrome,
            max_cards=args.max_cards,
            artifacts_dir=args.artifacts_dir,
        )
    )
