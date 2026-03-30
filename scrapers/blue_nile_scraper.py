"""Blue Nile FI/FV yellow diamonds scraper"""
import argparse, asyncio, csv, re
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from ._utils import clean_text, parse_price, parse_float, parse_measurements, l_w_ratio, today

LIST_URL = ("https://www.bluenile.com/diamonds/fancy-color-diamonds/all"
            "?CaratTo=20&Color=yellow&FancyIntensity=FI,FV&Sort=Price+desc")
CARD_SEL = "[class*='wideJewelGridItemContainer']"
HEADERS = ["vendor","source_url","sku_or_stock","date_seen","price_usd","carat",
           "fancy_color_primary","fancy_color_modifier","intensity","clarity","shape","polish","symmetry","fluorescence",
           "length_mm","width_mm","depth_mm","table_pct","depth_pct","girdle","culet","l_w_ratio","measurements",
           "cert_lab","cert_number","is_natural","is_lab_grown","in_stock"]

# Regex to parse the structured card title, e.g.:
# "GIA 5.06 Carat Fancy Vivid Yellow-VVS2 Round Cut Diamond"
TITLE_RE = re.compile(
    r"(?P<lab>GIA|IGI)?\s*"
    r"(?P<carat>\d+(?:\.\d+)?)\s*Carat\s+"
    r"(?P<intensity>Fancy\s+(?:Vivid|Intense|Deep|Light|Dark|Yellow)(?:\s+(?:Vivid|Intense|Deep|Light|Dark|Yellow))*)\s+"
    r"(?P<color>\w+)[-–]\s*"
    r"(?P<clarity>FL|IF|VVS1|VVS2|VS1|VS2|SI1|SI2|I1|I2|I3)\s+"
    r"(?P<shape>.+?)\s+(?:Cut\s+)?Diamond",
    re.IGNORECASE)


async def accept_cookies(page):
    for sel in ["button:has-text('Accept')","button:has-text('I Agree')","button:has-text('Got it')","text=Accept All"]:
        try:
            if await page.locator(sel).first.is_visible():
                await page.locator(sel).first.click(); break
        except Exception:
            pass


async def scroll_to_load(page, max_scrolls=10, pause=0.8):
    last = 0
    for _ in range(max_scrolls):
        cards = await page.locator(CARD_SEL).count()
        if cards > last:
            last = cards
            await page.mouse.wheel(0, 20000)
            await page.wait_for_timeout(int(pause * 1000))
        else:
            break
    return last


def parse_spec_table(rows_text):
    """Parse list of 'LabelValue' strings from the detail page spec table."""
    specs = {}
    patterns = [
        ("stock",       re.compile(r"Stock\s*Number\s*(.+)", re.I)),
        ("shape",       re.compile(r"^Shape\s*(.+)", re.I)),
        ("color",       re.compile(r"^Color\s*(.+)", re.I)),
        ("clarity",     re.compile(r"^Clarity\s*(.+)", re.I)),
        ("carat",       re.compile(r"Carat\s*Weight\s*(.+)", re.I)),
        ("fluorescence",re.compile(r"Fluorescence\s*(.+)", re.I)),
        ("lw_ratio",    re.compile(r"Length/Width\s*Ratio\s*(.+)", re.I)),
        ("depth_pct",   re.compile(r"Depth\s*%\s*(.+)", re.I)),
        ("table_pct",   re.compile(r"Table\s*%\s*(.+)", re.I)),
        ("polish",      re.compile(r"^Polish\s*(.+)", re.I)),
        ("symmetry",    re.compile(r"^Symmetry\s*(.+)", re.I)),
        ("girdle",      re.compile(r"^Girdle\s*(.+)", re.I)),
        ("culet",       re.compile(r"^Culet\s*(.+)", re.I)),
        ("intensity",   re.compile(r"^Intensity\s*(.+)", re.I)),
        ("measurements",re.compile(r"Measurements?\s*(.+)", re.I)),
    ]
    for text in rows_text:
        text = text.strip()
        for key, pat in patterns:
            m = pat.match(text)
            if m:
                specs[key] = m.group(1).strip()
                break
    return specs


async def scrape_detail(browser, url):
    """Open a detail page, click Show More, return parsed specs dict."""
    d = await browser.new_page()
    specs = {}
    try:
        await d.goto(url, wait_until="domcontentloaded")
        await accept_cookies(d)
        try:
            await d.wait_for_selector("[class*='cui-info-table']", timeout=10000)
        except PWTimeout:
            pass
        # Click "Show More" to reveal all spec rows
        try:
            btn = d.locator("text=Show More").first
            if await btn.is_visible():
                await btn.click()
                await d.wait_for_timeout(1000)
        except Exception:
            pass
        # Extract all rows from the spec table
        rows_text = await d.evaluate("""() => {
            const container = document.querySelector('[class*="cui-info-table-rows"]');
            if (!container) return [];
            return [...container.children].map(r => r.textContent.trim());
        }""")
        specs = parse_spec_table(rows_text)
    except Exception as e:
        print(f"  [BN] Detail error {url}: {e}")
    finally:
        await d.close()
    return specs


async def scrape(max_pages=1, outfile="data/raw/bluenile.csv", headless=False, use_chrome=True, max_cards=None):
    rows = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            channel="chrome" if use_chrome else None, headless=headless, args=["--disable-gpu"])
        page = await browser.new_page()
        await page.goto(LIST_URL, wait_until="domcontentloaded")
        await accept_cookies(page)
        try:
            await page.wait_for_selector(CARD_SEL, timeout=20000)
        except PWTimeout:
            await scroll_to_load(page, max_scrolls=2)
            await page.wait_for_selector(CARD_SEL, timeout=20000)
        loaded = await scroll_to_load(page, max_scrolls=20)
        print(f"[BN] Cards after scroll: {loaded}")

        # Extract card-level data (title, price, link) from all visible cards
        card_data = await page.evaluate("""(sel) => {
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
        }""", CARD_SEL)

        print(f"[BN] Extracted {len(card_data)} cards from page")
        for i, cd in enumerate(card_data):
            if max_cards and len(rows) >= max_cards:
                break
            url = cd.get("href", "")
            if not url:
                continue

            title = cd.get("title", "")
            price = parse_price(cd.get("price", ""))

            # Parse structured title
            tm = TITLE_RE.search(title)
            cert_lab = (tm.group("lab") or "") if tm else ""
            carat = parse_float(tm.group("carat")) if tm else None
            intensity = clean_text(tm.group("intensity")) if tm else ""
            clarity = clean_text(tm.group("clarity")) if tm else ""
            shape = clean_text(tm.group("shape")) if tm else ""

            # Scrape detail page for full specs
            specs = await scrape_detail(browser, url)

            # Detail specs override card-level data where available
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
                stock = m.group(1) if m else ""

            meas = specs.get("measurements", "")
            l, w, dep = parse_measurements(meas)
            lw = l_w_ratio(l, w)

            rows.append({
                "vendor": "BlueNile", "source_url": url, "sku_or_stock": stock,
                "date_seen": today(), "price_usd": price, "carat": carat,
                "fancy_color_primary": "Yellow", "fancy_color_modifier": "",
                "intensity": intensity, "clarity": clarity, "shape": shape,
                "polish": specs.get("polish", ""), "symmetry": specs.get("symmetry", ""),
                "fluorescence": specs.get("fluorescence", ""),
                "length_mm": l, "width_mm": w, "depth_mm": dep,
                "table_pct": parse_float(specs.get("table_pct")),
                "depth_pct": parse_float(specs.get("depth_pct")),
                "girdle": specs.get("girdle", ""), "culet": specs.get("culet", ""),
                "l_w_ratio": lw or parse_float(specs.get("lw_ratio")),
                "measurements": clean_text(meas),
                "cert_lab": cert_lab or "GIA", "cert_number": stock,
                "is_natural": True, "is_lab_grown": False, "in_stock": True,
            })
            print(f"  [{i+1}/{len(card_data)}] {carat}ct {intensity} {shape} {clarity} ${price}")

        with open(outfile, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=HEADERS)
            w.writeheader()
            w.writerows(rows)
        print(f"[BN] Wrote {len(rows)} rows to {outfile}")
        await browser.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--max_pages", type=int, default=1)
    ap.add_argument("--outfile", type=str, default="data/raw/bluenile.csv")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--use-chrome", action="store_true")
    ap.add_argument("--max_cards", type=int, default=None)
    args = ap.parse_args()
    asyncio.run(scrape(args.max_pages, args.outfile, headless=args.headless,
                       use_chrome=args.use_chrome, max_cards=args.max_cards))
