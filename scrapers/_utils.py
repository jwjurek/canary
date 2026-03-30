import re
from datetime import datetime
DATE_FMT = "%Y-%m-%d"
def clean_text(s): return re.sub(r"\s+", " ", (s or "").strip())
def parse_price(s):
    s = (s or "").replace(",", "").replace("$","").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    return float(m.group(1)) if m else None
def parse_float(s):
    try: return float(str(s).strip())
    except Exception: return None
def parse_measurements(s):
    if not s: return None, None, None
    s = s.lower().replace("mm","").replace("×","x")
    m = re.findall(r"(\d+(?:\.\d+)?)", s)
    if len(m) >= 3: return float(m[0]), float(m[1]), float(m[2])
    return None, None, None
def l_w_ratio(l, w):
    if not l or not w: return None
    return round(max(l, w) / min(l, w), 3)
def today(): return datetime.utcnow().strftime(DATE_FMT)
