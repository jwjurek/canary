"""Parser contracts shared by scrapers and tests."""

import re

BN_TITLE_RE = re.compile(
    r"(?P<lab>GIA|IGI)?\s*"
    r"(?P<carat>\d+(?:\.\d+)?)\s*Carat\s+"
    r"(?P<intensity>Fancy\s+(?:Vivid|Intense|Deep|Light|Dark|Yellow)(?:\s+(?:Vivid|Intense|Deep|Light|Dark|Yellow))*)\s+"
    r"(?P<color>\w+)[-–]\s*"
    r"(?P<clarity>FL|IF|VVS1|VVS2|VS1|VS2|SI1|SI2|I1|I2|I3)\s+"
    r"(?P<shape>.+?)\s+(?:Cut\s+)?Diamond",
    re.IGNORECASE,
)

LB_TITLE_RE = re.compile(
    r"(?P<carat>\d+(?:\.\d+)?)\s*Carat\s+"
    r"(?P<intensity>Fancy\s+(?:Vivid|Intense|Deep|Light|Dark|Yellow)(?:\s+(?:Vivid|Intense|Deep|Light|Dark|Yellow))*)\s+"
    r"(?P<shape>\w+(?:\s+\w+)?)\s+Diamond\s+"
    r"(?P<clarity>FL|IF|VVS1|VVS2|VS1|VS2|SI1|SI2|I1|I2|I3)\s+"
    r"(?P<lab>GIA|IGI)?",
    re.IGNORECASE,
)


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
