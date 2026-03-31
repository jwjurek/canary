"""Source registry and support-status policy."""

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Source:
    name: str
    module: str
    active: bool
    reason: str = ""


SOURCES: List[Source] = [
    Source(name="BlueNile", module="scrapers.blue_nile_scraper", active=True),
    Source(name="Leibish", module="scrapers.leibish_scraper", active=True),
    Source(
        name="JamesAllen",
        module="scrapers.james_allen_scraper",
        active=False,
        reason="Blocked by bot protection / CAPTCHA",
    ),
    Source(
        name="Ritani",
        module="scrapers.ritani_scraper",
        active=False,
        reason="No natural yellow inventory",
    ),
]


MIN_ACTIVE_SOURCES = 3


def active_sources() -> List[Source]:
    return [s for s in SOURCES if s.active]


def assert_min_active_sources(min_sources: int = MIN_ACTIVE_SOURCES) -> None:
    active = active_sources()
    if len(active) < min_sources:
        names = ", ".join(s.name for s in active) or "<none>"
        raise RuntimeError(
            f"Active source policy failed: {len(active)} active source(s) ({names}), "
            f"requires >= {min_sources}."
        )
