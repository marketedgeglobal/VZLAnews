import datetime
import json
import os
import re
from urllib.parse import urlparse

import requests

LATEST_JSON = "docs/data/latest.json"
TARGET_YEAR = 2025
OUT_JSON = "docs/data/pdf_publications_2025.json"

TODAY = datetime.date.today()
UA = "Mozilla/5.0 (compatible; MarketEdgeVZLAnews/1.0; +https://marketedgeglobal.github.io/VZLAnews/)"

ALLOWED_DOMAINS = [
    "imf.org",
    "worldbank.org",
    "iadb.org",
    "caf.com",
    "reliefweb.int",
    "ochaonline",
    "who.int",
    "paho.org",
    "un.org",
    "unicef.org",
    "ilo.org",
    "unesco.org",
    "unctad.org",
    "arxiv.org",
    "papers.ssrn.com",
    "ssrn.com",
    "doi.org",
    "openalex.org",
]

PAYWALL_HINTS = [
    "sciencedirect.com",
    "link.springer.com",
    "tandfonline.com",
    "jstor.org",
    "wiley.com",
    "nature.com",
    "cambridge.org/core",
    "sagepub.com",
    "ieeexplore.ieee.org",
    "academic.oup.com",
]

RESEARCH_HINTS = [
    "report",
    "working paper",
    "policy paper",
    "publication",
    "study",
    "assessment",
    "evaluation",
    "country report",
    "diagnostic",
    "systematic",
    "survey",
    "analysis",
    "bulletin",
    "technical note",
    "discussion paper",
    "research paper",
]

VZ_KEYS = [
    "venezuela",
    "venezuelan",
    "caracas",
    "pdvsa",
    "bolivarian republic of venezuela",
    "república bolivariana de venezuela",
    "república bolivariana",
    "miraflores",
]


def norm(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def allowed_domain(url: str) -> bool:
    domain = domain_of(url)
    if not domain:
        return False
    if any(pattern in domain for pattern in PAYWALL_HINTS):
        return False
    return any(domain.endswith(allowed) or allowed in domain for allowed in ALLOWED_DOMAINS)


def vz_relevant(text: str) -> bool:
    low = (text or "").lower()
    return any(key in low for key in VZ_KEYS)


def looks_like_research(text: str) -> bool:
    low = (text or "").lower()
    return any(key in low for key in RESEARCH_HINTS)


def infer_year(item: dict) -> int | None:
    published = item.get("publishedAt") or item.get("dateISO") or ""
    if published:
        match = re.match(r"^(20\d{2})", str(published))
        if match:
            return int(match.group(1))
    url = item.get("url") or ""
    match_url = re.search(r"(20\d{2})", str(url))
    if match_url:
        return int(match_url.group(1))
    return None


def is_pdf_url(url: str) -> bool:
    if not url:
        return False
    return url.lower().split("?")[0].endswith(".pdf")


def head_is_pdf(url: str) -> tuple[bool, str]:
    try:
        response = requests.head(url, allow_redirects=True, timeout=15, headers={"User-Agent": UA})
        content_type = (response.headers.get("content-type") or "").lower()
        return ("application/pdf" in content_type, response.url or url)
    except requests.RequestException:
        return False, url


def split_sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[\.\!\?])\s+(?=[A-ZÁÉÍÓÚÑ])", (text or "").strip())
    return [part.strip() for part in parts if part.strip()]


def make_abstract(item: dict) -> str:
    text = norm(item.get("preview") or "")
    if not text:
        insight = item.get("insight2") or {}
        if isinstance(insight, dict):
            text = norm(" ".join([insight.get("s1", ""), insight.get("s2", "")]))
    if not text:
        text = norm(item.get("description") or item.get("snippet") or "")

    text = re.sub(
        r"Comprehensive up-to-date news coverage.*?Google News\.?",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()

    sentences = [sentence for sentence in split_sentences(text) if len(sentence) >= 35]
    if not sentences:
        return (text[:320] + "…") if len(text) > 320 else text
    return " ".join(sentences[:4])


def _items_from_latest(payload: dict) -> list[dict]:
    direct_items = payload.get("items")
    if isinstance(direct_items, list):
        return [item for item in direct_items if isinstance(item, dict)]

    all_items = payload.get("allItems")
    if isinstance(all_items, list):
        return [item for item in all_items if isinstance(item, dict)]

    sectors = payload.get("sectors")
    if isinstance(sectors, list):
        flattened: list[dict] = []
        for sector in sectors:
            if not isinstance(sector, dict):
                continue
            for item in sector.get("items") or []:
                if isinstance(item, dict):
                    flattened.append(item)
        return flattened

    return []


def main() -> None:
    with open(LATEST_JSON, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    items = _items_from_latest(data)
    publications = []
    seen: set[str] = set()

    for item in items:
        url = item.get("url") or ""
        title = norm(item.get("title") or "")
        if not url or not title:
            continue

        haystack = " ".join(
            [
                title,
                norm(item.get("preview") or ""),
                norm(item.get("description") or ""),
                " ".join(item.get("tags") or []),
                " ".join(item.get("categories") or []),
            ]
        )
        if not vz_relevant(haystack):
            continue

        year = infer_year(item)
        if year != TARGET_YEAR:
            continue

        if not looks_like_research(haystack):
            # Keep only if domain is strongly institutional and it resolves to PDF.
            pass

        if not allowed_domain(url):
            continue

        final_url = url
        pdf = is_pdf_url(url)
        if not pdf:
            ok, resolved = head_is_pdf(url)
            if not ok:
                continue
            pdf = True
            final_url = resolved

        if not pdf:
            continue

        dedupe_key = final_url.split("#")[0]
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        publisher = norm(item.get("publisher") or domain_of(final_url))
        published_at = item.get("publishedAt") or item.get("dateISO") or str(TARGET_YEAR)

        publications.append(
            {
                "id": item.get("id") or "",
                "title": title,
                "url": final_url,
                "publisher": publisher,
                "publishedAt": published_at,
                "year": TARGET_YEAR,
                "sector": item.get("sector") or "",
                "abstract": make_abstract(item),
            }
        )

    publications.sort(key=lambda publication: str(publication.get("publishedAt") or ""), reverse=True)

    output = {
        "asOf": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "year": TARGET_YEAR,
        "count": len(publications),
        "publications": publications[:25],
    }

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
