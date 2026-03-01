import datetime
import json
import os
import re
import time as _time
from urllib.parse import urlparse

import feedparser
import requests
from dateutil import parser as dateutil_parser

LATEST_JSON = "docs/data/latest.json"
FEEDS_TXT = "feeds.txt"
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

    haystack = " ".join(
        [
            str(item.get("title") or ""),
            str(item.get("preview") or ""),
            str(item.get("description") or ""),
            str(item.get("source_url") or ""),
        ]
    )
    match_text = re.search(r"\b(20\d{2})\b", haystack)
    if match_text:
        return int(match_text.group(1))

    return None


def mentions_target_year(item: dict, year: int) -> bool:
    target = str(year)
    haystack = " ".join(
        [
            str(item.get("title") or ""),
            str(item.get("preview") or ""),
            str(item.get("description") or ""),
            str(item.get("url") or ""),
            str(item.get("source_url") or ""),
        ]
    )
    return re.search(rf"\b{re.escape(target)}\b", haystack) is not None


def is_pdf_url(url: str) -> bool:
    if not url:
        return False
    return url.lower().split("?")[0].endswith(".pdf")


def resolve_final_url(url: str) -> str:
    if not url:
        return ""
    try:
        response = requests.head(url, allow_redirects=True, timeout=12, headers={"User-Agent": UA})
        if response.url:
            return str(response.url)
    except requests.RequestException:
        pass
    try:
        response = requests.get(url, allow_redirects=True, timeout=12, headers={"User-Agent": UA}, stream=True)
        if response.url:
            return str(response.url)
    except requests.RequestException:
        pass
    return url


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


def load_feed_urls(path: str = FEEDS_TXT) -> list[str]:
    urls: list[str] = []
    if not os.path.exists(path):
        return urls
    with open(path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if " - http" in line:
                url = line.split(" - ", 1)[1].strip()
            else:
                url = line
            if url.startswith("http"):
                urls.append(url)
    return urls


def _parse_entry_datetime(entry) -> datetime.datetime | None:
    for attr in ("published_parsed", "updated_parsed"):
        val = entry.get(attr)
        if val:
            try:
                ts = _time.mktime(val)
                return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
            except (OverflowError, OSError):
                pass
    for attr in ("published", "updated"):
        val = entry.get(attr)
        if val:
            try:
                dt = dateutil_parser.parse(str(val))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                return dt
            except (ValueError, OverflowError):
                pass
    return None


def _extract_best_link(entry) -> str:
    direct = str(entry.get("link") or "").strip()
    if direct:
        return direct
    links = entry.get("links") or []
    if isinstance(links, list):
        for link in links:
            if not isinstance(link, dict):
                continue
            href = str(link.get("href") or "").strip()
            if href:
                return href
    return ""


def _items_from_feeds(feed_urls: list[str]) -> list[dict]:
    items: list[dict] = []
    for url in feed_urls:
        try:
            parsed = feedparser.parse(url, request_headers={"User-Agent": UA})
        except Exception:
            continue

        entries = getattr(parsed, "entries", []) or []
        for entry in entries:
            title = norm(entry.get("title") or "")
            link = _extract_best_link(entry)
            if not title or not link:
                continue

            published_dt = _parse_entry_datetime(entry)
            published_iso = published_dt.date().isoformat() if published_dt else ""

            source = entry.get("source") or {}
            publisher = ""
            if isinstance(source, dict):
                publisher = norm(source.get("title") or "")
            if not publisher:
                publisher = domain_of(link)

            summary = norm(entry.get("summary") or entry.get("description") or "")
            tags = entry.get("tags") or []
            categories: list[str] = []
            if isinstance(tags, list):
                for tag in tags:
                    if isinstance(tag, dict):
                        term = norm(tag.get("term") or "")
                        if term:
                            categories.append(term)

            items.append(
                {
                    "id": str(entry.get("id") or entry.get("guid") or ""),
                    "title": title,
                    "url": link,
                    "source_url": url,
                    "preview": summary,
                    "description": summary,
                    "publisher": publisher,
                    "publishedAt": published_iso,
                    "dateISO": published_iso,
                    "categories": categories,
                    "tags": categories,
                    "sector": "",
                }
            )

    return items


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


def _merge_items(primary: list[dict], secondary: list[dict]) -> list[dict]:
    merged: list[dict] = []
    seen: set[str] = set()
    for item in primary + secondary:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        title = str(item.get("title") or "").strip()
        if not url or not title:
            continue
        key = url.split("#", 1)[0]
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)
    return merged


def main() -> None:
    with open(LATEST_JSON, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    latest_items = _items_from_latest(data)
    feed_items = _items_from_feeds(load_feed_urls())
    items = _merge_items(latest_items, feed_items)
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
        if year != TARGET_YEAR and not mentions_target_year(item, TARGET_YEAR):
            continue

        if not looks_like_research(haystack):
            # Keep only if domain is strongly institutional and it resolves to PDF.
            pass

        final_url = resolve_final_url(url)
        if not final_url:
            continue

        if not allowed_domain(final_url):
            continue

        pdf = is_pdf_url(final_url)
        if not pdf:
            ok, resolved = head_is_pdf(final_url)
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
