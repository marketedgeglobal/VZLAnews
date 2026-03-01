import datetime
import json
import os
import re
import time as _time
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import feedparser
import requests
from dateutil import parser as dateutil_parser

LATEST_JSON = "docs/data/latest.json"
FEEDS_TXT = "feeds.txt"
TODAY = datetime.date.today()
YEAR_MAX = TODAY.year
YEAR_MIN = TODAY.year - 2
TARGET_YEARS = set(range(YEAR_MIN, YEAR_MAX + 1))
OUT_JSON = "docs/data/pdf_publications_recent.json"

UA = "Mozilla/5.0 (compatible; MarketEdgeVZLAnews/1.0; +https://marketedgeglobal.github.io/VZLAnews/)"

ALLOWED_DOMAINS = [
    "imf.org",
    "worldbank.org",
    "iadb.org",
    "caf.com",
    "undp.org",
    "cepal.org",
    "eclac.org",
    "fao.org",
    "ifpri.org",
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
    "brookings.edu",
    "csis.org",
    "wilsoncenter.org",
    "chathamhouse.org",
    "carnegieendowment.org",
    "iisd.org",
    "wri.org",
    "odi.org",
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

SECTOR_HINTS = [
    "extractives",
    "oil",
    "gas",
    "mining",
    "agriculture",
    "food security",
    "health",
    "public health",
    "geopolitics",
    "governance",
    "business environment",
    "private sector",
    "economic growth",
    "macroeconomic",
    "inflation",
    "environment",
    "sustainability",
    "climate",
    "energy transition",
    "infrastructure",
    "water",
]

PUBLICATION_FEED_URLS = [
    "https://news.google.com/rss/search?q=Venezuela+filetype:pdf+report+OR+publication+OR+working+paper&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:worldbank.org+Venezuela+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:iadb.org+Venezuela+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:undp.org+Venezuela+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:imf.org+Venezuela+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:un.org+Venezuela+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:brookings.edu+Venezuela+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:csis.org+Venezuela+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:wilsoncenter.org+Venezuela+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=site:chathamhouse.org+Venezuela+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=Venezuela+extractives+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=Venezuela+agriculture+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=Venezuela+health+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=Venezuela+geopolitics+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=Venezuela+business+environment+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=Venezuela+economic+growth+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://news.google.com/rss/search?q=Venezuela+environment+sustainability+filetype:pdf+report&hl=en-US&gl=US&ceid=US:en",
    "https://www.bing.com/news/search?q=site:worldbank.org+Venezuela+report&format=rss",
    "https://www.bing.com/news/search?q=site:iadb.org+Venezuela+report&format=rss",
    "https://www.bing.com/news/search?q=site:undp.org+Venezuela+report&format=rss",
    "https://www.bing.com/news/search?q=site:imf.org+Venezuela+report&format=rss",
    "https://www.bing.com/news/search?q=site:un.org+Venezuela+report&format=rss",
    "https://www.bing.com/news/search?q=site:cepal.org+Venezuela+report&format=rss",
    "https://www.bing.com/news/search?q=site:brookings.edu+Venezuela+report&format=rss",
    "https://www.bing.com/news/search?q=site:csis.org+Venezuela+report&format=rss",
    "https://www.bing.com/news/search?q=site:wilsoncenter.org+Venezuela+report&format=rss",
    "https://www.bing.com/news/search?q=site:chathamhouse.org+Venezuela+report&format=rss",
    "https://www.bing.com/news/search?q=Venezuela+extractives+report+pdf&format=rss",
    "https://www.bing.com/news/search?q=Venezuela+agriculture+report+pdf&format=rss",
    "https://www.bing.com/news/search?q=Venezuela+health+report+pdf&format=rss",
    "https://www.bing.com/news/search?q=Venezuela+geopolitics+report+pdf&format=rss",
    "https://www.bing.com/news/search?q=Venezuela+business+environment+report+pdf&format=rss",
    "https://www.bing.com/news/search?q=Venezuela+economic+growth+report+pdf&format=rss",
    "https://www.bing.com/news/search?q=Venezuela+environment+sustainability+report+pdf&format=rss",
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


def topic_relevant(text: str) -> bool:
    low = (text or "").lower()
    return any(key in low for key in SECTOR_HINTS)


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
    years = [int(found) for found in re.findall(r"\b(20\d{2})\b", haystack)]
    in_range = [year for year in years if YEAR_MIN <= year <= YEAR_MAX]
    if in_range:
        return max(in_range)

    return None


def mentions_target_years(item: dict, years: set[int]) -> bool:
    haystack = " ".join(
        [
            str(item.get("title") or ""),
            str(item.get("preview") or ""),
            str(item.get("description") or ""),
            str(item.get("url") or ""),
            str(item.get("source_url") or ""),
        ]
    )
    for year in years:
        target = str(year)
        if re.search(rf"\b{re.escape(target)}\b", haystack):
            return True
    return False


def is_pdf_url(url: str) -> bool:
    if not url:
        return False
    return url.lower().split("?")[0].endswith(".pdf")


def unwrap_search_redirect(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return url

    domain = (parsed.netloc or "").lower()
    query = parse_qs(parsed.query or "")

    if "bing.com" in domain and parsed.path.startswith("/news/apiclick"):
        target = (query.get("url") or [""])[0]
        if target:
            return unquote(target)

    if "news.google.com" in domain and "url" in query:
        target = (query.get("url") or [""])[0]
        if target:
            return unquote(target)

    return url


def resolve_final_url(url: str) -> str:
    url = unwrap_search_redirect(url)
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


def extract_pdf_links_from_page(url: str) -> list[str]:
    links: list[str] = []
    try:
        response = requests.get(url, allow_redirects=True, timeout=15, headers={"User-Agent": UA})
    except requests.RequestException:
        return links

    content_type = (response.headers.get("content-type") or "").lower()
    if "text/html" not in content_type:
        return links

    html = response.text or ""
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
    for href in hrefs:
        href = (href or "").strip()
        if not href:
            continue
        full = urljoin(response.url or url, href)
        if ".pdf" not in full.lower():
            continue
        links.append(full)

    deduped: list[str] = []
    seen: set[str] = set()
    for link in links:
        key = link.split("#", 1)[0]
        if key in seen:
            continue
        seen.add(key)
        deduped.append(link)
    return deduped


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
    for publication_url in PUBLICATION_FEED_URLS:
        urls.append(publication_url)

    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        key = url.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


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
        return unwrap_search_redirect(direct)
    links = entry.get("links") or []
    if isinstance(links, list):
        for link in links:
            if not isinstance(link, dict):
                continue
            href = str(link.get("href") or "").strip()
            if href:
                return unwrap_search_redirect(href)
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
        relevance_haystack = " ".join([haystack, str(item.get("url") or ""), str(item.get("source_url") or "")])
        if not vz_relevant(relevance_haystack):
            continue

        year = infer_year(item)
        if year not in TARGET_YEARS and not mentions_target_years(item, TARGET_YEARS):
            continue

        if not (looks_like_research(haystack) or topic_relevant(haystack)):
            continue

        final_url = resolve_final_url(url)
        if not final_url:
            continue

        if not allowed_domain(final_url):
            continue

        candidate_urls: list[str] = [final_url]
        if not is_pdf_url(final_url):
            candidate_urls.extend(extract_pdf_links_from_page(final_url))

        chosen_pdf_url = ""
        seen_candidates: set[str] = set()
        for candidate in candidate_urls:
            candidate = (candidate or "").strip()
            if not candidate:
                continue
            key = candidate.split("#", 1)[0]
            if key in seen_candidates:
                continue
            seen_candidates.add(key)

            resolved_candidate = resolve_final_url(candidate)
            if not resolved_candidate:
                continue
            if not allowed_domain(resolved_candidate):
                continue

            if is_pdf_url(resolved_candidate):
                chosen_pdf_url = resolved_candidate
                break

            ok, resolved = head_is_pdf(resolved_candidate)
            if ok:
                chosen_pdf_url = resolved
                break

        if not chosen_pdf_url:
            continue

        final_url = chosen_pdf_url

        dedupe_key = final_url.split("#")[0]
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        publisher = norm(item.get("publisher") or domain_of(final_url))
        published_at = item.get("publishedAt") or item.get("dateISO") or ""
        publication_year = year if year in TARGET_YEARS else YEAR_MAX
        if not published_at:
            published_at = str(publication_year)

        publications.append(
            {
                "id": item.get("id") or "",
                "title": title,
                "url": final_url,
                "publisher": publisher,
                "publishedAt": published_at,
                "year": publication_year,
                "sector": item.get("sector") or "",
                "abstract": make_abstract(item),
            }
        )

    publications.sort(key=lambda publication: str(publication.get("publishedAt") or ""), reverse=True)

    year_range_sorted = sorted(TARGET_YEARS)
    output = {
        "asOf": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "yearRange": year_range_sorted,
        "yearLabel": f"{year_range_sorted[0]}-{year_range_sorted[-1]}",
        "count": len(publications),
        "publications": publications[:25],
    }

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
