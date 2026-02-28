"""
collect_rfps.py – Venezuela Update Intelligence collector.

Pipeline:
  1. Load config & feeds
  2. Fetch RSS entries
  3. Filter (country match, age, exclude terms)
  4. Deduplicate
  5. Score & rank
  6. Output docs/index.md + data/last_run.json
"""

import hashlib
import json
import logging
import os
import re
import sys
import csv
from html import escape, unescape
from datetime import datetime, timezone
from difflib import SequenceMatcher
from urllib.parse import parse_qs, unquote, urlparse

import time as _time

import feedparser
import requests
import yaml
from dateutil import parser as dateutil_parser

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(ROOT_DIR, "config.yml")
FEEDS_PATH = os.path.join(ROOT_DIR, "feeds.txt")
DOCS_DIR = os.path.join(ROOT_DIR, "docs")
DOCS_DATA_DIR = os.path.join(DOCS_DIR, "data")
DATA_DIR = os.path.join(ROOT_DIR, "data")
OUTPUT_PATH = os.path.join(DOCS_DIR, "index.md")
METADATA_PATH = os.path.join(DATA_DIR, "last_run.json")


# ---------------------------------------------------------------------------
# Config & feed loading
# ---------------------------------------------------------------------------

def load_config(path: str = CONFIG_PATH) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    return cfg


def load_feeds(path: str = FEEDS_PATH) -> list[str]:
    """Return a list of feed URLs parsed from feeds.txt.

    Lines starting with ``#`` are treated as comments.  A line may be
    formatted either as a plain URL or as ``Label - URL``.
    """
    urls: list[str] = []
    with open(path, "r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            # Support "Label - URL" format
            if " - http" in line:
                url = line.split(" - ", 1)[1].strip()
            else:
                url = line
            if url.startswith("http"):
                urls.append(url)
    return urls


# ---------------------------------------------------------------------------
# Feed fetching
# ---------------------------------------------------------------------------

def fetch_feed(url: str) -> list[dict]:
    """Fetch a single RSS/Atom feed and return a list of normalised entry dicts."""
    try:
        feed = feedparser.parse(url, request_headers={"User-Agent": "VZLAnews/1.0"})
        if feed.bozo and not feed.entries:
            logger.warning("Bozo feed (no entries): %s", url)
            return []
        entries = []
        for e in feed.entries:
            title = e.get("title", "").strip()
            link = e.get("link", "").strip()
            link = _resolve_entry_link(link)
            summary = e.get("summary", "") or e.get("description", "") or ""
            content_items = e.get("content", []) or []
            content_value = ""
            if isinstance(content_items, list) and content_items:
                first_content = content_items[0] or {}
                if isinstance(first_content, dict):
                    content_value = first_content.get("value", "") or ""
            published = _parse_date(e)
            entries.append(
                {
                    "title": title,
                    "link": link,
                    "summary": summary,
                    "content": content_value,
                    "snippet": "",
                    "published": published,
                    "source_url": url,
                    "source_domain": _domain(url),
                }
            )
        return entries
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to fetch %s: %s", url, exc)
        return []


def _resolve_entry_link(link: str) -> str:
    if not link:
        return ""

    try:
        parsed = urlparse(link)
        host = parsed.netloc.lower()
        if "bing.com" in host and "apiclick.aspx" in parsed.path:
            query = parse_qs(parsed.query)
            target = query.get("url", [""])[0]
            if target.startswith("http"):
                return unquote(target)
        return link
    except Exception:  # noqa: BLE001
        return link


def _extract_visible_text(html: str) -> str:
    text = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", html)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def fetch_article_text(url: str, timeout_seconds: int = 6, max_chars: int = 6000) -> str:
    if not url:
        return ""
    try:
        response = requests.get(
            url,
            timeout=timeout_seconds,
            headers={"User-Agent": "VZLAnews/1.0"},
            allow_redirects=True,
        )
        if response.status_code != 200 or not response.text:
            return ""

        text = _extract_visible_text(response.text)
        if not text:
            return ""
        if len(text) > max_chars:
            return text[:max_chars]
        return text
    except requests.RequestException:
        return ""


def _fetch_article_html(url: str, timeout_seconds: int = 6) -> tuple[str, str]:
    if not url:
        return "", ""

    try:
        response = requests.get(
            url,
            timeout=timeout_seconds,
            headers={"User-Agent": "VZLAnews/1.0"},
            allow_redirects=True,
        )
        final_url = (response.url or url).strip()
        if response.status_code != 200 or not response.text:
            return final_url, ""
        return final_url, response.text
    except requests.RequestException:
        return url, ""


def _extract_meta_description(html: str) -> str:
    if not html:
        return ""

    patterns = [
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']twitter:description["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def _extract_first_meaningful_paragraph(html: str) -> str:
    if not html:
        return ""

    paragraphs = re.findall(r"(?is)<p[^>]*>(.*?)</p>", html)
    for paragraph in paragraphs:
        text = _normalize_text_block(paragraph)
        if len(text) < 120:
            continue
        if _sentence_is_noise(text):
            continue
        return text
    return ""


def _is_blocked_extraction_domain(entry: dict, blocked_domains: set[str]) -> bool:
    if not blocked_domains:
        return False

    source_domain = (entry.get("source_domain", "") or "").lower().strip()
    link_domain = _domain(entry.get("link", "") or "").lower().strip()

    for blocked in blocked_domains:
        blocked = blocked.lower().strip()
        if not blocked:
            continue
        if source_domain == blocked or source_domain.endswith(f".{blocked}"):
            return True
        if link_domain == blocked or link_domain.endswith(f".{blocked}"):
            return True
    return False


def enrich_entries_with_article_text(entries: list[dict], cfg: dict) -> None:
    extraction_cfg = cfg.get("article_extraction", {})
    if not extraction_cfg.get("enabled", True):
        return

    max_items = max(0, int(extraction_cfg.get("max_items", 12)))
    timeout_seconds = max(1, int(extraction_cfg.get("timeout_seconds", 6)))
    min_chars = max(100, int(extraction_cfg.get("min_chars", 240)))
    max_chars = max(1000, int(extraction_cfg.get("max_chars", 6000)))
    blocked_domains = {
        str(domain).lower().strip()
        for domain in extraction_cfg.get("blocked_domains", [])
        if str(domain).strip()
    }

    fetched_count = 0
    enriched_count = 0
    skipped_count = 0
    html_cache: dict[str, tuple[str, str]] = {}

    prioritized_entries = sorted(
        entries,
        key=lambda item: 1 if "news.google.com" in (item.get("link", "") or "") else 0,
    )

    for entry in prioritized_entries:
        raw_candidate = entry.get("snippet", "") or entry.get("summary", "") or entry.get("content", "") or ""
        if _is_google_news_boilerplate(raw_candidate):
            entry["snippet_status"] = "boilerplate_removed"
            entry["snippet"] = ""

        if not entry.get("snippet"):
            entry["snippet"] = _entry_feed_snippet(entry, max_chars=280)
        if _is_google_news_boilerplate(entry.get("snippet", "")):
            entry["snippet"] = ""
            entry["snippet_status"] = "boilerplate_removed"

        if fetched_count >= max_items:
            break
        link = entry.get("link", "")
        if not link:
            continue
        if entry.get("snippet"):
            continue
        if _is_blocked_extraction_domain(entry, blocked_domains):
            skipped_count += 1
            continue

        fetched_count += 1

        if link in html_cache:
            resolved_link, html = html_cache[link]
        else:
            resolved_link, html = _fetch_article_html(link, timeout_seconds=timeout_seconds)
            html_cache[link] = (resolved_link, html)

        if resolved_link and resolved_link != link:
            entry["link"] = resolved_link
            entry["source_domain"] = _domain(resolved_link) or entry.get("source_domain", "")

        meta_desc = _extract_meta_description(html)
        cleaned_meta = _clean_snippet(meta_desc, entry.get("title", ""), max_chars=280, min_chars=80)
        if cleaned_meta:
            entry["meta_description"] = cleaned_meta
            entry["snippet"] = cleaned_meta

        if not entry.get("snippet"):
            first_paragraph = _extract_first_meaningful_paragraph(html)
            cleaned_paragraph = _clean_snippet(
                first_paragraph,
                entry.get("title", ""),
                max_chars=280,
                min_chars=80,
            )
            if cleaned_paragraph:
                entry["first_paragraph"] = cleaned_paragraph
                entry["snippet"] = cleaned_paragraph

        article_url = entry.get("link") or resolved_link or link
        article_text = fetch_article_text(article_url, timeout_seconds=timeout_seconds, max_chars=max_chars)
        if len(article_text) >= min_chars:
            entry["article_text"] = article_text
            enriched_count += 1

    logger.info(
        "Article text enrichment: %d/%d entries enriched",
        enriched_count,
        fetched_count,
    )
    if skipped_count:
        logger.info("Article text enrichment: skipped %d blocked-domain entries", skipped_count)


def _parse_date(entry) -> datetime | None:
    """Try to extract a timezone-aware datetime from a feedparser entry."""
    for attr in ("published_parsed", "updated_parsed"):
        val = entry.get(attr)
        if val:
            try:
                ts = _time.mktime(val)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except (OverflowError, OSError):
                pass
    for attr in ("published", "updated"):
        val = entry.get(attr)
        if val:
            try:
                dt = dateutil_parser.parse(val)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except (ValueError, OverflowError):
                pass
    return None


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lstrip("www.")
    except Exception:  # noqa: BLE001
        return ""


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def _text(entry: dict) -> str:
    return (entry.get("title", "") + " " + entry.get("summary", "")).lower()


def passes_age_filter(entry: dict, max_age_days: int, now: datetime) -> bool:
    pub = entry.get("published")
    if pub is None:
        return True  # keep undated entries unless config says otherwise
    age = (now - pub).days
    return age <= max_age_days


def passes_country_filter(entry: dict, country_terms: list[str], geo_terms: list[str]) -> bool:
    text = _text(entry)
    all_terms = [t.lower() for t in country_terms + geo_terms]
    if any(t in text for t in all_terms):
        return True

    source = (entry.get("source_url", "") or "").lower()
    return "venezuela" in source or "venezuelan" in source


def passes_exclude_filter(entry: dict, exclude_terms: list[str]) -> bool:
    text = _text(entry)
    return not any(t.lower() in text for t in exclude_terms)


def filter_entries(
    entries: list[dict],
    cfg: dict,
    now: datetime,
) -> list[dict]:
    max_age = cfg.get("max_age_days", 7)
    sector_max_age = cfg.get("sector_max_age_days", {})
    country_terms = [t.lower() for t in cfg.get("country_terms", [])]
    geo_terms = [t.lower() for t in cfg.get("geo_context_terms", [])]
    exclude_terms = [t.lower() for t in cfg.get("exclude_terms", [])]
    require_country = cfg.get("require_country_match", True)

    filtered = []
    for e in entries:
        entry_max_age = max_age
        section_label = detect_sector_label(e, cfg)
        if section_label in sector_max_age:
            try:
                entry_max_age = int(sector_max_age[section_label])
            except (TypeError, ValueError):
                entry_max_age = max_age

        if not passes_age_filter(e, entry_max_age, now):
            continue
        if not passes_exclude_filter(e, exclude_terms):
            continue
        if require_country and not passes_country_filter(e, country_terms, geo_terms):
            continue
        filtered.append(e)
    return filtered


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _title_key(title: str) -> str:
    return re.sub(r"\W+", " ", title.lower()).strip()


def deduplicate(entries: list[dict], threshold: float = 0.90, cfg: dict | None = None) -> list[dict]:
    """Remove duplicate entries using URL + title-similarity checks."""
    seen_urls: set[str] = set()
    seen_titles: list[str] = []
    unique: list[dict] = []

    for e in entries:
        url = e.get("link", "")
        if url and url in seen_urls:
            continue

        title = _title_key(e.get("title", ""))
        duplicate = False
        for idx, seen in enumerate(seen_titles):
            ratio = SequenceMatcher(None, title, seen).ratio()
            if ratio >= threshold:
                if cfg is not None:
                    existing = unique[idx]
                    new_label = detect_sector_label(e, cfg)
                    existing_label = detect_sector_label(existing, cfg)
                    if new_label != existing_label:
                        continue
                duplicate = True
                break
        if duplicate:
            continue

        if url:
            seen_urls.add(url)
        seen_titles.append(title)
        unique.append(e)

    return unique


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _match_terms(text: str, terms: list[str]) -> int:
    """Count how many terms from the list appear in text."""
    return sum(1 for t in terms if t.lower() in text)


def score_entry(entry: dict, cfg: dict, now: datetime) -> float:
    """Return a normalised score in [0, 1] for a single entry."""
    weights = cfg.get("scoring", {}).get("weights", {})
    w_country = weights.get("country_match", 0.20)
    w_sector = weights.get("sector_relevance", 0.30)
    w_biz = weights.get("business_signals", 0.25)
    w_recency = weights.get("recency", 0.15)
    w_source = weights.get("source_priority", 0.10)
    multi_bonus = cfg.get("scoring", {}).get("multi_sector_bonus", 0.10)

    text = _text(entry)

    # --- Country match ---
    country_terms = [t.lower() for t in cfg.get("country_terms", [])]
    geo_terms = [t.lower() for t in cfg.get("geo_context_terms", [])]
    country_hits = _match_terms(text, country_terms) + _match_terms(text, geo_terms)
    country_score = min(1.0, country_hits / max(1, len(country_terms) * 0.2))

    # --- Sector relevance ---
    sectors = cfg.get("sectors", {})
    sector_scores = []
    for sector_data in sectors.values():
        include_terms = [t.lower() for t in sector_data.get("include", [])]
        hits = _match_terms(text, include_terms)
        sector_scores.append(min(1.0, hits / max(1, len(include_terms) * 0.3)))

    matched_sectors = sum(1 for s in sector_scores if s > 0)
    top_sector_score = max(sector_scores) if sector_scores else 0.0
    sector_score = top_sector_score
    if matched_sectors > 1:
        sector_score = min(1.0, sector_score + multi_bonus * (matched_sectors - 1))

    # --- Business signals ---
    biz_terms = [t.lower() for t in cfg.get("business_signal_terms", [])]
    biz_hits = _match_terms(text, biz_terms)
    biz_score = min(1.0, biz_hits / max(1, len(biz_terms) * 0.1))

    # --- Recency ---
    pub = entry.get("published")
    max_age = cfg.get("max_age_days", 7)
    if pub is None:
        recency_score = 0.5
    else:
        age_days = max(0, (now - pub).total_seconds() / 86400)
        recency_score = max(0.0, 1.0 - (age_days / max(1, max_age)))

    # --- Source priority ---
    source_weights = cfg.get("source_weights", {})
    domain = entry.get("source_domain", "")
    raw_weight = 1.0
    for host, w in source_weights.items():
        if host in domain:
            raw_weight = w
            break
    source_score = min(1.0, (raw_weight - 1.0) / 0.5) if raw_weight > 1.0 else 0.0

    total = (
        w_country * country_score
        + w_sector * sector_score
        + w_biz * biz_score
        + w_recency * recency_score
        + w_source * source_score
    )
    return round(min(1.0, total), 4)


def score_and_rank(entries: list[dict], cfg: dict, now: datetime) -> list[dict]:
    for e in entries:
        e["score"] = score_entry(e, cfg, now)
    return sorted(entries, key=lambda x: x["score"], reverse=True)


def select_diverse_top_entries(entries: list[dict], cfg: dict, max_results: int) -> list[dict]:
    """Select top entries while preserving sector diversity when available."""
    if not entries or max_results <= 0:
        return []

    selection_cfg = cfg.get("selection", {})
    min_per_section = max(0, int(selection_cfg.get("min_per_section", 1)))
    max_per_section = max(1, int(selection_cfg.get("max_per_section", max_results)))
    section_order = cfg.get("brief_sections", [])

    grouped: dict[str, list[dict]] = {section: [] for section in section_order}
    for entry in entries:
        label = detect_sector_label(entry, cfg)
        grouped.setdefault(label, []).append(entry)

    selected: list[dict] = []
    selected_keys: set[str] = set()
    section_counts: dict[str, int] = {section: 0 for section in grouped}

    def _entry_key(item: dict) -> str:
        link = item.get("link", "")
        title = _title_key(item.get("title", ""))
        return f"{link}::{title}"

    def _try_add(item: dict, section: str, enforce_cap: bool = True) -> bool:
        if len(selected) >= max_results:
            return False
        if enforce_cap and section_counts.get(section, 0) >= max_per_section:
            return False
        key = _entry_key(item)
        if key in selected_keys:
            return False
        selected.append(item)
        selected_keys.add(key)
        section_counts[section] = section_counts.get(section, 0) + 1
        return True

    # Pass 1: guarantee a minimum number per configured section if entries exist.
    for section in section_order:
        section_entries = grouped.get(section, [])
        for item in section_entries[:min_per_section]:
            _try_add(item, section)

    # Pass 2: fill remaining slots by global score while respecting section caps.
    for item in entries:
        section = detect_sector_label(item, cfg)
        _try_add(item, section)
        if len(selected) >= max_results:
            break

    # Pass 3: if caps leave spare slots, backfill by score regardless of caps.
    if len(selected) < max_results:
        for item in entries:
            section = detect_sector_label(item, cfg)
            _try_add(item, section, enforce_cap=False)
            if len(selected) >= max_results:
                break

    return selected


# ---------------------------------------------------------------------------
# Flag detection
# ---------------------------------------------------------------------------

def detect_flags(entry: dict, cfg: dict) -> list[str]:
    flags_cfg = cfg.get("flags", {})
    opp_terms = [t.lower() for t in flags_cfg.get("opportunity_flag_terms", [])]
    risk_terms = [t.lower() for t in flags_cfg.get("risk_flag_terms", [])]
    text = _text(entry)
    flags = []
    if any(t in text for t in opp_terms):
        flags.append("🟢 Opportunity")
    if any(t in text for t in risk_terms):
        flags.append("🔴 Risk")
    return flags


def _sector_hint_from_source(entry: dict, cfg: dict) -> str | None:
    """Infer sector from source URL/query hints when content keywords are sparse."""
    source = (entry.get("source_url", "") or "").lower()
    if not source:
        return None

    section_order = cfg.get("brief_sections", [])
    valid_sections = set(section_order)
    hint_map = {
        "Extractives & Mining": ["oil", "gas", "pdvsa", "mining", "orinoco", "energy"],
        "Food & Agriculture": ["agriculture", "food", "fertilizer", "irrigation"],
        "Health & Water": ["health", "hospital", "dengue", "malaria", "water", "sanitation"],
        "Education & Workforce": ["education", "schools", "teachers", "workforce", "vocational", "university", "students", "jobs", "labor"],
        "Finance & Investment": ["banking", "inflation", "exchange", "debt", "bonds", "investment", "fdi"],
    }

    for label, hints in hint_map.items():
        if label not in valid_sections:
            continue
        if any(hint in source for hint in hints):
            return label
    return None


def detect_sector_label(entry: dict, cfg: dict) -> str:
    source_hint_label = _sector_hint_from_source(entry, cfg)
    if source_hint_label:
        return source_hint_label

    sectors = cfg.get("sectors", {})
    text = _text(entry)
    best_label = "Cross-cutting / Policy / Risk"
    best_hits = 0
    for sector_data in sectors.values():
        include_terms = [t.lower() for t in sector_data.get("include", [])]
        hits = _match_terms(text, include_terms)
        if hits > best_hits:
            best_hits = hits
            best_label = sector_data.get("label", best_label)
    return best_label


# ---------------------------------------------------------------------------
# Output generation
# ---------------------------------------------------------------------------

def _fmt_date(dt: datetime | None) -> str:
    if dt is None:
        return "N/A"
    return dt.strftime("%Y-%m-%d")


def _fmt_source(entry: dict) -> str:
    source = (entry.get("source_domain", "") or "").strip()
    if source:
        return source

    link = (entry.get("link", "") or "").strip()
    if link:
        return _domain(link) or "Unknown Source"
    return "Unknown Source"


def _summary_excerpt(entry: dict, max_chars: int) -> str:
    raw = entry.get("summary", "") or ""
    clean = re.sub(r"<[^>]+>", " ", raw)
    clean = unescape(clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    if not clean:
        return ""
    if len(clean) <= max_chars:
        return clean
    clipped = clean[:max_chars].rsplit(" ", 1)[0].strip()
    return (clipped or clean[:max_chars]).strip() + "…"


GOOGLE_NEWS_BOILERPLATE = "Comprehensive up-to-date news coverage, aggregated from sources all over the world by Google News"


def _is_google_news_boilerplate(text: str) -> bool:
    clean = _normalize_text_block(text).strip().lower().rstrip(".")
    return clean == GOOGLE_NEWS_BOILERPLATE.lower().rstrip(".")


def _clean_snippet(
    text: str,
    title: str,
    max_chars: int = 280,
    min_chars: int = 80,
) -> str:
    clean = _normalize_text_block(text)
    if not clean:
        return ""

    if _is_google_news_boilerplate(clean):
        return ""

    clean = re.sub(
        r"\b(Read more|Click here|Continue reading|Learn more|Subscribe to read)\b.*$",
        "",
        clean,
        flags=re.IGNORECASE,
    ).strip(" -–—|·")

    title_clean = _normalize_text_block(title)
    if title_clean and clean.lower().startswith(title_clean.lower()):
        clean = clean[len(title_clean):].lstrip(" :.-–—|,")

    if title_clean and _title_similarity(clean, title_clean) >= 0.92:
        return ""

    if len(clean) < min_chars:
        return ""

    if len(clean) > max_chars:
        clipped = clean[:max_chars].rsplit(" ", 1)[0].strip()
        clean = (clipped or clean[:max_chars]).rstrip(" .") + "…"

    if clean and clean[-1] not in ".!?…":
        clean += "."
    return clean


def _normalize_text_block(text: str) -> str:
    clean = re.sub(r"<[^>]+>", " ", text or "")
    clean = unescape(clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def _sentence_is_noise(sentence: str) -> bool:
    lower = sentence.lower()
    noisy_markers = [
        "subscribe to read",
        "skip to",
        "sign in",
        "open side navigation",
        "close search bar",
        "home world sections",
        "accessibility help",
        "privacy policy",
        "cookies",
        "what’s included",
        "current edition topics",
        "get our news on your inbox",
        "open search bar",
        "close home",
        "videos & podcasts",
        "trial $1",
        "then $",
        "user account menu",
        "wrong login information",
        "forgot password",
        "create an account",
        "news today's news",
        "complete digital access to quality ft journalism",
        "wti crude",
        "brent crude",
        "natural gas",
        "gasoline",
        "click here for",
        "market data",
        "stock quote",
        "live updates",
        "open menu",
        "share this article",
        "advertisement",
    ]
    if any(marker in lower for marker in noisy_markers):
        return True

    # Filter out ticker-like fragments with repeated +/- percentage patterns.
    pct_hits = len(re.findall(r"[+-]?\d+(?:\.\d+)?%", sentence))
    price_hits = len(re.findall(r"\b\d+(?:\.\d+)?\b", sentence))
    if pct_hits >= 2 and price_hits >= 4:
        return True

    # Reject sentences that are mostly symbols/numbers (common in scraped nav/ticker text).
    alnum = re.findall(r"[A-Za-z0-9]", sentence)
    letters = re.findall(r"[A-Za-z]", sentence)
    if alnum:
        letter_ratio = len(letters) / len(alnum)
        if letter_ratio < 0.55:
            return True

    return False


def _sentence_quality_score(sentence: str) -> int:
    score = 0
    words = sentence.split()

    if len(words) >= 10:
        score += 2
    if len(words) >= 16:
        score += 1
    if re.search(r"\b(venezuela|venezuelan|caracas|pdvsa|government|policy|sanctions|investment|oil|gas)\b", sentence, flags=re.IGNORECASE):
        score += 2
    if sentence.endswith((".", "!", "?")):
        score += 1
    if re.search(r"\b(read more|click|subscribe|sign in)\b", sentence, flags=re.IGNORECASE):
        score -= 3

    return score


def _title_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _is_fragment(sentence: str) -> bool:
    words = sentence.strip().split()
    if len(words) < 8:
        return True
    if re.search(r"\b(?:U\.S|UK|Mr|Ms|Dr)\.$", sentence.strip()):
        return True
    if sentence.count("|") >= 2:
        return True
    return False


def _title_topic(entry: dict) -> str:
    title = (entry.get("title", "") or "").strip()
    if not title:
        return "a material policy and market development"

    # Drop trailing source tags like " - Reuters" when present.
    topic = re.sub(r"\s[-–—]\s[^-–—]{2,40}$", "", title).strip()
    topic = topic.rstrip(". ")
    return topic or title


def _entry_feed_snippet(entry: dict, max_chars: int = 280) -> str:
    title = entry.get("title", "") or ""
    snippet = entry.get("snippet", "") or ""
    summary = entry.get("summary", "") or ""
    content = entry.get("content", "") or ""

    if snippet:
        cleaned = _clean_snippet(snippet, title, max_chars=max_chars, min_chars=80)
        if cleaned:
            return cleaned

    for candidate in (summary, content):
        cleaned = _clean_snippet(candidate, title, max_chars=max_chars, min_chars=80)
        if cleaned:
            return cleaned
    return ""


def _fallback_summary(entry: dict, section_label: str, story_index: int, max_chars: int) -> str:
    topic = _title_topic(entry)
    topic_text = topic if topic else "the topic"
    section_text = section_label if section_label else "policy and risk"

    templates = [
        f"This cycle brought a concrete update on {topic_text}.",
        f"Sources indicate implementation movement tied to {topic_text}.",
        f"{section_text} signals in Venezuela shifted in relation to this development.",
        f"New reporting links {topic_text} to near-term operating changes.",
        f"This development remains material for {section_text.lower()} monitoring.",
    ]
    fallback = templates[story_index % len(templates)]
    if len(fallback) > max_chars:
        clipped = fallback[:max_chars].rsplit(" ", 1)[0].strip()
        fallback = (clipped or fallback[:max_chars]).rstrip(".") + "…"
    return fallback


def _descriptive_summary(entry: dict, cfg: dict, max_chars: int) -> str:
    return _descriptive_summary_for_story(
        entry,
        cfg,
        max_chars=max_chars,
        section_label=detect_sector_label(entry, cfg),
        story_index=0,
    )


def _descriptive_summary_for_story(
    entry: dict,
    cfg: dict,
    max_chars: int,
    section_label: str,
    story_index: int,
) -> str:
    title = (entry.get("title", "") or "").strip().rstrip(".")

    feed_snippet = _entry_feed_snippet(entry, max_chars=max_chars)
    if feed_snippet:
        entry["_summary_source"] = "feed_snippet"
        return feed_snippet

    for key in ("meta_description", "first_paragraph"):
        cleaned = _clean_snippet(
            entry.get(key, "") or "",
            title,
            max_chars=max_chars,
            min_chars=80,
        )
        if cleaned:
            entry["_summary_source"] = key
            return cleaned

    base_text = (entry.get("article_text", "") or "").strip()
    clean = _normalize_text_block(base_text)
    candidates = []
    if clean:
        parts = re.split(r"(?<=[.!?])\s+", clean)
        for part in parts:
            p = part.strip(" -\u2022\t\n\r")
            if len(p) >= 35 and not _sentence_is_noise(p) and not _is_fragment(p):
                candidates.append(p)

    ranked_candidates = sorted(
        candidates,
        key=lambda s: (_sentence_quality_score(s), -abs(len(s) - 180)),
        reverse=True,
    )
    for sentence in ranked_candidates:
        if _title_similarity(title, sentence) < 0.88 and not _sentence_is_noise(sentence):
            chosen = _clean_snippet(sentence, title, max_chars=max_chars, min_chars=60)
            if chosen:
                entry["_summary_source"] = "article_text"
                return chosen

    entry["_summary_source"] = "fallback"
    return _fallback_summary(entry, section_label, story_index, max_chars)


def _compact_summary(
    entry: dict,
    cfg: dict,
    max_chars: int = 280,
    section_label: str = "",
    story_index: int = 0,
) -> str:
    text = _descriptive_summary_for_story(
        entry,
        cfg,
        max_chars=max_chars,
        section_label=section_label,
        story_index=story_index,
    )
    sentences = re.split(r"(?<=[.!?])\s+", text)
    compact = " ".join(s.strip() for s in sentences if s.strip()[:1]).strip()
    if not compact:
        return ""

    first_two = []
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        first_two.append(sentence)
        if len(first_two) == 2:
            break
    compact = " ".join(first_two).strip()

    if len(compact) > max_chars:
        clipped = compact[:max_chars].rsplit(" ", 1)[0].strip()
        compact = (clipped or compact[:max_chars]).strip().rstrip(".") + "…"
    return compact


def _published_sort_key(entry: dict) -> tuple[int, datetime, float]:
    published = entry.get("published")
    has_date = 1 if hasattr(published, "strftime") and hasattr(published, "tzinfo") else 0
    dt = published if has_date else datetime.min.replace(tzinfo=timezone.utc)
    score = float(entry.get("score", 0.0) or 0.0)
    return (has_date, dt, score)


def _sort_entries_for_sector(entries: list[dict]) -> list[dict]:
    return sorted(entries, key=_published_sort_key, reverse=True)


def _latest_updates(entries: list[dict], cfg: dict, limit: int = 5) -> list[dict]:
    eligible = [e for e in entries if detect_sector_label(e, cfg)]
    sorted_entries = _sort_entries_for_sector(eligible)
    return sorted_entries[:limit]

def _latest_news_synthesis(entries: list[dict], cfg: dict) -> list[str]:
    section_order = cfg.get("brief_sections", [])
    if not entries:
        return [
            (
                "This cycle produced no qualifying Venezuela-focused items after filtering and relevance controls, "
                "which suggests either a temporary lull in material developments or weak signal quality in current feed output. "
                "From a strategy perspective, maintain baseline monitoring and use the next run to validate whether this is noise or a genuine deceleration in actionable market and policy movement."
            ),
        ]

    sector_counts: dict[str, int] = {}
    for entry in entries:
        label = detect_sector_label(entry, cfg)
        sector_counts[label] = sector_counts.get(label, 0) + 1

    represented = [s for s in section_order if sector_counts.get(s, 0) > 0]
    top_sector_pairs = sorted(sector_counts.items(), key=lambda kv: kv[1], reverse=True)
    top_sector_text = ", ".join([f"{name} ({count})" for name, count in top_sector_pairs[:3]])

    opportunity_count = 0
    risk_count = 0
    domains: dict[str, int] = {}
    dated_entries = []
    for entry in entries:
        flags = detect_flags(entry, cfg)
        if "🟢 Opportunity" in flags:
            opportunity_count += 1
        if "🔴 Risk" in flags:
            risk_count += 1
        domain = entry.get("source_domain", "")
        if domain:
            domains[domain] = domains.get(domain, 0) + 1
        if entry.get("published") is not None:
            dated_entries.append(entry)

    top_domains = sorted(domains.items(), key=lambda kv: kv[1], reverse=True)
    domain_text = ", ".join([d for d, _ in top_domains[:3]]) or "mixed sources"

    # Lightweight theme extraction from high-scoring titles/summaries.
    theme_map = {
        "energy and extractives": ["oil", "gas", "pdvsa", "orinoco", "mining", "gold"],
        "finance and macro policy": ["inflation", "debt", "bonds", "banking", "fx", "exchange", "investment"],
        "public services and health": ["health", "hospital", "water", "sanitation", "outbreak", "dengue", "malaria"],
        "food systems": ["food", "agriculture", "fertilizer", "imports", "food security"],
        "workforce and education": ["education", "students", "schools", "jobs", "labor", "workforce"],
    }
    theme_scores: dict[str, int] = {k: 0 for k in theme_map}
    for entry in entries[:20]:
        text = _text(entry)
        for theme, terms in theme_map.items():
            if any(term in text for term in terms):
                theme_scores[theme] += 1
    top_themes = [name for name, score in sorted(theme_scores.items(), key=lambda kv: kv[1], reverse=True) if score > 0][:3]
    themes_text = ", ".join(top_themes) if top_themes else "cross-sector policy developments"

    if dated_entries:
        latest_date = _fmt_date(max(e["published"] for e in dated_entries))
        earliest_date = _fmt_date(min(e["published"] for e in dated_entries))
        date_span = f"from {earliest_date} to {latest_date}"
    else:
        date_span = "with undated entries"

    represented_text = ", ".join(represented) if represented else "cross-cutting policy coverage"

    return [
        (
            f"This cycle surfaces {len(entries)} ranked Venezuela-linked developments across {len(represented)} sectors ({represented_text}), "
            f"with activity concentrated in {top_sector_text}. "
            f"The near-term strategic storyline is a policy-led reopening dynamic—especially around energy and capital channels—while thematic pressure remains centered on {themes_text}. "
            f"Commercial upside is present but selective ({opportunity_count} opportunity-flagged items), and execution risk remains material ({risk_count} risk-flagged items tied to sanctions, regulatory uncertainty, or operating friction). "
            f"Cross-source coverage with publication timing {date_span} indicates rising momentum, but not yet a structurally de-risked environment. "
            "For strategy teams, the practical posture is scenario-based planning: prioritize partner diligence, compliance-ready operating models, and trigger-based monitoring before committing irreversible capital."
        )
    ]


def _summary_confidence_label(entry: dict) -> str:
    source = (entry.get("_summary_source") or "").strip()
    mapping = {
        "feed_snippet": "High (Feed snippet)",
        "meta_description": "Medium (Meta description)",
        "first_paragraph": "Medium (Page paragraph)",
        "article_text": "Low (Page extraction)",
        "fallback": "Low (Template fallback)",
    }
    return mapping.get(source, "Unknown")


def _source_quality_tier(domain: str) -> str:
    host = (domain or "").lower().strip()
    if not host:
        return "Unknown"

    wire = ("reuters.com", "apnews.com", "bloomberg.com", "ft.com", "wsj.com")
    ngo_multilateral = ("paho.org", "who.int", "reliefweb.int", "worldbank.org", "nrc.no")
    major_media = ("bbc.com", "cnn.com", "nytimes.com", "wsj.com", "ft.com", "nbcnews.com")
    local_regional = ("elpitazo.net", "efectococuyo.com", "el-nacional.com", "talcualdigital.com", "venezuelanalysis.com")

    if any(host == d or host.endswith(f".{d}") for d in wire):
        return "Tier 1"
    if any(host == d or host.endswith(f".{d}") for d in ngo_multilateral):
        return "Tier 1"
    if any(host == d or host.endswith(f".{d}") for d in major_media):
        return "Tier 1"
    if any(host == d or host.endswith(f".{d}") for d in local_regional):
        return "Tier 2"
    return "Tier 3"


def _classify_event_types(entry: dict) -> list[str]:
    text = _text(entry)
    event_map = {
        "Sanctions": ["sanction", "ofac", "license", "embargo", "asset freeze"],
        "Oil production/export": ["oil", "pdvsa", "barrel", "bpd", "export", "refinery", "cargo"],
        "Political transition": ["election", "opposition", "transition", "cabinet", "decree", "maduro"],
        "Regulatory reform": ["regulation", "regulatory", "reform", "law", "framework", "compliance"],
        "Humanitarian crisis": ["hunger", "malnutrition", "crisis", "humanitarian", "displacement"],
        "FX / Inflation": ["inflation", "exchange", "fx", "currency", "devaluation", "bolivar"],
        "Debt restructuring": ["debt", "bond", "restructuring", "creditor", "sovereign spread"],
        "Security": ["security", "military", "protest", "violence", "guerrilla", "conflict"],
    }
    events = [event for event, terms in event_map.items() if any(term in text for term in terms)]
    return events[:3]


def _detect_entities(entry: dict) -> list[str]:
    text = _text(entry)
    entities = [
        "PDVSA",
        "Maduro",
        "OFAC",
        "IMF",
        "Chevron",
        "Bond restructuring",
        "FX controls",
    ]
    checks = {
        "PDVSA": ["pdvsa"],
        "Maduro": ["maduro"],
        "OFAC": ["ofac", "license"],
        "IMF": ["imf", "international monetary fund"],
        "Chevron": ["chevron"],
        "Bond restructuring": ["bond", "restructuring", "creditor"],
        "FX controls": ["fx", "exchange", "currency control", "devaluation"],
    }
    out = [name for name in entities if any(token in text for token in checks[name])]
    return out[:4]


def _sentiment_label(entry: dict) -> str:
    text = _text(entry)
    positive_terms = ["agreement", "easing", "recovery", "growth", "approval", "restart", "deal"]
    negative_terms = ["sanction", "crisis", "decline", "shortage", "default", "conflict", "risk", "protest"]
    pos = sum(1 for token in positive_terms if token in text)
    neg = sum(1 for token in negative_terms if token in text)
    if neg >= pos + 1:
        return "Negative"
    if pos >= neg + 1:
        return "Positive"
    return "Neutral"


def _materiality_score(entry: dict) -> int:
    text = _text(entry)
    score = 1
    high_impact = ["sanction", "oil", "pdvsa", "debt", "inflation", "export", "license", "regulation"]
    medium_impact = ["policy", "investment", "currency", "humanitarian", "security"]
    score += min(2, sum(1 for token in high_impact if token in text))
    score += min(1, sum(1 for token in medium_impact if token in text))
    if _source_quality_tier(entry.get("source_domain", "")) == "Tier 1":
        score += 1
    return max(1, min(5, score))


def _risk_score(entry: dict) -> int:
    sentiment = _sentiment_label(entry)
    materiality = _materiality_score(entry)
    events = _classify_event_types(entry)
    base = materiality * 15
    if sentiment == "Negative":
        base += 20
    elif sentiment == "Positive":
        base -= 10
    if "Sanctions" in events:
        base += 15
    if "Security" in events:
        base += 10
    if "Oil production/export" in events and sentiment == "Positive":
        base -= 8
    return max(0, min(100, base))


def _annotate_intelligence(entry: dict) -> None:
    entry["event_types"] = _classify_event_types(entry)
    entry["sentiment"] = _sentiment_label(entry)
    entry["materiality"] = _materiality_score(entry)
    entry["risk_score"] = _risk_score(entry)
    entry["entities"] = _detect_entities(entry)


def _sparkline(values: list[float]) -> str:
    if not values:
        return ""
    bars = "▁▂▃▄▅▆▇█"
    low = min(values)
    high = max(values)
    if high == low:
        return bars[3] * len(values)
    out = []
    for value in values:
        idx = int(round((value - low) / (high - low) * (len(bars) - 1)))
        out.append(bars[max(0, min(len(bars) - 1, idx))])
    return "".join(out)


def _window_stats(history: list[dict], key: str, days: int) -> float:
    if not history:
        return 0.0
    recent = history[-days:]
    if not recent:
        return 0.0
    return sum(float(item.get(key, 0) or 0) for item in recent) / len(recent)


def _trend_direction(current: float, prior: float, threshold: float = 0.08) -> str:
    if prior <= 0:
        return "→ Stable"
    delta = (current - prior) / prior
    if delta > threshold:
        return "↑ Risk Increasing"
    if delta < -threshold:
        return "↓ Risk Decreasing"
    return "→ Stable"


def _build_sector_brief(section: str, rows: list[dict]) -> dict:
    if not rows:
        return {
            "health": 5,
            "summary": "Coverage is limited this cycle; monitor for confirmation in the next run.",
            "risks": ["Signal density is low", "Data confidence is constrained", "Cross-check with primary sources"],
            "opportunities": ["Watch for early policy movement", "Track counterpart statements", "Review exposure scenarios"],
            "watch": ["Next weekly cycle", "Regulatory updates", "Source confirmation"],
        }

    avg_risk = sum(int(r.get("risk_score", 50)) for r in rows) / len(rows)
    health = max(1, min(10, round((100 - avg_risk) / 10)))
    positives = [r for r in rows if r.get("sentiment") == "Positive"]
    negatives = [r for r in rows if r.get("sentiment") == "Negative"]
    top_events = []
    for row in rows:
        top_events.extend(row.get("event_types", []))
    top_events_text = ", ".join(sorted(set(top_events))[:3]) or "cross-cutting policy signals"

    summary = (
        f"This week in {section.lower()}, signal flow is concentrated around {top_events_text}. "
        f"Risk pressure is {'elevated' if avg_risk >= 60 else 'mixed'} with an average score of {avg_risk:.0f}/100. "
        f"Positive momentum appears in {len(positives)} items, while {len(negatives)} items indicate material downside pressure. "
        "Decision context should balance regulatory trajectory, operational feasibility, and partner exposure. "
        "Near-term positioning favors scenario planning with trigger-based execution gates."
    )

    risks = [
        f"{row.get('title','')[:95]}" for row in sorted(rows, key=lambda r: int(r.get("risk_score", 0)), reverse=True)[:3]
    ]
    opportunities = [
        f"{row.get('title','')[:95]}" for row in [r for r in rows if r.get("sentiment") != "Negative"][:3]
    ]
    while len(opportunities) < 3:
        opportunities.append("Monitor policy/market openings tied to licensing, contracts, or donor leverage.")
    watch = [
        "Executive decrees and regulatory circulars",
        "Sanctions/license language shifts",
        "Operational updates from Tier 1 and Tier 2 sources",
    ]
    return {
        "health": health,
        "summary": summary,
        "risks": risks,
        "opportunities": opportunities[:3],
        "watch": watch,
    }


def _calculate_sanctions_index(rows: list[dict], history: list[dict]) -> int:
    sanctions_rows = [row for row in rows if "Sanctions" in row.get("event_types", [])]
    if not sanctions_rows:
        baseline = _window_stats(history, "sanctions_count", 7)
        return int(max(0, min(100, baseline * 8)))

    negative = sum(1 for row in sanctions_rows if row.get("sentiment") == "Negative")
    avg_materiality = sum(int(row.get("materiality", 1)) for row in sanctions_rows) / len(sanctions_rows)
    escalation_terms = ("rollback", "tighten", "enforcement", "penalty", "blacklist")
    escalation_hits = sum(
        1
        for row in sanctions_rows
        if any(term in (row.get("summary", "") or "").lower() for term in escalation_terms)
    )
    index = len(sanctions_rows) * 8 + negative * 7 + int(avg_materiality * 6) + escalation_hits * 5
    return int(max(0, min(100, index)))


def _research_tags(entry: dict, section_label: str) -> list[str]:
    text = _text(entry)
    tag_terms = {
        "Sanctions": ["sanction", "embargo", "license"],
        "Energy": ["oil", "gas", "pdvsa", "refinery"],
        "Elections": ["election", "vote", "ballot", "campaign"],
        "Migration": ["migration", "migrant", "displacement", "refugee"],
        "Health": ["health", "hospital", "outbreak", "dengue", "malaria"],
        "Debt/FX": ["debt", "bond", "inflation", "exchange", "fx"],
        "Infrastructure": ["infrastructure", "port", "pipeline", "transport"],
    }
    tags = [tag for tag, terms in tag_terms.items() if any(term in text for term in terms)]
    if section_label == "Cross-cutting / Policy / Risk" and "Policy" not in tags:
        tags.append("Policy")
    return tags[:4]


def _why_this_matters(entry: dict, cfg: dict, section_label: str) -> str:
    flags = detect_flags(entry, cfg)
    if "🔴 Risk" in flags:
        return "Why this matters: this item may affect risk assumptions, compliance posture, or operational continuity planning."
    if "🟢 Opportunity" in flags:
        return "Why this matters: this item may signal near-term openings for partnerships, contracting, or market entry decisions."

    section_map = {
        "Extractives & Mining": "Why this matters: it informs outlooks for production, export channels, and energy-related policy direction.",
        "Food & Agriculture": "Why this matters: it helps assess food-system resilience, input constraints, and trade-linked supply risks.",
        "Health & Water": "Why this matters: it can shift humanitarian priorities and infrastructure reliability assumptions.",
        "Education & Workforce": "Why this matters: it indicates labor availability, social stability, and long-term human-capital trends.",
        "Finance & Investment": "Why this matters: it influences macro risk, financing conditions, and investor confidence signals.",
        "Cross-cutting / Policy / Risk": "Why this matters: it can alter regulatory scenarios, stakeholder positions, and execution risk.",
    }
    return section_map.get(
        section_label,
        "Why this matters: it provides context for policy, risk, and execution planning.",
    )


def _entry_id(entry: dict) -> str:
    link = (entry.get("link") or "").strip()
    if link:
        return f"url::{link}"
    title = _title_key(entry.get("title", ""))
    return f"title::{title}"


def _serialize_entry(entry: dict, section_label: str) -> dict:
    tags = _research_tags(entry, section_label)
    return {
        "id": _entry_id(entry),
        "title": entry.get("title", ""),
        "url": entry.get("link", ""),
        "source": _fmt_source(entry),
        "source_quality": _source_quality_tier(entry.get("source_domain", "")),
        "published": _fmt_date(entry.get("published")),
        "retrieved": "",
        "sector": section_label,
        "summary": entry.get("_summary_text", ""),
        "summary_confidence": _summary_confidence_label(entry),
        "event_types": entry.get("event_types", []),
        "sentiment": entry.get("sentiment", "Neutral"),
        "materiality": int(entry.get("materiality", 1) or 1),
        "risk_score": int(entry.get("risk_score", 0) or 0),
        "entities": entry.get("entities", []),
        "tags": tags,
    }


def _stable_item_id(url: str, title: str, published_iso: str) -> str:
    raw = f"{url}|{title}|{published_iso}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _deterministic_pick(seed: str, options: list[str]) -> str:
    if not options:
        return ""
    h = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    idx = int(h[:8], 16) % len(options)
    return options[idx]


def _extract_numbers(text: str) -> list[dict]:
    clean = _normalize_text_block(text)
    if not clean:
        return []
    words = clean.split()
    if not words:
        return []

    spans = []
    cursor = 0
    for word in words:
        start = clean.find(word, cursor)
        end = start + len(word)
        spans.append((start, end))
        cursor = end

    def _word_index(char_pos: int) -> int:
        for idx, (start, end) in enumerate(spans):
            if start <= char_pos < end:
                return idx
        return max(0, min(len(spans) - 1, len(spans) // 2))

    pattern = re.compile(
        r"(?<!\w)(?:US\$|\$)?\d{1,3}(?:[\.,]\d{3})*(?:[\.,]\d+)?(?:%|\s?(?:bpd|barrels(?:/day)?|million|billion|bn|m|days?|months?|years?))?(?!\w)",
        flags=re.IGNORECASE,
    )
    priority_terms = (
        "sanction", "treasury", "oil", "pdvsa", "release", "prison", "amnesty", "election",
        "health", "outbreak", "inflation", "fx", "debt", "revenue", "license", "arrest",
    )

    candidates = []
    seen = set()
    for match in pattern.finditer(clean):
        value = match.group(0).strip()
        if not value:
            continue
        if re.fullmatch(r"\d{4}", value) and value.startswith("20"):
            continue
        if re.fullmatch(r"\d{1,2}", value) and int(value) < 5 and "%" not in value and "$" not in value:
            continue
        word_idx = _word_index(match.start())
        left = max(0, word_idx - 12)
        right = min(len(words), word_idx + 13)
        context = " ".join(words[left:right]).strip()
        if len(context) < 20:
            continue
        context_lower = context.lower()
        if re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b", context_lower):
            if "%" not in value and "$" not in value and not re.search(r"bpd|barrel|million|billion|bn|m", value, flags=re.IGNORECASE):
                continue
        if re.fullmatch(r"\d{1,2}", value) and re.search(r"\b(week|month|year|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", context_lower):
            continue
        score = 0
        if any(term in context_lower for term in priority_terms):
            score += 3
        if "%" in value or "$" in value or "bpd" in value.lower() or "barrel" in value.lower():
            score += 2
        if re.search(r"\b(today|homepage|menu|copyright|privacy)\b", context_lower):
            score -= 3

        label = "Policy or market figure"
        if any(term in context_lower for term in ("release", "prison", "amnesty", "arrest")):
            label = "Detentions and releases"
        elif any(term in context_lower for term in ("sanction", "treasury", "license", "revenue")):
            label = "Sanctions and state revenue"
        elif any(term in context_lower for term in ("oil", "pdvsa", "barrel", "bpd")):
            label = "Oil and production"
        elif any(term in context_lower for term in ("health", "outbreak", "dengue", "hospital")):
            label = "Health signal"
        elif any(term in context_lower for term in ("inflation", "fx", "debt", "bond")):
            label = "Macro and finance"

        dedupe_key = f"{value.lower()}|{context_lower[:100]}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        candidates.append({"label": label, "value": value, "context": clamp_text_py(context, 120), "score": score})

    candidates.sort(key=lambda item: (int(item.get("score", 0)), len(str(item.get("context", "")))), reverse=True)
    return candidates[:8]


def _derive_publisher(entry: dict) -> str:
    title = (entry.get("title", "") or "").strip()
    title_match = re.search(r"\s[-–—]\s([^\-–—]{2,50})$", title)
    if title_match:
        candidate = title_match.group(1).strip()
        if candidate and len(candidate.split()) <= 6:
            return candidate
    return _fmt_source(entry)


def _icons_for_item(item: dict) -> list[str]:
    icons = []
    tags = set(item.get("tags", []))
    events = set(item.get("event_types", []))
    if item.get("flags", {}).get("risk"):
        icons.append("RISK")
    if item.get("flags", {}).get("opportunity"):
        icons.append("OPPORTUNITY")
    if "Sanctions" in events:
        icons.extend(["RISK", "POLICY"])
    if item.get("sector") == "Extractives & Mining":
        icons.append("ENERGY")
    if "FX / Inflation" in events or "Debt/FX" in tags:
        icons.append("FX")
    if "Infrastructure" in tags:
        icons.append("TRADE")
    if "Health" in tags or "Humanitarian crisis" in events:
        icons.append("HUMAN")
    if item.get("flags", {}).get("new"):
        icons.append("NEW")
    try:
        pub = dateutil_parser.parse(item.get("publishedAt", ""))
        if (datetime.now(timezone.utc) - pub).total_seconds() <= 172800:
            icons.append("NEW")
    except Exception:  # noqa: BLE001
        pass
    deduped = []
    for icon in icons:
        if icon not in deduped:
            deduped.append(icon)
    return deduped


def _generate_insight2(item: dict, source_text: str = "") -> dict:
    seed = item["id"]
    snippet = _normalize_text_block(item.get("snippet", ""))
    source_text = _normalize_text_block(source_text)
    facts = item.get("metrics", {}).get("numbers", [])
    first_fact = facts[0]["value"] if facts else ""
    sector = item.get("sector", "this sector")

    candidate_text = source_text or snippet
    sentence_parts = [
        s.strip()
        for s in re.split(r"(?<=[.!?])\s+", candidate_text)
        if s.strip() and len(s.strip()) >= 35 and not _sentence_is_noise(s.strip())
    ]

    title_norm = _normalize_text_block(item.get("title", ""))
    filtered = [
        s for s in sentence_parts
        if _title_similarity(title_norm, s) < 0.90
        and not re.search(r"\b(this article discusses|the report highlights|according to reports)\b", s, flags=re.IGNORECASE)
    ]

    if filtered:
        s1 = clamp_text_py(filtered[0], 180)
    else:
        event_hint = _deterministic_pick(seed + "-evt", item.get("event_types", []) or [sector])
        s1 = f"Officials and counterpart institutions advanced {event_hint.lower()} actions in Venezuela"
        if first_fact:
            s1 += f", with figures such as {first_fact} reported"
        s1 += "."

    if len(filtered) > 1:
        s2 = clamp_text_py(filtered[1], 180)
    else:
        mechanism = "execution constraints and financing channels"
        if item.get("flags", {}).get("risk"):
            mechanism = "compliance requirements and delivery timelines"
        elif item.get("flags", {}).get("opportunity"):
            mechanism = "implementation windows and partner sequencing"
        s2 = f"This matters because it changes {mechanism} for {sector.lower()} operations over the next reporting cycle."

    for banned in ("this article discusses", "the report highlights", "according to reports"):
        s1 = re.sub(re.escape(banned), "", s1, flags=re.IGNORECASE).strip()
        s2 = re.sub(re.escape(banned), "", s2, flags=re.IGNORECASE).strip()

    headline_norm = title_norm.lower()
    if headline_norm and _title_similarity(headline_norm, s1.lower()) > 0.88:
        event_hint = _deterministic_pick(seed + "-evt2", item.get("event_types", []) or [sector])
        s1 = f"Authorities and counterpart actors advanced {event_hint.lower()} actions in Venezuela."

    confidence = "HIGH" if item.get("summary_confidence", "").startswith("High") else "MED"
    if item.get("summary_confidence", "").startswith("Low"):
        confidence = "LOW"

    evidence = []
    if snippet:
        evidence.append(clamp_text_py(snippet, 120))
    for fact in facts[:2]:
        context = fact.get("context", "")
        if context:
            evidence.append(clamp_text_py(context, 120))
    evidence = evidence[:3]

    return {"s1": clamp_text_py(s1, 180), "s2": clamp_text_py(s2, 180), "confidence": confidence, "evidence": evidence}


def clamp_text_py(text: str, limit: int) -> str:
    clean = _normalize_text_block(text)
    if len(clean) <= limit:
        return clean
    return clean[:limit].rsplit(" ", 1)[0].strip() + "…"


def _strip_summary_leadin(text: str) -> str:
    clean = _normalize_text_block(text)
    patterns = [
        r"^This cycle brought a concrete update on\s+",
        r"^Sources indicate implementation movement tied to\s+",
        r"^New reporting links\s+",
        r"^Reporting points to concrete movement linked to\s+",
        r"^Developments around\s+",
    ]
    for pattern in patterns:
        clean = re.sub(pattern, "", clean, flags=re.IGNORECASE)
    return clean.strip().rstrip(".")


def _sentence_case_start(text: str) -> str:
    clean = _normalize_text_block(text)
    if not clean:
        return ""
    if len(clean) == 1:
        return clean.upper()
    return clean[0].upper() + clean[1:]


def _build_sector_synth(section: str, items: list[dict]) -> dict:
    if not items:
        return {
            "bullets": [
                "Coverage was limited in this sector during the current cycle.",
                "No concrete multi-source development met the threshold for a stronger synthesis bullet.",
                "Watch the next run for confirmed operational or policy updates tied to this sector.",
            ],
            "drivers": [],
            "watch": ["Regulatory updates", "Partner statements", "Execution constraints"],
        }

    events = []
    for item in items:
        events.extend(item.get("event_types", []))
    drivers = sorted(set(events))[:4]
    top_items = sorted(
        items,
        key=lambda i: (int(i.get("materiality", 1) or 1), int(i.get("risk_score", 0) or 0)),
        reverse=True,
    )[:3]

    bullet_openers = [
        "Officials reported",
        "Field updates show",
        "Separate sources indicate",
    ]
    bullets = []
    for idx, item in enumerate(top_items):
        base = item.get("insight2", {}).get("s1", "") or item.get("title", "")
        base = _strip_summary_leadin(base)
        if not base:
            continue
        base = _sentence_case_start(base.rstrip(". "))
        if _title_similarity(base, item.get("title", "")) > 0.90:
            base = f"{_title_topic({ 'title': item.get('title', '') })} advanced as a concrete sector development"
        bullets.append(clamp_text_py(f"{bullet_openers[idx % len(bullet_openers)]} {base}.", 180))

    while len(bullets) < 3:
        bullets.append("Additional concrete developments were limited in this cycle; watch next updates for confirmed changes.")

    return {
        "bullets": bullets[:3],
        "drivers": drivers,
        "watch": ["Regulatory deadlines", "Executive decrees", "Sanctions and licensing language"],
    }


def _build_highlights(items: list[dict], sector_synth: dict[str, dict]) -> dict:
    ranked = sorted(items, key=lambda i: (int(i.get("materiality", 1)), int(i.get("risk_score", 0))), reverse=True)
    banned_phrases = (
        "coverage points to",
        "reporting clustered around",
        "recent reporting",
    )

    executive_bullets: list[str] = []
    opener_patterns = [
        "Government and institutional moves show",
        "Operational evidence across sources indicates",
        "Multiple reports converge on",
        "The current cycle confirms",
        "Cross-sector tracking now shows",
    ]
    for idx in range(min(5, len(ranked))):
        primary = ranked[idx]
        secondary = ranked[(idx + 1) % len(ranked)] if len(ranked) > 1 else ranked[idx]
        p_text = _sentence_case_start(_strip_summary_leadin(clamp_text_py(primary.get("insight2", {}).get("s1", primary.get("title", "")), 120)))
        p_actor = primary.get("publisher") or primary.get("sector", "primary source")
        s_actor = secondary.get("publisher") or secondary.get("sector", "secondary source")
        first_num = ""
        if primary.get("metrics", {}).get("numbers"):
            first_num = str(primary["metrics"]["numbers"][0].get("value", "")).strip()
        detail_suffix = f" with figures including {first_num}" if first_num else ""
        line = f"{opener_patterns[idx]} {p_actor} and {s_actor} converging on {p_text}{detail_suffix}."
        line = _normalize_text_block(line)
        if any(phrase in line.lower() for phrase in banned_phrases):
            continue
        executive_bullets.append(clamp_text_py(line, 220))
    while len(executive_bullets) < 5:
        executive_bullets.append("Cross-source corroboration remained limited for one track this cycle, so monitoring continues for confirmation in the next run.")

    key_developments = []
    for item in ranked[:8]:
        supporting = [item.get("id")]
        for peer in ranked:
            if peer.get("id") == item.get("id"):
                continue
            if set(peer.get("event_types", [])) & set(item.get("event_types", [])) or peer.get("sector") == item.get("sector"):
                supporting.append(peer.get("id"))
            if len(supporting) == 3:
                break
        sentence = _sentence_case_start(_strip_summary_leadin(clamp_text_py(item.get("insight2", {}).get("s1", item.get("title", "")), 180)))
        if sentence and sentence[-1] not in ".!?…":
            sentence += "."
        if len(sentence.split()) < 7:
            topic = _title_topic({"title": item.get("title", "")})
            sentence = clamp_text_py(f"{topic} became a material development in this cycle.", 180)
        if any(phrase in sentence.lower() for phrase in banned_phrases):
            continue
        key_developments.append({"text": sentence, "itemIds": [sid for sid in supporting if sid][:3]})
        if len(key_developments) == 5:
            break
    while len(key_developments) < 5:
        key_developments.append({"text": "Monitoring continues for concrete policy and operational developments.", "itemIds": []})

    scored_numbers = []
    label_priority = {
        "Sanctions and state revenue": 6,
        "Oil and production": 5,
        "Detentions and releases": 5,
        "Macro and finance": 4,
        "Health signal": 3,
        "Policy or market figure": 2,
    }
    for item in ranked:
        for metric in item.get("metrics", {}).get("numbers", []):
            context = str(metric.get("context", ""))
            value = str(metric.get("value", "")).strip()
            if not value or len(context) < 20:
                continue
            if re.fullmatch(r"\d{4}", value) and value.startswith("20"):
                continue
            if re.fullmatch(r"\d{1,2}", value) and int(value) < 5 and "%" not in value and "$" not in value:
                continue
            label = str(metric.get("label", "Policy or market figure"))
            scored_numbers.append(
                {
                    "label": label,
                    "value": value,
                    "context": clamp_text_py(context, 90),
                    "itemId": item.get("id"),
                    "sector": str(item.get("sector", "")),
                    "score": int(metric.get("score", 0)) + int(item.get("materiality", 1)) + int(label_priority.get(label, 0)),
                }
            )
    scored_numbers.sort(key=lambda row: (int(row.get("score", 0)), len(str(row.get("context", "")))), reverse=True)
    by_numbers = []
    seen_num = set()
    seen_label_value = set()
    label_counts: dict[str, int] = {}
    sector_counts: dict[str, int] = {}
    for metric in scored_numbers:
        label = str(metric.get("label", "Policy or market figure"))
        value_raw = str(metric.get("value", ""))
        value_norm = re.sub(r"\s+", "", value_raw.lower()).rstrip(".")
        key = f"{label}|{value_norm}|{metric['context'][:40]}"
        label_value_key = f"{label.lower()}|{value_norm}"
        if key in seen_num:
            continue
        if label_value_key in seen_label_value:
            continue
        sector = str(metric.get("sector", ""))
        if label_counts.get(label, 0) >= 2:
            continue
        if sector and sector_counts.get(sector, 0) >= 2:
            continue
        seen_num.add(key)
        seen_label_value.add(label_value_key)
        label_counts[label] = label_counts.get(label, 0) + 1
        if sector:
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
        by_numbers.append({
            "label": metric["label"],
            "value": metric["value"],
            "context": metric["context"],
            "itemId": metric["itemId"],
        })
        if len(by_numbers) == 5:
            break
    if len(by_numbers) < 5:
        sanctions_count = sum(1 for item in items if "Sanctions" in (item.get("event_types", []) or []))
        oil_count = sum(1 for item in items if "Oil production/export" in (item.get("event_types", []) or []))
        risk_count = sum(1 for item in items if item.get("flags", {}).get("risk"))
        fallback_facts = [
            {"label": "Items tracked this cycle", "value": str(len(items)), "context": "High-materiality items selected from source reporting", "itemId": ""},
            {"label": "Sanctions-linked developments", "value": str(sanctions_count), "context": "Items tagged with sanctions-related event signals", "itemId": ""},
            {"label": "Oil-linked developments", "value": str(oil_count), "context": "Items tagged with oil production or export signals", "itemId": ""},
            {"label": "Risk-flagged developments", "value": str(risk_count), "context": "Items carrying explicit risk flags in this cycle", "itemId": ""},
        ]
        for fact in fallback_facts:
            fact_key = f"{fact['label']}|{fact['value']}|{fact['context']}"
            if fact_key in seen_num:
                continue
            by_numbers.append(fact)
            seen_num.add(fact_key)
            if len(by_numbers) == 5:
                break

    while len(by_numbers) < 5:
        by_numbers.append({"label": "Quantitative signal", "value": "N/A", "context": "Limited high-confidence numeric context in current sources", "itemId": ""})

    return {
        "executiveBriefBullets": executive_bullets[:5],
        "keyDevelopments": key_developments[:5],
        "byTheNumbers": by_numbers[:5],
        "sectorSynth": sector_synth,
    }


def _build_docs_shell(run_at: str) -> str:
    return "\n".join([
        "# VZLAnews Intelligence Platform",
        "",
        f"> Updated: **{run_at}**",
        "",
        '<link rel="stylesheet" href="{{ \'/assets/styles.css\' | relative_url }}">',
        "",
        '<div id="app-root" class="vzla-app">Loading intelligence dashboard…</div>',
        "",
        '<script src="{{ \'/assets/nlg.js\' | relative_url }}"></script>',
        '<script src="{{ \'/assets/app.js\' | relative_url }}"></script>',
        "",
    ])


def _default_app_js() -> str:
        app_js_path = os.path.join(DOCS_DIR, "assets", "app.js")
        if os.path.exists(app_js_path):
                with open(app_js_path, "r", encoding="utf-8") as fh:
                        return fh.read()
        return "(function () {})();\n"


def _default_styles_css() -> str:
        styles_css_path = os.path.join(DOCS_DIR, "assets", "styles.css")
        if os.path.exists(styles_css_path):
                with open(styles_css_path, "r", encoding="utf-8") as fh:
                        return fh.read()
        return ".error { color: #b91c1c; }\n"


def _write_if_changed(path: str, content: str) -> bool:
        existing = ""
        if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as fh:
                        existing = fh.read()
        if existing == content:
                return False
        with open(path, "w", encoding="utf-8") as fh:
                fh.write(content)
        return True


def build_markdown(entries: list[dict], cfg: dict, run_meta: dict) -> str:
    country_name = cfg.get("country", {}).get("name", "Venezuela")
    summary_max_chars = min(280, int(cfg.get("summary_max_chars", 280)))
    run_at = (run_meta.get("run_at", "") or "").replace("T", " ").replace("+00:00", " UTC")
    diff_new = int(run_meta.get("diff_new", 0) or 0)
    diff_updated = int(run_meta.get("diff_updated", 0) or 0)
    diff_dropped = int(run_meta.get("diff_dropped", 0) or 0)
    executive_brief = _latest_news_synthesis(entries, cfg)[0]
    trend_summary = run_meta.get("trend_summary", {}) or {}
    sanctions_index = int(run_meta.get("sanctions_index", 0) or 0)
    macro_indicators = run_meta.get("macro_indicators", []) or []
    sector_briefs = run_meta.get("sector_briefs", {}) or {}
    timeline_rows = run_meta.get("timeline_rows", []) or []
    section_descriptions = {
        "Extractives & Mining": "Oil, gas, mining activity, concessions, production shifts, and energy security developments.",
        "Food & Agriculture": "Food supply, agricultural output, imports, and nutrition-related policy and market developments.",
        "Health & Water": "Public health, hospitals, outbreaks, water access, sanitation, and infrastructure reliability updates.",
        "Education & Workforce": "Schools, labor conditions, student activity, workforce policy, and human capital developments.",
        "Finance & Investment": "Inflation, exchange rates, debt, sanctions, investment flows, and financial policy signals.",
        "Cross-cutting / Policy / Risk": "Governance, diplomacy, legal shifts, sanctions context, and systemic risk signals.",
    }

    lines = [
        f"# {country_name} News Intelligence",
        "",
        f"> Last refreshed: **{run_at or 'N/A'}**",
        "",
        "<style>",
        ".vzla-page { max-width: 920px; margin: 0 auto; }",
        ".vzla-section { margin-top: 32px; }",
        ".vzla-controls { display: grid; gap: 10px; margin: 14px 0 6px; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); }",
        ".vzla-controls input, .vzla-controls select { width: 100%; padding: 8px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 14px; }",
        ".vzla-toolbar { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px; }",
        ".vzla-toolbar a { border: 1px solid #d1d5db; border-radius: 6px; padding: 6px 10px; font-size: 13px; text-decoration: none; }",
        ".jump-links { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }",
        ".jump-links a { font-size: 13px; color: #1f2937; text-decoration: none; border: 1px solid #d1d5db; border-radius: 999px; padding: 4px 10px; }",
        ".exec-brief { border: 1px solid #e5e7eb; background: #f9fafb; border-radius: 8px; padding: 14px; font-size: 15px; line-height: 1.6; }",
        ".diff-note { margin-top: 10px; color: #374151; font-size: 14px; }",
        ".intel-grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); margin-top: 14px; }",
        ".intel-card { border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; background: #fff; }",
        ".intel-title { font-size: 13px; color: #6b7280; margin: 0 0 6px; }",
        ".intel-value { font-size: 22px; font-weight: 700; margin: 0; }",
        ".risk-green { color: #166534; }",
        ".risk-yellow { color: #92400e; }",
        ".risk-red { color: #991b1b; }",
        ".sector-brief-grid { display: grid; gap: 14px; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); }",
        ".sector-brief { border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; }",
        ".sector-brief h4 { margin: 0 0 8px; }",
        ".sector-brief ul { margin: 6px 0 0 18px; }",
        ".timeline-table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; }",
        ".timeline-table th, .timeline-table td { border-bottom: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; }",
        ".latest-updates-list { list-style: none; margin: 0; padding: 0; }",
        ".latest-updates-item { padding: 10px 0; border-bottom: 1px solid #e5e7eb; }",
        ".latest-updates-item:last-child { border-bottom: none; }",
        ".latest-updates-meta { margin-top: 4px; color: #6b7280; font-size: 13px; line-height: 1.4; }",
        ".sector-title { margin-bottom: 6px; }",
        ".sector-description { margin: 0 0 20px; color: #4b5563; font-size: 16px; line-height: 1.5; }",
        ".sector-cards { display: block; }",
        ".story-card { border: 1px solid #e5e7eb; box-shadow: 0 1px 3px rgba(15, 23, 42, 0.08); border-radius: 8px; padding: 18px; margin-bottom: 22px; }",
        ".story-card.hidden { display: none; }",
        ".story-card:last-child { margin-bottom: 0; }",
        ".story-title { margin: 0 0 10px; font-size: 18px; line-height: 1.35; font-weight: 700; }",
        ".story-card.featured .story-title { font-size: 20px; }",
        ".story-summary { margin: 0 0 10px; font-size: 16px; line-height: 1.55; color: #111827; }",
        ".story-one-liner { margin: 0 0 6px; color: #4b5563; font-size: 13px; line-height: 1.45; }",
        ".story-meta { margin: 0; color: #6b7280; font-size: 13px; line-height: 1.4; }",
        ".story-tags { margin: 8px 0 0; display: flex; flex-wrap: wrap; gap: 6px; }",
        ".story-tag { border: 1px solid #d1d5db; border-radius: 999px; padding: 2px 8px; font-size: 12px; color: #374151; }",
        ".story-badge { font-size: 12px; border-radius: 999px; padding: 2px 8px; border: 1px solid #d1d5db; margin-left: 6px; }",
        ".story-why { margin: 8px 0 0; color: #374151; font-size: 14px; line-height: 1.5; }",
        ".story-actions { margin-top: 10px; }",
        ".story-actions button { font-size: 12px; border: 1px solid #d1d5db; border-radius: 6px; background: #fff; padding: 4px 8px; cursor: pointer; }",
        ".tone-badges { margin-top: 8px; display: flex; flex-wrap: wrap; gap: 6px; }",
        ".tone-badge { font-size: 11px; border: 1px solid #d1d5db; border-radius: 999px; padding: 2px 7px; color: #374151; }",
        ".method-box { margin-top: 20px; border-top: 1px solid #e5e7eb; padding-top: 12px; }",
        ".method-box details { margin-top: 8px; }",
        ".transparency-note { margin-top: 36px; color: #6b7280; font-size: 14px; line-height: 1.5; border-top: 1px solid #e5e7eb; padding-top: 16px; }",
        "@media (max-width: 720px) {",
        "  .vzla-page { padding: 0 12px; }",
        "  .story-card { padding: 16px; margin-bottom: 20px; }",
        "  .story-title { font-size: 18px; }",
        "  .story-card.featured .story-title { font-size: 19px; }",
        "  .story-summary, .sector-description { font-size: 16px; }",
        "  .latest-updates-item a, .story-title a { display: inline-block; padding: 2px 0; }",
        "}",
        "</style>",
        "",
        "<div class=\"vzla-page\">",
        "",
        "## Executive Brief",
        "",
        f'<section class="vzla-section"><div class="exec-brief">{escape(executive_brief)}</div>',
        f'<p class="diff-note">Run-to-run diff: <strong>{diff_new}</strong> new · <strong>{diff_updated}</strong> updated · <strong>{diff_dropped}</strong> dropped since prior snapshot.</p></section>',
        '<section class="vzla-section">',
        '<h3>Signal & Trend Dashboard</h3>',
        '<div class="intel-grid">',
        f'<div class="intel-card"><p class="intel-title">7-day signal frequency</p><p class="intel-value">{escape(str(trend_summary.get("freq_7d", 0)))}</p></div>',
        f'<div class="intel-card"><p class="intel-title">30-day intensity change</p><p class="intel-value">{escape(str(trend_summary.get("intensity_30d", "→ Stable")))}</p></div>',
        f'<div class="intel-card"><p class="intel-title">90-day direction shift</p><p class="intel-value">{escape(str(trend_summary.get("direction_90d", "→ Stable")))}</p></div>',
        '</div>',
        '</section>',
        '<section class="vzla-section">',
        '<h3>Sanctions & Compliance</h3>',
        f'<p class="diff-note">Sanctions Risk Index (0–100): <strong>{sanctions_index}</strong></p>',
        '<p class="diff-note">This index blends sanctions-signal frequency, negative sentiment, materiality, and escalation language.</p>',
        '</section>',
        '<section class="vzla-section">',
        '<h3>Macro Indicators</h3>',
        '<div class="intel-grid">',
        "",
        "## Research Tools",
        "",
        '<section class="vzla-section">',
        '<div class="vzla-controls">',
        '<input id="searchInput" placeholder="Search title, snippet, or tags" />',
        '<select id="sectorFilter"><option value="all">All sectors</option></select>',
        '<select id="qualityFilter"><option value="all">All source quality tiers</option><option>Tier 1</option><option>Tier 2</option><option>Tier 3</option><option>Unknown</option></select>',
        '<select id="confidenceFilter"><option value="all">All snippet confidence</option><option>High (Feed snippet)</option><option>Medium (Meta description)</option><option>Medium (Page paragraph)</option><option>Low (Page extraction)</option><option>Low (Template fallback)</option></select>',
        '<select id="eventFilter"><option value="all">All event types</option><option>Sanctions</option><option>Oil production/export</option><option>Political transition</option><option>Regulatory reform</option><option>Humanitarian crisis</option><option>FX / Inflation</option><option>Debt restructuring</option><option>Security</option></select>',
        '<select id="sentimentFilter"><option value="all">All sentiment</option><option>Positive</option><option>Neutral</option><option>Negative</option></select>',
        '<select id="riskFilter"><option value="all">All risk scores</option><option value="70">70+ High</option><option value="40">40+ Medium</option><option value="0">0+ Low</option></select>',
        '<select id="entityFilter"><option value="all">All entities</option><option>PDVSA</option><option>Maduro</option><option>OFAC</option><option>IMF</option><option>Chevron</option><option>Bond restructuring</option><option>FX controls</option></select>',
        '<select id="dateFilter"><option value="all">All dates</option><option value="7">Last 7 days</option><option value="30">Last 30 days</option></select>',
        '</div>',
        '<div class="vzla-toolbar">',
        '<a href="{{ \'/data/latest_stories.json\' | relative_url }}" download>Download JSON</a>',
        '<a href="{{ \'/data/latest_stories.csv\' | relative_url }}" download>Download CSV</a>',
        '<a href="#methodology">Methodology</a>',
        '</div>',
        '<div class="jump-links" id="jumpLinks"></div>',
        '</section>',
    ]

    if macro_indicators:
        for indicator in macro_indicators:
            name = str(indicator.get("name", "Indicator"))
            value = str(indicator.get("value", "N/A"))
            trend = str(indicator.get("trend", ""))
            risk_flag = str(indicator.get("risk_flag", "Yellow"))
            spark = _sparkline([float(v) for v in indicator.get("series", []) if isinstance(v, (int, float))])
            risk_class = "risk-yellow"
            if risk_flag.lower().startswith("green"):
                risk_class = "risk-green"
            elif risk_flag.lower().startswith("red"):
                risk_class = "risk-red"
            lines.append(
                f'<div class="intel-card"><p class="intel-title">{escape(name)}</p>'
                f'<p class="intel-value {risk_class}">{escape(value)}</p>'
                f'<p class="diff-note">{escape(trend)} {escape(spark)}</p></div>'
            )
    else:
        lines.append('<div class="intel-card"><p class="intel-title">Macro feed</p><p class="intel-value">Pending</p><p class="diff-note">Add macro_indicators data file.</p></div>')

    lines += [
        '</div>',
        '</section>',
        '',
        '## Sector Intelligence Briefs',
        '<section class="vzla-section">',
        '<div class="sector-brief-grid">',
    ]

    for sector_name in [
        "Extractives",
        "Finance",
        "Agriculture",
        "Health",
        "Governance",
        "Infrastructure",
    ]:
        brief = sector_briefs.get(sector_name, {})
        lines.append('<article class="sector-brief">')
        lines.append(f'<h4>{escape(sector_name)} · Health Score {int(brief.get("health", 5))}/10</h4>')
        lines.append(f'<p>{escape(str(brief.get("summary", "No brief available.")))}</p>')
        lines.append('<p><strong>Top Risks</strong></p><ul>')
        for risk_item in brief.get("risks", [])[:3]:
            lines.append(f'<li>{escape(str(risk_item))}</li>')
        lines.append('</ul><p><strong>Top Opportunities</strong></p><ul>')
        for opp_item in brief.get("opportunities", [])[:3]:
            lines.append(f'<li>{escape(str(opp_item))}</li>')
        lines.append('</ul><p><strong>Forward Watch List</strong></p><ul>')
        for watch_item in brief.get("watch", [])[:3]:
            lines.append(f'<li>{escape(str(watch_item))}</li>')
        lines.append('</ul></article>')

    lines += [
        '</div>',
        '</section>',
        '',
        '<section class="vzla-section">',
        '<h3>Interactive Timeline (Policy + Economic Overlay)</h3>',
        '<table class="timeline-table"><thead><tr><th>Date</th><th>Risk Avg</th><th>Sanctions Signals</th><th>Oil Signals</th></tr></thead><tbody>',
    ]

    if timeline_rows:
        for item in timeline_rows[:12]:
            lines.append(
                f'<tr><td>{escape(str(item.get("date", "")))}</td>'
                f'<td>{escape(str(item.get("risk_avg", 0)))}</td>'
                f'<td>{escape(str(item.get("sanctions_count", 0)))}</td>'
                f'<td>{escape(str(item.get("oil_count", 0)))}</td></tr>'
            )
    else:
        lines.append('<tr><td colspan="4">No timeline history yet.</td></tr>')

    lines += [
        '</tbody></table>',
        '</section>',
        '',
        '## Latest Updates',
        '',
        '<section class="vzla-section">',
        '<ul class="latest-updates-list">',
    ]

    updates = _latest_updates(entries, cfg, limit=5)
    if not updates:
        lines.append('<li class="latest-updates-item">No qualifying updates this cycle.</li>')
    else:
        for entry in updates:
            title = entry.get("title", "(no title)")
            link = entry.get("link", "")
            source = _fmt_source(entry)
            pub = _fmt_date(entry.get("published"))
            sector = detect_sector_label(entry, cfg)
            if link:
                lines.append(f'<li class="latest-updates-item"><a href="{link}">{title}</a>')
            else:
                lines.append(f'<li class="latest-updates-item">{title}')
            lines.append(
                f'<div class="latest-updates-meta">{source} · {pub} · {sector}</div></li>'
            )

    lines += [
        "</ul>",
        "</section>",
        "",
    ]

    section_order = cfg.get("brief_sections", [])
    grouped: dict[str, list[dict]] = {s: [] for s in section_order}
    grouped["Cross-cutting / Policy / Risk"] = grouped.get(
        "Cross-cutting / Policy / Risk", []
    )

    for e in entries:
        label = detect_sector_label(e, cfg)
        if label in grouped:
            grouped[label].append(e)
        else:
            grouped.setdefault(label, []).append(e)

    for section in section_order:
        section_entries = grouped.get(section, [])
        section_id = re.sub(r"[^a-z0-9]+", "-", section.lower()).strip("-")
        lines.append(f'<section class="vzla-section" id="{section_id}" data-sector-header="{escape(section)}">')
        lines.append(f"### {section}")
        lines.append("")
        lines.append(
            f'<p class="sector-description">{section_descriptions.get(section, "Key developments and relevant updates.")}</p>'
        )

        lines.append('<div class="sector-cards">')
        if not section_entries:
            lines.append('<article class="story-card"><p class="story-summary">No qualifying stories available this cycle.</p></article>')
            lines.append("</div>")
            lines.append("</section>")
            lines.append("")
            continue

        top_three = _sort_entries_for_sector(section_entries)[:3]
        for idx, e in enumerate(top_three):
            title = e.get("title", "(no title)")
            link = e.get("link", "")
            pub = _fmt_date(e.get("published"))
            source = _fmt_source(e)
            summary_sentence = _compact_summary(
                e,
                cfg,
                summary_max_chars,
                section_label=section,
                story_index=idx,
            )
            e["_summary_text"] = summary_sentence
            confidence_label = _summary_confidence_label(e)
            quality_tier = _source_quality_tier(e.get("source_domain", ""))
            tags = _research_tags(e, section)
            events = e.get("event_types", []) or []
            sentiment = str(e.get("sentiment", "Neutral"))
            risk_score = int(e.get("risk_score", 0) or 0)
            entities = e.get("entities", []) or []
            why = _why_this_matters(e, cfg, section)
            citation = f'{title} — {source} ({pub}). {link}'.strip()
            tag_text = ",".join(tags + events + entities).lower()
            feature_class = " featured" if idx == 0 else ""
            item_id = _entry_id(e)
            item_date_iso = ""
            if e.get("published") is not None and hasattr(e.get("published"), "strftime"):
                item_date_iso = e.get("published").strftime("%Y-%m-%d")
            flag_risk = "1" if "🔴 Risk" in detect_flags(e, cfg) else "0"
            flag_opp = "1" if "🟢 Opportunity" in detect_flags(e, cfg) else "0"

            lines.append(
                f'<article class="story-card{feature_class}" '
                f'data-sector="{escape(section)}" '
                f'data-quality="{escape(quality_tier)}" '
                f'data-confidence="{escape(confidence_label)}" '
                f'data-event="{escape((events[0] if events else ""))}" '
                f'data-sentiment="{escape(sentiment)}" '
                f'data-risk="{risk_score}" '
                f'data-entity="{escape((entities[0] if entities else ""))}" '
                f'data-date="{escape(pub)}" '
                f'data-item-id="{escape(item_id)}" '
                f'data-item-url="{escape(link)}" '
                f'data-item-title="{escape(title)}" '
                f'data-item-sector="{escape(section)}" '
                f'data-item-snippet="{escape(summary_sentence)}" '
                f'data-item-why="{escape(why)}" '
                f'data-item-dateiso="{escape(item_date_iso)}" '
                f'data-item-publisher="{escape(source)}" '
                f'data-item-confidence="{escape(confidence_label)}" '
                f'data-item-flag-risk="{flag_risk}" '
                f'data-item-flag-opportunity="{flag_opp}" '
                f'data-search="{escape((title + " " + summary_sentence + " " + tag_text).lower())}">'
            )
            lines.append('<h4 class="story-title">')
            if link:
                lines.append(f'<a href="{link}">{escape(title)}</a>')
            else:
                lines.append(escape(title))
            lines.append("</h4>")
            lines.append('<p class="story-one-liner" data-role="one-liner"></p>')
            lines.append(f'<p class="story-summary">{escape(summary_sentence)}</p>')
            lines.append(
                f'<p class="story-meta">{escape(source)} · {escape(pub)} · Retrieved {escape(run_at or "N/A")} '
                f'<span class="story-badge">{escape(quality_tier)}</span>'
                f'<span class="story-badge">{escape(confidence_label)}</span></p>'
            )
            if tags:
                lines.append('<div class="story-tags">')
                for tag in tags:
                    lines.append(f'<span class="story-tag">{escape(tag)}</span>')
                for event in events[:2]:
                    lines.append(f'<span class="story-tag">{escape(event)}</span>')
                lines.append('</div>')
            lines.append(f'<p class="story-why">{escape(why)}</p>')
            lines.append('<div class="tone-badges" data-role="tone-badges"></div>')
            lines.append(
                f'<div class="story-actions"><button class="copy-citation" data-citation="{escape(citation)}">Copy citation</button></div>'
            )
            lines.append("</article>")

        lines.append("</div>")
        lines.append("</section>")
        lines.append("")

    lines += [
        '<section class="vzla-section method-box" id="methodology">',
        '<h3>Methodology & Limitations</h3>',
        '<details><summary>Methodology</summary><p>Ranking combines country/sector relevance, business signals, recency, and source weighting. Snippets prioritize feed-provided text, then page metadata, then first paragraph extraction, then fallback phrasing. Duplicate control uses URL and title similarity checks.</p></details>',
        '<details><summary>Known limitations</summary><p>Some sources are paywalled, throttled, or inaccessible from automated environments; those items may rely on feed snippets or fallback text. Publication dates can vary by feed timezone formatting. This page is a research triage tool and should be paired with source-level verification.</p></details>',
        '</section>',
        '<footer class="transparency-note">This page aggregates publicly available reporting from Venezuelan and international sources. Summaries are descriptive and non-partisan. Updated regularly.</footer>',
        "</div>",
        '<script src="{{ \'/assets/nlg.js\' | relative_url }}"></script>',
        "<script>",
        "(function () {",
        "  const cards = Array.from(document.querySelectorAll('.story-card'));",
        "  const searchInput = document.getElementById('searchInput');",
        "  const sectorFilter = document.getElementById('sectorFilter');",
        "  const qualityFilter = document.getElementById('qualityFilter');",
        "  const confidenceFilter = document.getElementById('confidenceFilter');",
        "  const eventFilter = document.getElementById('eventFilter');",
        "  const sentimentFilter = document.getElementById('sentimentFilter');",
        "  const riskFilter = document.getElementById('riskFilter');",
        "  const entityFilter = document.getElementById('entityFilter');",
        "  const dateFilter = document.getElementById('dateFilter');",
        "  const jumpLinks = document.getElementById('jumpLinks');",
        "",
        "  const sectorNames = [...new Set(cards.map((c) => c.dataset.sector).filter(Boolean))];",
        "  sectorNames.forEach((name) => {",
        "    const option = document.createElement('option');",
        "    option.value = name; option.textContent = name; sectorFilter.appendChild(option);",
        "",
        "    const id = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');",
        "    const link = document.createElement('a');",
        "    link.href = '#' + id; link.textContent = name; jumpLinks.appendChild(link);",
        "  });",
        "",
        "  function daysSince(dateStr) {",
        "    if (!dateStr || dateStr === 'N/A') return null;",
        "    const parsed = new Date(dateStr + 'T00:00:00Z');",
        "    if (Number.isNaN(parsed.getTime())) return null;",
        "    return Math.floor((Date.now() - parsed.getTime()) / 86400000);",
        "  }",
        "",
        "  function applyFilters() {",
        "    const q = (searchInput.value || '').toLowerCase().trim();",
        "    const sector = sectorFilter.value;",
        "    const quality = qualityFilter.value;",
        "    const confidence = confidenceFilter.value;",
        "    const eventType = eventFilter.value;",
        "    const sentiment = sentimentFilter.value;",
        "    const riskThreshold = riskFilter.value === 'all' ? null : Number(riskFilter.value);",
        "    const entity = entityFilter.value;",
        "    const daysWindow = dateFilter.value === 'all' ? null : Number(dateFilter.value);",
        "",
        "    cards.forEach((card) => {",
        "      const text = card.dataset.search || '';",
        "      const sectorMatch = sector === 'all' || card.dataset.sector === sector;",
        "      const qualityMatch = quality === 'all' || card.dataset.quality === quality;",
        "      const confidenceMatch = confidence === 'all' || card.dataset.confidence === confidence;",
        "      const eventMatch = eventType === 'all' || card.dataset.event === eventType;",
        "      const sentimentMatch = sentiment === 'all' || card.dataset.sentiment === sentiment;",
        "      const riskValue = Number(card.dataset.risk || '0');",
        "      const riskMatch = riskThreshold === null || riskValue >= riskThreshold;",
        "      const entityMatch = entity === 'all' || card.dataset.entity === entity;",
        "      const days = daysSince(card.dataset.date);",
        "      const dateMatch = daysWindow === null || (days !== null && days <= daysWindow);",
        "      const searchMatch = !q || text.includes(q);",
        "      card.classList.toggle('hidden', !(sectorMatch && qualityMatch && confidenceMatch && eventMatch && sentimentMatch && riskMatch && entityMatch && dateMatch && searchMatch));",
        "    });",
        "  }",
        "",
        "  [searchInput, sectorFilter, qualityFilter, confidenceFilter, eventFilter, sentimentFilter, riskFilter, entityFilter, dateFilter].forEach((el) => el && el.addEventListener('input', applyFilters));",
        "  [sectorFilter, qualityFilter, confidenceFilter, eventFilter, sentimentFilter, riskFilter, entityFilter, dateFilter].forEach((el) => el && el.addEventListener('change', applyFilters));",
        "  if (window.enhanceNlgCards) { window.enhanceNlgCards(); }",
        "  applyFilters();",
        "",
        "  document.querySelectorAll('.copy-citation').forEach((button) => {",
        "    button.addEventListener('click', async () => {",
        "      const text = button.getAttribute('data-citation') || '';",
        "      try {",
        "        await navigator.clipboard.writeText(text);",
        "        const old = button.textContent; button.textContent = 'Copied';",
        "        setTimeout(() => { button.textContent = old; }, 1200);",
        "      } catch (_) {",
        "        button.textContent = 'Copy failed';",
        "      }",
        "    });",
        "  });",
        "})();",
        "</script>",
        "",
    ]

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run(config_path: str = CONFIG_PATH, feeds_path: str = FEEDS_PATH) -> None:
    cfg = load_config(config_path)
    feed_urls = load_feeds(feeds_path)
    now = datetime.now(timezone.utc)

    logger.info("Fetching %d feeds…", len(feed_urls))
    raw_entries: list[dict] = []
    for url in feed_urls:
        fetched = fetch_feed(url)
        logger.info("  %s → %d entries", url, len(fetched))
        raw_entries.extend(fetched)

    fetched_count = len(raw_entries)
    logger.info("Total fetched: %d", fetched_count)

    filtered = filter_entries(raw_entries, cfg, now)
    filtered_count = len(filtered)
    logger.info("After filtering: %d", filtered_count)

    threshold = cfg.get("deduplication", {}).get("title_similarity_threshold", 0.90)
    deduped = deduplicate(filtered, threshold, cfg=cfg)
    deduped_count = len(deduped)
    logger.info("After deduplication: %d", deduped_count)

    ranked = score_and_rank(deduped, cfg, now)
    max_results = cfg.get("max_results", 35)
    top = select_diverse_top_entries(ranked, cfg, max_results)
    enrich_entries_with_article_text(top, cfg)
    selected_count = len(top)
    logger.info("Selected top %d entries", selected_count)

    for entry in top:
        _annotate_intelligence(entry)

    os.makedirs(DOCS_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    docs_data_dir = os.path.join(DOCS_DIR, "data")
    docs_assets_dir = os.path.join(DOCS_DIR, "assets")
    os.makedirs(docs_data_dir, exist_ok=True)
    os.makedirs(docs_assets_dir, exist_ok=True)

    latest_snapshot_path = os.path.join(DATA_DIR, "latest_stories.json")
    latest_csv_path = os.path.join(DATA_DIR, "latest_stories.csv")
    signal_history_path = os.path.join(DATA_DIR, "signal_history.json")
    alerts_path = os.path.join(DATA_DIR, "alerts.json")
    intelligence_summary_path = os.path.join(DATA_DIR, "intelligence_summary.json")
    macro_path = os.path.join(DATA_DIR, "macro_indicators.json")

    previous_snapshot: list[dict] = []
    if os.path.exists(latest_snapshot_path):
        try:
            with open(latest_snapshot_path, "r", encoding="utf-8") as fh:
                loaded = json.load(fh)
                if isinstance(loaded, list):
                    previous_snapshot = loaded
        except (json.JSONDecodeError, OSError):
            previous_snapshot = []

    previous_map = {
        str(item.get("id", "")): item
        for item in previous_snapshot
        if isinstance(item, dict) and item.get("id")
    }
    current_ids = {_entry_id(entry) for entry in top}
    diff_new_ids: list[str] = []
    diff_updated_ids: list[str] = []
    diff_dropped_ids: list[str] = []
    if not previous_map:
        diff_new = 0
        diff_updated = 0
        diff_dropped = 0
    else:
        diff_new = 0
        diff_updated = 0
        for entry in top:
            entry_id = _entry_id(entry)
            prev = previous_map.get(entry_id)
            if prev is None:
                diff_new += 1
                diff_new_ids.append(entry_id)
            elif str(prev.get("title", "")) != str(entry.get("title", "")):
                diff_updated += 1
                diff_updated_ids.append(entry_id)
        diff_dropped_ids = [entry_id for entry_id in previous_map if entry_id not in current_ids]
        diff_dropped = len(diff_dropped_ids)

    section_alias = {
        "Extractives & Mining": "Extractives",
        "Finance & Investment": "Finance",
        "Food & Agriculture": "Agriculture",
        "Health & Water": "Health",
        "Cross-cutting / Policy / Risk": "Governance",
    }

    intelligence_rows: list[dict] = []
    normalized_items: list[dict] = []
    for entry in top:
        section = detect_sector_label(entry, cfg)
        aliased_section = section_alias.get(section, section)
        summary_text = _compact_summary(
            entry,
            cfg,
            max_chars=min(280, int(cfg.get("summary_max_chars", 280))),
            section_label=section,
            story_index=0,
        )
        entry["_summary_text"] = summary_text
        intelligence_rows.append(_serialize_entry(entry, aliased_section))

        published_at = ""
        if entry.get("published") is not None and hasattr(entry.get("published"), "strftime"):
            published_at = entry["published"].strftime("%Y-%m-%d")

        item_id = _stable_item_id(
            str(entry.get("link", "")),
            str(entry.get("title", "")),
            published_at,
        )
        entry_key = _entry_id(entry)
        previous_item = previous_map.get(entry_key, {})
        flags = {
            "risk": "🔴 Risk" in detect_flags(entry, cfg),
            "opportunity": "🟢 Opportunity" in detect_flags(entry, cfg),
            "new": entry_key in diff_new_ids,
            "updated": entry_key in diff_updated_ids,
        }
        if not previous_map:
            flags["new"] = True

        item = {
            "id": item_id,
            "title": str(entry.get("title", "")),
            "url": str(entry.get("link", "")),
            "publisher": _derive_publisher(entry),
            "publishedAt": published_at,
            "sourceTier": _source_quality_tier(str(entry.get("source_domain", ""))),
            "sector": section,
            "snippet": summary_text,
            "snippet_status": str(entry.get("snippet_status", "")),
            "summary_confidence": _summary_confidence_label(entry),
            "event_types": list(entry.get("event_types", []) or []),
            "sentiment": str(entry.get("sentiment", "Neutral")),
            "materiality": int(entry.get("materiality", 1) or 1),
            "risk_score": int(entry.get("risk_score", 0) or 0),
            "entities": list(entry.get("entities", []) or []),
            "tags": _research_tags(entry, section),
            "flags": flags,
            "metrics": {
                "numbers": _extract_numbers(
                    " ".join(
                        [
                            summary_text,
                            str(entry.get("article_text", "") or ""),
                            str(entry.get("summary", "") or ""),
                            str(previous_item.get("summary", "") or ""),
                        ]
                    )
                )
            },
        }
        source_text_for_insight = " ".join(
            [
                str(entry.get("article_text", "") or ""),
                str(entry.get("meta_description", "") or ""),
                str(entry.get("first_paragraph", "") or ""),
                summary_text,
            ]
        ).strip()
        item["insight2"] = _generate_insight2(item, source_text_for_insight)
        item["icons"] = _icons_for_item(item)
        normalized_items.append(item)

    infra_rows = [row for row in intelligence_rows if "Infrastructure" in row.get("tags", [])]

    sector_rows: dict[str, list[dict]] = {
        "Extractives": [r for r in intelligence_rows if r.get("sector") == "Extractives"],
        "Finance": [r for r in intelligence_rows if r.get("sector") == "Finance"],
        "Agriculture": [r for r in intelligence_rows if r.get("sector") == "Agriculture"],
        "Health": [r for r in intelligence_rows if r.get("sector") == "Health"],
        "Governance": [r for r in intelligence_rows if r.get("sector") == "Governance"],
        "Infrastructure": infra_rows,
    }
    sector_briefs = {name: _build_sector_brief(name, rows) for name, rows in sector_rows.items()}

    if os.path.exists(macro_path):
        try:
            with open(macro_path, "r", encoding="utf-8") as fh:
                macro_indicators = json.load(fh)
            if not isinstance(macro_indicators, list):
                macro_indicators = []
        except (json.JSONDecodeError, OSError):
            macro_indicators = []
    else:
        macro_indicators = [
            {"name": "GDP growth estimate", "value": "N/A", "trend": "Weekly refresh pending", "risk_flag": "Yellow", "series": [0, 0, 0]},
            {"name": "Inflation rate", "value": "N/A", "trend": "Weekly refresh pending", "risk_flag": "Yellow", "series": [0, 0, 0]},
            {"name": "Oil production (bpd)", "value": "N/A", "trend": "Weekly refresh pending", "risk_flag": "Yellow", "series": [0, 0, 0]},
            {"name": "FX official vs parallel", "value": "N/A", "trend": "Weekly refresh pending", "risk_flag": "Yellow", "series": [0, 0, 0]},
        ]
        with open(macro_path, "w", encoding="utf-8") as fh:
            json.dump(macro_indicators, fh, indent=2)

    history_records: list[dict] = []
    if os.path.exists(signal_history_path):
        try:
            with open(signal_history_path, "r", encoding="utf-8") as fh:
                loaded = json.load(fh)
                if isinstance(loaded, list):
                    history_records = loaded
        except (json.JSONDecodeError, OSError):
            history_records = []

    today_key = now.strftime("%Y-%m-%d")
    sanctions_count = sum(1 for row in intelligence_rows if "Sanctions" in row.get("event_types", []))
    oil_count = sum(1 for row in intelligence_rows if "Oil production/export" in row.get("event_types", []))
    risk_avg = round(
        sum(int(row.get("risk_score", 0)) for row in intelligence_rows) / max(1, len(intelligence_rows)),
        2,
    )
    current_record = {
        "date": today_key,
        "signals_count": len(intelligence_rows),
        "sanctions_count": sanctions_count,
        "oil_count": oil_count,
        "risk_avg": risk_avg,
    }
    if history_records and history_records[-1].get("date") == today_key:
        history_records[-1] = current_record
    else:
        history_records.append(current_record)
    history_records = history_records[-120:]

    freq_7d = round(_window_stats(history_records, "signals_count", 7), 1)
    curr_30 = _window_stats(history_records, "risk_avg", 30)
    prev_30 = _window_stats(history_records[:-30], "risk_avg", 30)
    curr_90 = _window_stats(history_records, "risk_avg", 90)
    prev_90 = _window_stats(history_records[:-90], "risk_avg", 90)
    trend_summary = {
        "freq_7d": freq_7d,
        "intensity_30d": _trend_direction(curr_30, prev_30),
        "direction_90d": _trend_direction(curr_90, prev_90),
    }

    sanctions_index = _calculate_sanctions_index(intelligence_rows, history_records)

    alerts = []
    if sanctions_count > 0:
        alerts.append({"type": "New sanctions", "triggered": True, "detail": f"{sanctions_count} sanctions-related signals in current cycle."})
    if oil_count >= 3:
        alerts.append({"type": "Oil export change >10%", "triggered": True, "detail": "Oil/export signal density elevated; review cargo and production narratives."})
    inflation_hits = sum(1 for row in intelligence_rows if "FX / Inflation" in row.get("event_types", []))
    if inflation_hits > 0:
        alerts.append({"type": "Inflation spike news", "triggered": True, "detail": f"{inflation_hits} inflation/FX items surfaced."})
    decree_hits = sum(1 for row in intelligence_rows if "Political transition" in row.get("event_types", []))
    if decree_hits > 0:
        alerts.append({"type": "Executive decree", "triggered": True, "detail": f"{decree_hits} governance/decree-related signals detected."})
    protest_hits = sum(1 for row in intelligence_rows if "Security" in row.get("event_types", []))
    if protest_hits > 0:
        alerts.append({"type": "Protest escalation", "triggered": True, "detail": f"{protest_hits} security/protest indicators detected."})

    timeline_rows = list(reversed(history_records[-12:]))

    run_meta = {
        "run_at": now.isoformat(),
        "fetched": fetched_count,
        "filtered": filtered_count,
        "deduplicated": deduped_count,
        "selected": selected_count,
        "diff_new": diff_new,
        "diff_updated": diff_updated,
        "diff_dropped": diff_dropped,
        "trend_summary": trend_summary,
        "sanctions_index": sanctions_index,
        "macro_indicators": macro_indicators,
        "sector_briefs": sector_briefs,
        "timeline_rows": timeline_rows,
        "output_file": OUTPUT_PATH,
        "metadata_file": METADATA_PATH,
        "docs_data_dir": docs_data_dir,
    }

    markdown = _build_docs_shell(now.strftime("%Y-%m-%d %H:%M UTC"))
    if _write_if_changed(OUTPUT_PATH, markdown):
        logger.info("Wrote %s", OUTPUT_PATH)
    else:
        logger.info("No changes to %s", OUTPUT_PATH)

    app_js_path = os.path.join(docs_assets_dir, "app.js")
    styles_css_path = os.path.join(docs_assets_dir, "styles.css")
    if _write_if_changed(app_js_path, _default_app_js()):
        logger.info("Wrote %s", app_js_path)
    if _write_if_changed(styles_css_path, _default_styles_css()):
        logger.info("Wrote %s", styles_css_path)

    with open(METADATA_PATH, "w", encoding="utf-8") as fh:
        json.dump(run_meta, fh, indent=2, default=str)
    logger.info("Wrote %s", METADATA_PATH)

    section_order = cfg.get("brief_sections", [])
    grouped: dict[str, list[dict]] = {s: [] for s in section_order}
    grouped["Cross-cutting / Policy / Risk"] = grouped.get("Cross-cutting / Policy / Risk", [])
    for entry in top:
        section = detect_sector_label(entry, cfg)
        grouped.setdefault(section, []).append(entry)

    export_rows: list[dict] = []
    for section in section_order:
        top_three = _sort_entries_for_sector(grouped.get(section, []))[:3]
        for idx, entry in enumerate(top_three):
            summary_text = _compact_summary(
                entry,
                cfg,
                max_chars=min(280, int(cfg.get("summary_max_chars", 280))),
                section_label=section,
                story_index=idx,
            )
            entry["_summary_text"] = summary_text
            row = _serialize_entry(entry, section)
            row["retrieved"] = now.strftime("%Y-%m-%d %H:%M UTC")
            export_rows.append(row)

    normalized_by_id = {str(item.get("id", "")): item for item in normalized_items}
    sector_synth: dict[str, dict] = {}
    sectors_payload: list[dict] = []
    for section in section_order:
        top_three = _sort_entries_for_sector(grouped.get(section, []))[:3]
        items_for_section = []
        for entry in top_three:
            published_at = ""
            if entry.get("published") is not None and hasattr(entry.get("published"), "strftime"):
                published_at = entry["published"].strftime("%Y-%m-%d")
            item = normalized_by_id.get(
                _stable_item_id(
                    str(entry.get("link", "")),
                    str(entry.get("title", "")),
                    published_at,
                )
            )
            if item:
                items_for_section.append(item)
        synth = _build_sector_synth(section, items_for_section)
        sector_synth[section] = synth
        sectors_payload.append({"name": section, "synth": synth, "items": items_for_section})

    highlights_payload = _build_highlights(normalized_items, sector_synth)
    latest_payload = {
        "runAt": now.isoformat(),
        "totalItems": len(normalized_items),
        "sectors": sectors_payload,
    }
    diff_payload = {
        "runAt": now.isoformat(),
        "counts": {
            "new": diff_new,
            "updated": diff_updated,
            "dropped": diff_dropped,
        },
        "new": diff_new_ids,
        "updated": diff_updated_ids,
        "dropped": diff_dropped_ids,
    }
    macros_payload = {
        "runAt": now.isoformat(),
        "indicators": macro_indicators,
    }

    docs_latest_path = os.path.join(docs_data_dir, "latest.json")
    docs_diff_path = os.path.join(docs_data_dir, "diff.json")
    docs_macros_path = os.path.join(docs_data_dir, "macros.json")
    docs_highlights_path = os.path.join(docs_data_dir, "highlights.json")

    if _write_if_changed(docs_latest_path, json.dumps(latest_payload, indent=2)):
        logger.info("Wrote %s", docs_latest_path)
    if _write_if_changed(docs_diff_path, json.dumps(diff_payload, indent=2)):
        logger.info("Wrote %s", docs_diff_path)
    if _write_if_changed(docs_macros_path, json.dumps(macros_payload, indent=2)):
        logger.info("Wrote %s", docs_macros_path)
    if _write_if_changed(docs_highlights_path, json.dumps(highlights_payload, indent=2)):
        logger.info("Wrote %s", docs_highlights_path)

    with open(latest_snapshot_path, "w", encoding="utf-8") as fh:
        json.dump(export_rows, fh, indent=2)
    logger.info("Wrote %s", latest_snapshot_path)

    with open(latest_csv_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "id",
            "title",
            "url",
            "source",
            "source_quality",
            "published",
            "retrieved",
            "sector",
            "summary",
            "summary_confidence",
            "event_types",
            "sentiment",
            "materiality",
            "risk_score",
            "entities",
            "tags",
        ])
        for row in export_rows:
            writer.writerow([
                row.get("id", ""),
                row.get("title", ""),
                row.get("url", ""),
                row.get("source", ""),
                row.get("source_quality", ""),
                row.get("published", ""),
                row.get("retrieved", ""),
                row.get("sector", ""),
                row.get("summary", ""),
                row.get("summary_confidence", ""),
                "; ".join(row.get("event_types", [])),
                row.get("sentiment", ""),
                row.get("materiality", ""),
                row.get("risk_score", ""),
                "; ".join(row.get("entities", [])),
                "; ".join(row.get("tags", [])),
            ])
    logger.info("Wrote %s", latest_csv_path)

    with open(signal_history_path, "w", encoding="utf-8") as fh:
        json.dump(history_records, fh, indent=2)
    logger.info("Wrote %s", signal_history_path)

    with open(alerts_path, "w", encoding="utf-8") as fh:
        json.dump({"run_at": now.isoformat(), "alerts": alerts}, fh, indent=2)
    logger.info("Wrote %s", alerts_path)

    with open(intelligence_summary_path, "w", encoding="utf-8") as fh:
        json.dump(
            {
                "run_at": now.isoformat(),
                "trend_summary": trend_summary,
                "sanctions_index": sanctions_index,
                "sector_briefs": sector_briefs,
            },
            fh,
            indent=2,
        )
    logger.info("Wrote %s", intelligence_summary_path)


if __name__ == "__main__":
    run()
