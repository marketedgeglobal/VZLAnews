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
from html import unescape
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
        if not entry.get("snippet"):
            entry["snippet"] = _entry_feed_snippet(entry, max_chars=280)

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


def _clean_snippet(
    text: str,
    title: str,
    max_chars: int = 280,
    min_chars: int = 80,
) -> str:
    clean = _normalize_text_block(text)
    if not clean:
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
    topic_lower = topic[:1].lower() + topic[1:] if topic else "the topic"
    section_text = section_label if section_label else "policy and risk"

    templates = [
        f"Key update on {topic_lower}.",
        f"Coverage focuses on developments linked to {topic_lower}.",
        f"Developments related to {section_text.lower()} in Venezuela remain active.",
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
        return feed_snippet

    for key in ("meta_description", "first_paragraph"):
        cleaned = _clean_snippet(
            entry.get(key, "") or "",
            title,
            max_chars=max_chars,
            min_chars=80,
        )
        if cleaned:
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
                return chosen

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


def build_markdown(entries: list[dict], cfg: dict, run_meta: dict) -> str:
    country_name = cfg.get("country", {}).get("name", "Venezuela")
    summary_max_chars = min(280, int(cfg.get("summary_max_chars", 280)))
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
        "<style>",
        ".vzla-page { max-width: 920px; margin: 0 auto; }",
        ".vzla-section { margin-top: 32px; }",
        ".latest-updates-list { list-style: none; margin: 0; padding: 0; }",
        ".latest-updates-item { padding: 10px 0; border-bottom: 1px solid #e5e7eb; }",
        ".latest-updates-item:last-child { border-bottom: none; }",
        ".latest-updates-meta { margin-top: 4px; color: #6b7280; font-size: 13px; line-height: 1.4; }",
        ".sector-title { margin-bottom: 6px; }",
        ".sector-description { margin: 0 0 20px; color: #4b5563; font-size: 16px; line-height: 1.5; }",
        ".sector-cards { display: block; }",
        ".story-card { border: 1px solid #e5e7eb; box-shadow: 0 1px 3px rgba(15, 23, 42, 0.08); border-radius: 8px; padding: 18px; margin-bottom: 22px; }",
        ".story-card:last-child { margin-bottom: 0; }",
        ".story-title { margin: 0 0 10px; font-size: 18px; line-height: 1.35; font-weight: 700; }",
        ".story-card.featured .story-title { font-size: 20px; }",
        ".story-summary { margin: 0 0 10px; font-size: 16px; line-height: 1.55; color: #111827; }",
        ".story-meta { margin: 0; color: #6b7280; font-size: 13px; line-height: 1.4; }",
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
        "## Latest Updates",
        "",
        "<section class=\"vzla-section\">",
        "<ul class=\"latest-updates-list\">",
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
        lines.append('<section class="vzla-section">')
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
            feature_class = " featured" if idx == 0 else ""

            lines.append(f'<article class="story-card{feature_class}">')
            lines.append('<h4 class="story-title">')
            if link:
                lines.append(f'<a href="{link}">{title}</a>')
            else:
                lines.append(title)
            lines.append("</h4>")
            lines.append(f'<p class="story-summary">{summary_sentence}</p>')
            lines.append(f'<p class="story-meta">{source} · {pub}</p>')
            lines.append("</article>")

        lines.append("</div>")
        lines.append("</section>")
        lines.append("")

    lines += [
        '<footer class="transparency-note">This page aggregates publicly available reporting from Venezuelan and international sources. Summaries are descriptive and non-partisan. Updated regularly.</footer>',
        "</div>",
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

    run_meta = {
        "run_at": now.isoformat(),
        "fetched": fetched_count,
        "filtered": filtered_count,
        "deduplicated": deduped_count,
        "selected": selected_count,
        "output_file": OUTPUT_PATH,
        "metadata_file": METADATA_PATH,
    }

    markdown = build_markdown(top, cfg, run_meta)

    os.makedirs(DOCS_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)

    # Idempotency: only write if content changed
    existing_md = ""
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, "r", encoding="utf-8") as fh:
            existing_md = fh.read()

    if markdown != existing_md:
        with open(OUTPUT_PATH, "w", encoding="utf-8") as fh:
            fh.write(markdown)
        logger.info("Wrote %s", OUTPUT_PATH)
    else:
        logger.info("No changes to %s", OUTPUT_PATH)

    with open(METADATA_PATH, "w", encoding="utf-8") as fh:
        json.dump(run_meta, fh, indent=2, default=str)
    logger.info("Wrote %s", METADATA_PATH)


if __name__ == "__main__":
    run()
