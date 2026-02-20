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
            published = _parse_date(e)
            entries.append(
                {
                    "title": title,
                    "link": link,
                    "summary": summary,
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


def enrich_entries_with_article_text(entries: list[dict], cfg: dict) -> None:
    extraction_cfg = cfg.get("article_extraction", {})
    if not extraction_cfg.get("enabled", True):
        return

    max_items = max(0, int(extraction_cfg.get("max_items", 12)))
    timeout_seconds = max(1, int(extraction_cfg.get("timeout_seconds", 6)))
    min_chars = max(100, int(extraction_cfg.get("min_chars", 240)))
    max_chars = max(1000, int(extraction_cfg.get("max_chars", 6000)))

    fetched_count = 0
    enriched_count = 0

    prioritized_entries = sorted(
        entries,
        key=lambda item: 1 if "news.google.com" in (item.get("link", "") or "") else 0,
    )

    for entry in prioritized_entries:
        if fetched_count >= max_items:
            break
        link = entry.get("link", "")
        if not link:
            continue
        fetched_count += 1
        article_text = fetch_article_text(link, timeout_seconds=timeout_seconds, max_chars=max_chars)
        if len(article_text) >= min_chars:
            entry["article_text"] = article_text
            enriched_count += 1

    logger.info(
        "Article text enrichment: %d/%d entries enriched",
        enriched_count,
        fetched_count,
    )


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
    ]
    return any(marker in lower for marker in noisy_markers)


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


def _descriptive_summary(entry: dict, cfg: dict, max_chars: int) -> str:
    base_text = (entry.get("article_text", "") or "").strip()
    has_article_text = bool(base_text)
    if not has_article_text:
        base_text = ""
    clean = _normalize_text_block(base_text)

    candidates = []
    if clean:
        parts = re.split(r"(?<=[.!?])\s+", clean)
        for part in parts:
            p = part.strip(" -\u2022\t\n\r")
            if len(p) >= 35 and not _sentence_is_noise(p) and not _is_fragment(p):
                candidates.append(p)

    title = (entry.get("title", "") or "").strip().rstrip(".")
    sector = detect_sector_label(entry, cfg)
    domain = entry.get("source_domain", "") or "the source"
    pub = _fmt_date(entry.get("published"))
    flags = detect_flags(entry, cfg)

    # Prefer substantive extracted article text when available.
    if has_article_text:
        for sentence in candidates:
            if _title_similarity(title, sentence) < 0.78:
                chosen = sentence
                if len(chosen) > max_chars:
                    clipped = chosen[:max_chars].rsplit(" ", 1)[0].strip()
                    chosen = (clipped or chosen[:max_chars]).strip().rstrip(".") + "…"
                return chosen if chosen.endswith((".", "!", "?", "…")) else chosen + "."

    flag_text = ""
    if "🔴 Risk" in flags and "🟢 Opportunity" in flags:
        flag_text = " with both commercial upside and material policy risk signals"
    elif "🔴 Risk" in flags:
        flag_text = " with elevated policy or operational risk signals"
    elif "🟢 Opportunity" in flags:
        flag_text = " with potential near-term commercial openings"

    topic = _title_topic(entry)
    fallback = (
        f"In {sector}, reporting from {domain} ({pub}) highlights {topic.lower()}"
        f"{flag_text}, with potential implications for near-term policy, operations, or investment decisions."
    )
    if len(fallback) > max_chars:
        clipped = fallback[:max_chars].rsplit(" ", 1)[0].strip()
        fallback = (clipped or fallback[:max_chars]).strip().rstrip(".") + "…"
    return fallback

def _latest_news_synthesis(entries: list[dict], cfg: dict) -> list[str]:
    section_order = cfg.get("brief_sections", [])
    if not entries:
        return [
            "This run did not identify qualifying Venezuela news items across the configured sectors.",
            "No validated sector signals were strong enough to populate the ranked shortlist.",
            "Opportunity-linked terms were not present in qualifying entries for this cycle.",
            "Risk-linked terms were not present in qualifying entries for this cycle.",
            "Source activity was observed, but filtering and relevance controls removed all candidates.",
            "The next scheduled run will refresh this summary as new sector-relevant items are published.",
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

    return [
        f"This brief synthesizes {len(entries)} ranked Venezuela-focused items spanning {len(represented)} sectors: {', '.join(represented)}.",
        f"The strongest concentration is in {top_sector_text}.",
        f"Across headlines and summaries, the dominant themes are {themes_text}.",
        f"Opportunity signals appear in {opportunity_count} items, indicating active commercial or partnership openings.",
        f"Risk signals appear in {risk_count} items, highlighting policy, sanctions, or operational uncertainty to monitor.",
        f"Coverage is sourced primarily from {domain_text}, with publication timing {date_span}.",
    ]


def build_markdown(entries: list[dict], cfg: dict, run_meta: dict) -> str:
    country_name = cfg.get("country", {}).get("name", "Venezuela")
    summary_max_chars = int(cfg.get("summary_max_chars", 520))

    lines = [
        f"# {country_name} News Intelligence",
        "",
        "## Latest News Synthesis",
        "",
    ]

    for sentence in _latest_news_synthesis(entries, cfg):
        lines.append(f"- {sentence}")

    lines += [
        "",
        "---",
        "",
        "## Top Results",
        "",
    ]

    # Group by section order
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
        lines.append(f"### {section}")
        lines.append("")
        if not section_entries:
            lines.append("_No items this week._")
            lines.append("")
            continue
        for e in section_entries:
            flags = detect_flags(e, cfg)
            flag_str = " ".join(flags)
            title = e.get("title", "(no title)")
            link = e.get("link", "")
            pub = _fmt_date(e.get("published"))
            if link:
                lines.append(f"- **[{title}]({link})**  ")
            else:
                lines.append(f"- **{title}**  ")
            meta_parts = [f"Date: {pub}"]
            if flag_str:
                meta_parts.append(flag_str)
            lines.append(f"  {' | '.join(meta_parts)}")
            summary_sentence = _descriptive_summary(e, cfg, summary_max_chars)
            lines.append("  Summary:")
            lines.append(f"  - {summary_sentence}")
            lines.append("")

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
