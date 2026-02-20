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
from datetime import datetime, timezone
from difflib import SequenceMatcher
from urllib.parse import urlparse

import time as _time

import feedparser
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
    return any(t in text for t in all_terms)


def passes_exclude_filter(entry: dict, exclude_terms: list[str]) -> bool:
    text = _text(entry)
    return not any(t.lower() in text for t in exclude_terms)


def filter_entries(
    entries: list[dict],
    cfg: dict,
    now: datetime,
) -> list[dict]:
    max_age = cfg.get("max_age_days", 7)
    country_terms = [t.lower() for t in cfg.get("country_terms", [])]
    geo_terms = [t.lower() for t in cfg.get("geo_context_terms", [])]
    exclude_terms = [t.lower() for t in cfg.get("exclude_terms", [])]
    require_country = cfg.get("require_country_match", True)

    filtered = []
    for e in entries:
        if not passes_age_filter(e, max_age, now):
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


def deduplicate(entries: list[dict], threshold: float = 0.90) -> list[dict]:
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
        for seen in seen_titles:
            ratio = SequenceMatcher(None, title, seen).ratio()
            if ratio >= threshold:
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


def detect_sector_label(entry: dict, cfg: dict) -> str:
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


def build_markdown(entries: list[dict], cfg: dict, run_meta: dict) -> str:
    country_name = cfg.get("country", {}).get("name", "Venezuela")
    now_str = run_meta.get("run_at", datetime.now(timezone.utc).isoformat())

    lines = [
        f"# {country_name} News Intelligence",
        "",
        f"> Generated: {now_str}  ",
        f"> Entries fetched: {run_meta['fetched']}  ",
        f"> After filtering: {run_meta['filtered']}  ",
        f"> After deduplication: {run_meta['deduplicated']}  ",
        f"> Top results shown: {run_meta['selected']}",
        "",
        "---",
        "",
        "## Pipeline Metrics",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Fetched | {run_meta['fetched']} |",
        f"| Filtered | {run_meta['filtered']} |",
        f"| Deduplicated | {run_meta['deduplicated']} |",
        f"| Selected | {run_meta['selected']} |",
        "",
        "## Scoring Summary",
        "",
    ]

    if entries:
        scores = [e["score"] for e in entries]
        lines += [
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Entries scored | {len(scores)} |",
            f"| Average score | {sum(scores)/len(scores):.3f} |",
            f"| Highest score | {max(scores):.3f} |",
            f"| Lowest score | {min(scores):.3f} |",
        ]
    else:
        lines.append("_No entries scored this run._")

    lines += [
        "",
        "## Run Metadata",
        "",
        f"| Key | Value |",
        f"|-----|-------|",
        f"| Output file | `{run_meta.get('output_file', OUTPUT_PATH)}` |",
        f"| Metadata file | `{run_meta.get('metadata_file', METADATA_PATH)}` |",
        f"| Timezone | UTC |",
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
            domain = e.get("source_domain", "")
            score = e.get("score", 0.0)
            if link:
                lines.append(f"- **[{title}]({link})**  ")
            else:
                lines.append(f"- **{title}**  ")
            meta_parts = [f"Score: {score:.3f}", f"Date: {pub}", f"Source: {domain}"]
            if flag_str:
                meta_parts.append(flag_str)
            lines.append(f"  {' | '.join(meta_parts)}")
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
    deduped = deduplicate(filtered, threshold)
    deduped_count = len(deduped)
    logger.info("After deduplication: %d", deduped_count)

    ranked = score_and_rank(deduped, cfg, now)
    max_results = cfg.get("max_results", 35)
    top = ranked[:max_results]
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
