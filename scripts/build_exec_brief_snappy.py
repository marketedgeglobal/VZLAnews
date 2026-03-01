import datetime
import json
import os
import re
from urllib.parse import urlparse

LATEST_JSON = "docs/data/latest.json"
PDF_CANDIDATES = [
    "docs/data/pdf_publications_2026.json",
    "docs/data/pdf_publications_recent.json",
    "docs/data/pdf_publications_2025_2026.json",
]
OUT_JSON = "docs/data/exec_brief.json"

THEMES = [
    ("sanctions", ["sanction", "ofac", "license", "compliance", "restriction", "designated", "enforcement"]),
    ("politics", ["election", "opposition", "government", "parliament", "national assembly", "cabello", "maduro", "cne"]),
    ("macro", ["inflation", "gdp", "fiscal", "debt", "budget", "exchange rate", "currency", "reserves", "growth"]),
    ("energy", ["oil", "pdvsa", "barrel", "export", "opec", "refinery", "crude", "gas"]),
    ("humanitarian", ["food", "hunger", "health", "water", "migration", "displacement", "disease", "school", "unemployment"]),
    ("investment", ["investment", "financing", "loan", "bond", "tender", "procurement", "grant", "eoi", "rfp", "rfi"]),
]

SNAPPY_HEADLINES = {
    "sanctions": [
        "Sanctions pressure shifts",
        "Compliance risk moves",
        "Sanctions signals tighten or loosen",
    ],
    "politics": [
        "Political calculus changes",
        "Governance signal spikes",
        "Domestic politics reprice risk",
    ],
    "macro": [
        "Macro picture re-sets",
        "Inflation and growth signals move",
        "Fiscal reality shows through",
    ],
    "energy": [
        "Oil flows matter again",
        "PDVSA signal changes",
        "Energy constraints and openings",
    ],
    "humanitarian": [
        "Social strain persists",
        "Humanitarian pressure points",
        "Services and livelihoods under stress",
    ],
    "investment": [
        "Capital and contracting signals",
        "Financing windows open or close",
        "Procurement and funding pipeline",
    ],
}

SO_WHAT = {
    "sanctions": "this changes counterparty risk and deal feasibility for corporates and donors",
    "politics": "this affects policy continuity, stakeholder alignment, and implementation risk",
    "macro": "this shifts assumptions on stability, pricing, and operating conditions",
    "energy": "this influences export revenue, contract risk, and near-term cash dynamics",
    "humanitarian": "this raises program urgency and shapes delivery constraints on the ground",
    "investment": "this creates near-term entry points for advisory, partnerships, and bids",
}

NOISE_PATTERNS = [
    r"The publication adds implementation detail that clarifies pace, constraints, and expected counterpart response\.?",
    r"See All Newsletters.*$",
    r"AP QUIZZES.*$",
    r"Test Your News I\.Q.*$",
    r"The Afternoon Wire.*$",
    r"Click here.*$",
]


def norm(text):
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    return cleaned.replace("—", "-")


def split_sentences(text):
    parts = re.split(r"(?<=[\.\!\?])\s+(?=[A-ZÁÉÍÓÚÑ])", norm(text))
    return [part.strip() for part in parts if part.strip()]


def one_sentence(text):
    for sentence in split_sentences(text):
        if len(sentence) >= 45:
            capped = sentence[:220].rstrip(".")
            return f"{capped}."
    compact = norm(text)
    if len(compact) > 220:
        compact = compact[:220].rstrip(".")
    if not compact:
        return ""
    return compact if compact.endswith(".") else f"{compact}."


def host(url):
    try:
        return urlparse(str(url or "")).netloc.lower()
    except Exception:
        return ""


def clean_substance(text):
    cleaned = norm(text)
    for pattern in NOISE_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .;")
    return cleaned


def clean_title(text):
    cleaned = norm(text)
    cleaned = re.sub(r"\s*\|\s*.*$", "", cleaned)
    cleaned = re.sub(r"\s*-\s*Reuters\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*\(.*?\)\s*$", "", cleaned)
    return cleaned


def is_noisy(text):
    low = str(text or "").lower()
    bad_tokens = [
        "newsletter",
        "quiz",
        "markdown content",
        "url source",
        "anthropic",
        "caught up",
        "click here",
    ]
    return any(token in low for token in bad_tokens)


def substance(item):
    insight = item.get("insight2")
    if isinstance(insight, dict):
        candidate = clean_substance(f"{insight.get('s1', '')} {insight.get('s2', '')}")
        if len(candidate) >= 60:
            return candidate
        candidate = clean_substance(insight.get("s1", ""))
        if len(candidate) >= 60:
            return candidate

    return clean_substance(item.get("preview") or item.get("description") or item.get("snippet") or "")


def pick_theme(text):
    low = str(text or "").lower()
    for theme, keys in THEMES:
        if any(keyword in low for keyword in keys):
            return theme
    return "macro"


def pick_snappy(theme, seed):
    options = SNAPPY_HEADLINES.get(theme, ["Key development"])
    index = sum(ord(char) for char in str(seed or "")) % len(options)
    return options[index]


def build_one_liner(theme, what_happened):
    core = one_sentence(clean_substance(what_happened))
    if not core:
        core = "Reporting indicates a meaningful development in Venezuela with immediate operational implications."
    so_what = SO_WHAT.get(theme, "this affects risk and opportunity in Venezuela")
    line = f"{core.rstrip('.').rstrip(';')}; {so_what}."
    line = line.replace("—", "-")
    return line[:320].rstrip("; ").rstrip(".") + "."


def flatten_news_items(latest):
    direct = latest.get("items") or latest.get("allItems")
    if isinstance(direct, list) and direct:
        return direct
    flat = []
    for sector in latest.get("sectors") or []:
        for item in sector.get("items") or []:
            if "sector" not in item and sector.get("name"):
                item = dict(item)
                item["sector"] = sector.get("name")
            flat.append(item)
    return flat


def choose_candidates(items, limit=30):
    scored = []
    for item in items:
        title = clean_title(item.get("title") or "")
        if len(title) < 20 or is_noisy(title):
            continue

        text = substance(item)
        if len(text) < 60:
            text = title
        if is_noisy(text):
            continue

        published = item.get("sourcePublishedAt") or item.get("publishedAt") or item.get("dateISO") or ""
        recency = 2 if (isinstance(published, str) and published.startswith("2026-")) else 0
        tier = str(item.get("sourceTier") or item.get("tier") or "").lower()
        tier_boost = 60 if "1" in tier else (30 if "2" in tier else 0)
        quality = len(text) + (200 * recency) + tier_boost
        scored.append((quality, item))
    scored.sort(key=lambda row: row[0], reverse=True)
    return [item for _, item in scored[:limit]]


def valid_source_links(item):
    links = []
    primary = str(item.get("url") or "").strip()
    if primary.startswith("http"):
        links.append({"title": norm(item.get("title") or "Source"), "url": primary})
    source_url = str(item.get("source_url") or "").strip()
    if source_url.startswith("http") and source_url != primary:
        links.append({"title": "Source page", "url": source_url})
    return links[:2]


def load_pdf_items():
    for path in PDF_CANDIDATES:
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            publications = payload.get("publications") or []
            if publications:
                return publications
        except Exception:
            continue
    return []


def extract_markers(item):
    markers = []
    for value in item.get("event_types") or []:
        normalized = norm(value)
        if normalized:
            markers.append(normalized.lower())
    for value in item.get("tags") or []:
        normalized = norm(value)
        if normalized:
            markers.append(normalized.lower())
    return markers


def phrase_for_theme(theme, markers):
    preferred = [marker for marker in markers if len(marker) > 3][:2]
    if preferred:
        return " and ".join(preferred)

    defaults = {
        "sanctions": "licensing and compliance conditions",
        "politics": "stakeholder alignment and policy signaling",
        "macro": "inflation and growth expectations",
        "energy": "oil flows and contract execution",
        "humanitarian": "service delivery and population pressure",
        "investment": "financing and procurement pathways",
    }
    return defaults.get(theme, "policy and market conditions")


def build_theme_sentence(theme, items):
    count = len(items)
    sectors = []
    markers = []
    latest_date = ""
    for item in items:
        sector = norm(item.get("sector") or "")
        if sector and sector not in sectors:
            sectors.append(sector)
        markers.extend(extract_markers(item))
        date_value = str(item.get("sourcePublishedAt") or item.get("publishedAt") or item.get("dateISO") or "")
        if date_value and date_value > latest_date:
            latest_date = date_value

    sector_phrase = ", ".join(sectors[:2]) if sectors else "priority sectors"
    what_phrase = phrase_for_theme(theme, markers)
    date_phrase = f" through {latest_date}" if latest_date else " in the latest cycle"
    sentence = (
        f"Recent reporting across {count} items in {sector_phrase} points to shifts in {what_phrase}{date_phrase}; "
        f"{SO_WHAT.get(theme, 'this affects risk and opportunity in Venezuela')}"
    )
    return sentence.rstrip(". ") + "."


def build_rows(news_items, pdf_items, min_rows=4, max_rows=6):
    rows = []
    grouped = {}
    ordered_themes = []
    for item in choose_candidates(news_items, limit=40):
        text = f"{clean_title(item.get('title') or '')} {substance(item)}"
        theme = pick_theme(text)
        if theme not in grouped:
            grouped[theme] = []
            ordered_themes.append(theme)
        grouped[theme].append(item)

    for theme in ordered_themes:
        items = grouped.get(theme) or []
        if not items:
            continue
        top = items[0]
        seed = clean_title(top.get("title") or top.get("sector") or theme)
        links = []
        for candidate in items:
            links.extend(valid_source_links(candidate))
            dedup = []
            seen = set()
            for link in links:
                url = link.get("url") or ""
                if url and url not in seen:
                    dedup.append(link)
                    seen.add(url)
                if len(dedup) >= 2:
                    break
            links = dedup
            if len(links) >= 2:
                break

        rows.append(
            {
                "theme": theme,
                "subheading": pick_snappy(theme, seed),
                "sentence": build_theme_sentence(theme, items),
                "sources": links,
            }
        )
        if len(rows) >= max_rows - 1:
            break

    if pdf_items:
        publication = pdf_items[0]
        abstract = clean_substance(publication.get("abstract") or "")
        if abstract:
            core = one_sentence(abstract)
        else:
            core = "A new open-access Venezuela publication adds detailed evidence beyond daily reporting."

        publication_url = str(publication.get("pageUrl") or publication.get("url") or "").strip()
        links = []
        if publication_url.startswith("http"):
            links.append({"title": norm(publication.get("title") or "PDF publication"), "url": publication_url})

        rows.append(
            {
                "theme": "macro",
                "subheading": "Deep-dive evidence update",
                "sentence": f"{core.rstrip('.').rstrip(';')}; it provides a stronger basis for decisions than headline-driven signals.",
                "sources": links[:1],
            }
        )
    else:
        rows.append(
            {
                "theme": "macro",
                "subheading": "Deep-dive evidence update",
                "sentence": "No new open-access Venezuela publication was confirmed in this cycle; this keeps uncertainty higher for medium-term planning than when fresh deep-dive evidence is available.",
                "sources": [],
            }
        )

    unique_rows = []
    seen_sentences = set()
    for row in rows:
        sentence = norm(row.get("sentence") or "")
        if not sentence or sentence in seen_sentences:
            continue
        row["subheading"] = norm(row.get("subheading") or "Key development")
        row["sentence"] = sentence.replace("—", "-")
        row["sources"] = [s for s in row.get("sources") or [] if str(s.get("url") or "").startswith("http")][:2]
        unique_rows.append(row)
        seen_sentences.add(sentence)

    if len(unique_rows) < min_rows:
        fallback = {
            "theme": "macro",
            "subheading": "Monitoring baseline",
            "sentence": "Current reporting still indicates moving policy and market conditions; decision-makers should use this brief as directional until additional high-confidence sources arrive.",
            "sources": [],
        }
        while len(unique_rows) < min_rows:
            unique_rows.append(dict(fallback))

    return unique_rows[:max_rows]


def main():
    with open(LATEST_JSON, "r", encoding="utf-8") as handle:
        latest = json.load(handle)

    news_items = flatten_news_items(latest)
    pdf_items = load_pdf_items()
    rows = build_rows(news_items, pdf_items, min_rows=4, max_rows=6)

    output = {
        "asOf": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        "title": "Executive Rapid Brief",
        "rows": rows,
    }

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as handle:
        json.dump(output, handle, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()