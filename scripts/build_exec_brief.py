import datetime
import json
import os
import re

LATEST_JSON = "docs/data/latest.json"
PUBS_JSON = "docs/data/pdf_publications_recent.json"
OUT_JSON = "docs/data/exec_brief.json"

NOISE_PATTERNS = [
    r"The publication adds implementation detail that clarifies pace, constraints, and expected counterpart response\.?",
    r"See All Newsletters.*$",
    r"AP QUIZZES.*$",
    r"Test Your News I\.Q.*$",
    r"The Afternoon Wire.*$",
    r"Anthropic.*$",
]


def norm(text):
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    cleaned = cleaned.replace("—", "-")
    return cleaned


def clean_substance_text(text):
    cleaned = norm(text)
    for pattern in NOISE_PATTERNS:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .;")
    return cleaned


def split_sentences(text):
    parts = re.split(r"(?<=[\.\!\?])\s+(?=[A-ZÁÉÍÓÚÑ])", norm(text))
    return [segment.strip() for segment in parts if segment.strip()]


def cap_sentences(text, max_sentences=2):
    sentences = [segment for segment in split_sentences(clean_substance_text(text)) if len(segment) > 30]
    if not sentences:
        return ""
    return " ".join(sentences[:max_sentences])


def flatten_news_items(latest):
    direct = latest.get("items") or latest.get("allItems")
    if isinstance(direct, list) and direct:
        return direct
    sectors = latest.get("sectors") or []
    flat = []
    for sector in sectors:
        for item in sector.get("items") or []:
            flat.append(item)
    return flat


def choose_top_news(items, n=12):
    def score(item):
        points = 0
        tier = str(item.get("tier") or item.get("sourceTier") or "").upper()
        if "T1" in tier or "TIER 1" in tier:
            points += 6
        elif "T2" in tier or "TIER 2" in tier:
            points += 4
        elif "T3" in tier or "TIER 3" in tier:
            points += 2

        if item.get("preview"):
            points += 2
        if item.get("insight2"):
            points += 2
        if item.get("materiality") is not None:
            try:
                points += min(3, max(0, int(item.get("materiality"))))
            except Exception:
                pass

        date_str = str(item.get("sourcePublishedAt") or item.get("publishedAt") or item.get("dateISO") or "")
        if date_str.startswith("2026-"):
            points += 2
        elif date_str.startswith("2025-"):
            points += 1
        return points

    ranked = sorted(items, key=score, reverse=True)
    return ranked[:n]


def substance(item):
    insight = item.get("insight2")
    if isinstance(insight, dict):
        merged = " ".join([insight.get("s1", "")])
        compact = cap_sentences(merged, max_sentences=2)
        if compact:
            return compact
    return cap_sentences(item.get("preview") or item.get("description") or item.get("snippet") or "", max_sentences=2)


def first_sentence(text):
    sentences = split_sentences(clean_substance_text(text))
    if not sentences:
        return ""
    sentence = sentences[0]
    if len(sentence) > 210:
        sentence = sentence[:207].rsplit(" ", 1)[0].strip() + "..."
    return norm(sentence)


def quality_sentence(text):
    sentence = first_sentence(text)
    if len(sentence) < 60:
        return ""
    low = sentence.lower()
    blocked = ["newsletter", "quiz", "caught up", "see all"]
    if any(token in low for token in blocked):
        return ""
    return sentence


def build_news_bullets(latest, news_items):
    starters = [
        "Political and regulatory signals:",
        "Economic and market movement:",
        "Humanitarian and social conditions:",
        "Energy and extractives developments:",
    ]

    sectors = latest.get("sectors") or []
    sector_bullets = []
    for sector in sectors:
        sector_name = norm(sector.get("name") or "")
        if not sector_name:
            continue

        synth = sector.get("synth") or {}
        synth_bullets = [quality_sentence(value) for value in (synth.get("bullets") or [])]
        synth_bullets = [value for value in synth_bullets if value]

        item_summaries = []
        for item in (sector.get("items") or [])[:6]:
            candidate = quality_sentence(substance(item))
            if candidate:
                item_summaries.append(candidate)

        merged = []
        for candidate in synth_bullets + item_summaries:
            if candidate and candidate not in merged:
                merged.append(candidate)
            if len(merged) >= 2:
                break

        drivers = [norm(value).lower() for value in (synth.get("drivers") or []) if norm(value)]
        if drivers:
            lead = f"{sector_name} reporting keeps {', '.join(drivers[:2])} in focus for near-term decisions."
        else:
            lead = f"{sector_name} reporting points to meaningful shifts that require close monitoring in the coming days."

        if merged:
            if len(merged) > 1:
                body = f"{merged[0]} {merged[1]}"
            else:
                body = merged[0]
            sector_bullets.append(norm(f"{lead} {body}"))

        if len(sector_bullets) >= 4:
            break

    if not sector_bullets:
        pool = [quality_sentence(substance(item)) for item in news_items]
        pool = [entry for entry in pool if entry]
    else:
        pool = []

    if not sector_bullets and not pool:
        return [
            "Political and regulatory signals: Coverage remains thin in the current cycle, so directional interpretation should stay conservative until fresh source reporting arrives."
        ]

    if sector_bullets:
        return sector_bullets[:4]

    fallback = []
    for idx, candidate in enumerate(pool[:4]):
        fallback.append(norm(f"{starters[idx % len(starters)]} {candidate}"))
    return fallback[:4]


def build_publication_bullet(publications):
    if publications:
        top = publications[0]
        abstract = cap_sentences(top.get("abstract") or "", max_sentences=2)
        if abstract:
            return norm(f"Deep-dive research to anchor decisions: {abstract}")
        title = norm(top.get("title") or "A newly surfaced Venezuela-focused publication")
        return norm(
            f"Deep-dive research to anchor decisions: {title} adds analytical depth that complements daily reporting and sharpens medium-term scenario assumptions."
        )
    return (
        "Deep-dive research to anchor decisions: No new open-access Venezuela-focused PDF publication was detected in the current source set."
    )


def build_exec_bullets(news_items, publications):
    with open(LATEST_JSON, "r", encoding="utf-8") as handle:
        latest = json.load(handle)
    bullets = build_news_bullets(latest, news_items)
    bullets.append(build_publication_bullet(publications))
    final = [norm(bullet) for bullet in bullets if norm(bullet)]
    return final[:5]


def main():
    with open(LATEST_JSON, "r", encoding="utf-8") as handle:
        latest = json.load(handle)

    items = flatten_news_items(latest)

    publications = []
    if os.path.exists(PUBS_JSON):
        try:
            with open(PUBS_JSON, "r", encoding="utf-8") as handle:
                pubs_data = json.load(handle)
            publications = pubs_data.get("publications") or []
        except Exception:
            publications = []

    top_news = choose_top_news(items, n=12)
    bullets = build_exec_bullets(top_news, publications)

    output = {
        "asOf": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        "title": "Executive Rapid Brief",
        "bullets": bullets,
    }

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as handle:
        json.dump(output, handle, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()