import datetime
import json
import os
import re
from collections import Counter

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


def build_news_bullets(_latest, news_items):
    grouped = {}
    for item in news_items:
        sector = norm(item.get("sector") or "General")
        grouped.setdefault(sector, []).append(item)

    if not grouped:
        return [
            "Political and regulatory signals: Coverage remains thin in the current cycle, so interpretation should stay conservative until fresh reporting arrives."
        ]

    ranked_sectors = sorted(grouped.items(), key=lambda entry: len(entry[1]), reverse=True)
    bullets = []

    for sector_name, sector_items in ranked_sectors[:4]:
        theme_counts = Counter()
        latest_date = ""

        for item in sector_items:
            for theme in item.get("event_types") or []:
                normalized = norm(theme)
                if normalized:
                    theme_counts[normalized] += 2
            for tag in item.get("tags") or []:
                normalized = norm(tag)
                if normalized:
                    theme_counts[normalized] += 1

            date_str = str(item.get("sourcePublishedAt") or item.get("publishedAt") or item.get("dateISO") or "")
            if date_str and date_str > latest_date:
                latest_date = date_str

        top_themes = [theme.lower() for theme, _count in theme_counts.most_common(2)]
        if len(top_themes) == 0:
            top_themes = ["policy direction", "market conditions"]
        elif len(top_themes) == 1:
            top_themes.append("implementation timing")

        count = len(sector_items)
        count_label = "item" if count == 1 else "items"
        sentence1 = (
            f"{sector_name} coverage across {count} {count_label} points to combined pressure around {top_themes[0]} and {top_themes[1]}, "
            "with counterpart decisions likely to hinge on near-term policy execution."
        )
        if latest_date:
            sentence2 = (
                f"Signals remain active in reporting through {latest_date}, indicating the trend set is still developing rather than resolved."
            )
            bullets.append(norm(f"{sentence1} {sentence2}"))
        else:
            bullets.append(norm(sentence1))

    return bullets[:4]


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


def build_exec_bullets(latest, news_items, publications):
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
    bullets = build_exec_bullets(latest, top_news, publications)

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