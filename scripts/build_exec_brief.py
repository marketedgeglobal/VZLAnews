import datetime
import json
import os
import re

LATEST_JSON = "docs/data/latest.json"
PUBS_JSON = "docs/data/pdf_publications_recent.json"
OUT_JSON = "docs/data/exec_brief.json"


def norm(text):
    cleaned = re.sub(r"\s+", " ", str(text or "")).strip()
    cleaned = cleaned.replace("—", "-")
    return cleaned


def split_sentences(text):
    parts = re.split(r"(?<=[\.\!\?])\s+(?=[A-ZÁÉÍÓÚÑ])", norm(text))
    return [segment.strip() for segment in parts if segment.strip()]


def cap_sentences(text, max_sentences=2):
    sentences = [segment for segment in split_sentences(text) if len(segment) > 30]
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
        merged = " ".join([insight.get("s1", ""), insight.get("s2", "")])
        compact = cap_sentences(merged, max_sentences=2)
        if compact:
            return compact
    return cap_sentences(item.get("preview") or item.get("description") or item.get("snippet") or "", max_sentences=2)


def build_news_bullets(news_items):
    starters = [
        "Political and regulatory signals:",
        "Economic and market movement:",
        "Humanitarian and social conditions:",
        "Energy and extractives developments:",
    ]

    pool = []
    for item in news_items:
        text = substance(item)
        if text:
            pool.append({
                "sector": item.get("sector") or "General",
                "text": norm(text),
            })

    if not pool:
        return [
            "Political and regulatory signals: Coverage remains thin in the current cycle, so directional interpretation should stay conservative until fresh source reporting arrives."
        ]

    grouped = {}
    for entry in pool:
        grouped.setdefault(entry["sector"], []).append(entry["text"])

    preferred_sectors = sorted(grouped.keys(), key=lambda key: len(grouped[key]), reverse=True)
    bullets = []

    for index in range(4):
        if index >= len(preferred_sectors):
            break
        sector = preferred_sectors[index]
        snippets = grouped.get(sector, [])[:2]
        if not snippets:
            continue
        if len(snippets) == 1:
            sentence = snippets[0]
        else:
            sentence = f"{snippets[0]} {snippets[1]}"
        bullets.append(norm(f"{starters[index]} {sentence}"))

    if len(bullets) < 4:
        leftovers = [entry["text"] for entry in pool]
        used = set(" ".join(bullets).split())
        for candidate in leftovers:
            if len(bullets) >= 4:
                break
            if candidate and candidate.split()[0] not in used:
                lead = starters[len(bullets) % len(starters)]
                bullets.append(norm(f"{lead} {candidate}"))

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


def build_exec_bullets(news_items, publications):
    bullets = build_news_bullets(news_items)
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