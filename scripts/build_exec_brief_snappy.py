import datetime
import json
import os
import re

LATEST_JSON = "docs/data/latest.json"
PDF_JSON = "docs/data/pdf_publications_2026.json"
PDF_FALLBACKS = [
    "docs/data/pdf_publications_recent.json",
    "docs/data/pdf_publications_2025_2026.json",
]
OUT_JSON = "docs/data/exec_brief.json"

THEME_ORDER = ["sanctions", "governance", "macro", "energy", "social", "opportunity"]

SNAPPY = {
    "sanctions": [
        "Compliance risk just re-priced",
        "Sanctions posture shifted",
        "Licensing risk moved",
    ],
    "governance": [
        "Regulatory signal spiked",
        "Policy continuity risk rose",
        "Governance calculus shifted",
    ],
    "macro": [
        "Political volatility affects pricing assumptions",
        "Macro assumptions moved",
        "FX and inflation risk reset",
    ],
    "energy": [
        "Oil contracting and licensing environment shifted",
        "PDVSA contracting signal changed",
        "Energy execution risk moved",
    ],
    "social": [
        "Human rights and social exposure remains elevated",
        "Humanitarian pressure persists",
        "Delivery constraints increased",
    ],
    "opportunity": [
        "Live funding and tender signals moved",
        "Procurement window shifted",
        "Bid pipeline emerged",
    ],
}

SO_WHAT = {
    "sanctions": "This continues to shape counterparty risk and deal feasibility, especially for state-linked exposure and import channels.",
    "governance": "This raises policy discontinuity risk and weakens enforcement predictability for operators and partners.",
    "macro": "This can flow through to FX, inflation expectations, and near-term operating stability assumptions.",
    "energy": "Net effect is that licensing pathways and compliance screens now determine who can transact, at what margins, and with what execution risk.",
    "social": "This raises reputational, duty-of-care, and third-party screening requirements for any field footprint.",
    "opportunity": "This creates near-term entry points where requirements align, but qualification and partner diligence remain critical.",
}

NOISE_PATTERNS = [
    r"https?://\S+",
    r"\b(URL Source|Published Time|Markdown Content)\b:?",
    r"The publication adds implementation detail that clarifies pace, constraints, and expected counterpart response\.?",
    r"The Afternoon Wire.*$",
    r"See All Newsletters.*$",
    r"AP QUIZZES.*$",
    r"Test Your News I\.Q.*$",
    r"\*\s*\[[^\]]+\]\([^\)]+\)",
    r"=+",
]

SPANISH_MARKERS = [
    " de ", " la ", " el ", " en ", " para ", " con ", " una ", " un ", " y ",
    " transicion ", " venezuela ", " gobierno ", " politica ", " economica ",
]

ENGLISH_EVENT = {
    "sanctions": "Authorities and counterparties signaled a meaningful shift in sanctions-related operating conditions",
    "governance": "Political actors triggered an institutional change with direct policy execution implications",
    "macro": "Market and political signals shifted near-term assumptions for inflation, FX, and operating stability",
    "energy": "Energy-side actors signaled a contracting and licensing shift in oil market access",
    "social": "Humanitarian and rights-related signals point to sustained pressure on delivery conditions",
    "opportunity": "Commercial and funding signals point to near-term openings with higher qualification scrutiny",
}

SECTOR_REPHRASE = {
    "cross-cutting / policy / risk": "policy and institutional channels",
    "extractives & mining": "energy and extractives channels",
    "finance & investment": "capital and market channels",
    "food & agriculture": "food system channels",
    "health & water": "public service channels",
    "education & workforce": "labor and human capital channels",
}


def norm(text):
    return re.sub(r"\s+", " ", str(text or "")).strip()


def clean_text(text):
    out = norm(text).replace("—", "-")
    for pattern in NOISE_PATTERNS:
        out = re.sub(pattern, "", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+", " ", out).strip(" .;:-")
    return out


def is_noisy_text(text):
    low = (text or "").lower()
    noisy_tokens = [
        "january", "february", "march", "april", "published time", "markdown content",
        "the publication reports movement", "(...)", "afternoon wire", "newsletter",
        "findings are drawn from open-access material", "url source",
    ]
    if any(token in low for token in noisy_tokens):
        return True
    if len(text or "") < 45:
        return True
    return False


def is_likely_non_english(text):
    value = f" {clean_text(text).lower()} "
    if re.search(r"[áéíóúñ]", value):
        return True
    marker_hits = sum(1 for marker in SPANISH_MARKERS if marker in value)
    return marker_hits >= 3


def split_sentences(text):
    parts = re.split(r"(?<=[\.\!\?])\s+(?=[A-ZÁÉÍÓÚÑ0-9])", clean_text(text))
    return [part.strip() for part in parts if part.strip()]


def get_list(item, key):
    value = item.get(key)
    if isinstance(value, list):
        return [clean_text(v) for v in value if clean_text(v)]
    return []


def substance(item):
    insight = item.get("insight2")
    if isinstance(insight, dict):
        candidate = clean_text(f"{insight.get('s1', '')} {insight.get('s2', '')}")
        if len(candidate) >= 80:
            return candidate
    return clean_text(item.get("preview") or item.get("description") or item.get("snippet") or "")


def choose_best_sentence(text):
    rejects = [
        "recent reporting",
        "this cycle",
        "why this matters",
        "copy citation",
        "newsletter",
        "retrieved",
    ]
    action_regex = r"\b(approved|announced|signed|suspended|launched|met|agreed|allowed|imposed|lifted|expanded|cut|raised|reviewed|reformed|invested|sanctioned|licensed|exported|inflation|debt|election|resigned|released|resale|contract)\b"

    for sentence in split_sentences(text):
        low = sentence.lower()
        if any(term in low for term in rejects):
            continue
        if re.search(action_regex, low):
            return sentence

    for sentence in split_sentences(text):
        if len(sentence) >= 65:
            return sentence

    fallback = clean_text(text)
    return fallback[:210].rstrip(".,;: ")


def has_action_signal(text):
    return bool(re.search(r"\b(approved|announced|signed|suspended|launched|met|agreed|allowed|imposed|lifted|expanded|cut|raised|reviewed|reformed|invested|sanctioned|licensed|exported|inflation|debt|election|resigned|released|resale|contract|seeking|sold|rejected)\b", (text or "").lower()))


def looks_like_title_fragment(text):
    value = clean_text(text)
    if len(value) < 60:
        return False
    if has_action_signal(value):
        return False
    words = value.split()
    title_like = sum(1 for word in words if word[:1].isupper())
    if title_like >= max(6, len(words) // 2):
        return True
    if ":" in value and len(words) >= 10:
        return True
    return False


def clean_title(title):
    t = clean_text(title)
    t = re.sub(r"\s*\|\s*.*$", "", t)
    t = re.sub(r"\s*-\s*Reuters\s*$", "", t, flags=re.I)
    t = re.sub(r"\s*\(.*?\)\s*$", "", t)
    return clean_text(t)


def smooth_sector_phrase(sector):
    normalized = clean_text(sector).lower()
    if not normalized:
        return "priority operating channels"
    return SECTOR_REPHRASE.get(normalized, normalized)


def flatten_items(latest):
    direct = latest.get("items") or latest.get("allItems") or []
    if direct:
        return direct

    rows = []
    for sector in latest.get("sectors") or []:
        sector_name = sector.get("name") or ""
        for item in sector.get("items") or []:
            if sector_name and not item.get("sector"):
                item = dict(item)
                item["sector"] = sector_name
            rows.append(item)
    return rows


def pick_entities(item):
    entities = get_list(item, "entities")
    if entities:
        return entities[:2]
    return []


def infer_theme(item, text):
    corpus = f"{clean_text(item.get('title') or '')} {clean_text(text)}"
    corpus += " " + " ".join([v.lower() for v in (get_list(item, "eventTypes") + get_list(item, "event_types") + get_list(item, "tags"))])
    low = corpus.lower()

    if any(key in low for key in ["sanction", "ofac", "license", "compliance", "designation", "amnesty bill"]):
        return "sanctions"
    if any(key in low for key in ["election", "opposition", "government", "assembly", "cne", "decree", "law", "regulation", "resigned", "ombudsman"]):
        return "governance"
    if any(key in low for key in ["inflation", "fx", "exchange", "debt", "bond", "reserve", "gdp", "budget", "fragile economy"]):
        return "macro"
    if any(key in low for key in ["oil", "pdvsa", "export", "refinery", "crude", "opec", "gas", "hydrocarbon", "resale"]):
        return "energy"
    if any(key in low for key in ["food", "health", "water", "dengue", "migration", "school", "poverty", "humanitarian", "human rights"]):
        return "social"
    if any(key in low for key in ["tender", "procurement", "rfp", "rfi", "rfq", "grant", "eoi", "bid"]):
        return "opportunity"
    return "macro"


def choose_items(items, limit=80):
    scored = []
    for item in items:
        title = clean_text(item.get("title") or "")
        text = substance(item)
        if len(title) < 20 and len(text) < 90:
            continue
        date_str = item.get("sourcePublishedAt") or item.get("publishedAt") or item.get("dateISO") or ""
        recency = 2 if isinstance(date_str, str) and date_str.startswith("2026-") else 0
        tier = str(item.get("sourceTier") or item.get("tier") or "").lower()
        tier_boost = 60 if "1" in tier else (30 if "2" in tier else 0)
        quality = len(text) + len(title) + (200 * recency) + tier_boost
        scored.append((quality, item))
    scored.sort(key=lambda row: row[0], reverse=True)
    return [item for _, item in scored[:limit]]


def build_sentence(item, theme):
    title = clean_title(item.get("title") or "")
    core_text = choose_best_sentence(substance(item) or title)
    if (is_noisy_text(core_text) or len(core_text) < 60) and len(title) >= 30:
        core_text = title
    if is_noisy_text(core_text) or is_likely_non_english(core_text) or looks_like_title_fragment(core_text):
        sector = smooth_sector_phrase(item.get("sector") or "")
        date_str = clean_text(item.get("sourcePublishedAt") or item.get("publishedAt") or item.get("dateISO") or "")
        base = ENGLISH_EVENT.get(theme, "A material development was reported with direct implications for near-term operating conditions")
        if sector and date_str:
            core_text = f"{base} in {sector} through {date_str}"
        elif sector:
            core_text = f"{base} in {sector}"
        elif date_str:
            core_text = f"{base} in reporting through {date_str}"
        else:
            core_text = base

    entities = pick_entities(item)
    actor_prefix = f"{', '.join(entities)}: " if entities else ""

    happened = core_text.rstrip(". ")
    if len(happened) > 210:
        happened = happened[:208].rsplit(" ", 1)[0].rstrip(".,;: ")
    # Remove date suffix like " through YYYY-MM-DD" to streamline prose
    happened = re.sub(r"\s+through\s+\d{4}-\d{2}-\d{2}$", "", happened)

    so_what = SO_WHAT.get(theme, "This matters for risk and opportunity in Venezuela.")
    sentence = f"{actor_prefix}{happened}. {so_what}"
    sentence = clean_text(sentence)
    if not sentence.endswith("."):
        sentence += "."
    return sentence


def pick_snappy(theme, seed):
    options = SNAPPY.get(theme, ["Key development"])
    index = sum(ord(char) for char in str(seed or "")) % len(options)
    return options[index]


def load_pdfs():
    for path in [PDF_JSON] + PDF_FALLBACKS:
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


def build_rows(items, pdfs, max_rows=6):
    selected = choose_items(items, limit=80)
    grouped = {theme: [] for theme in THEME_ORDER}

    for item in selected:
        probe_text = f"{item.get('title', '')} {substance(item)}"
        theme = infer_theme(item, probe_text)
        grouped.setdefault(theme, []).append(item)

    rows = []
    for theme in THEME_ORDER:
        cluster = grouped.get(theme) or []
        if not cluster:
            continue
        top = cluster[0]
        sentence = build_sentence(top, theme)
        subheading = pick_snappy(theme, top.get("title") or sentence[:80])
        rows.append(
            {
                "subheading": subheading,
                "sentence": sentence,
                "theme": theme,
                "publishedAt": top.get("sourcePublishedAt") or top.get("publishedAt") or top.get("dateISO") or "",
            }
        )
        if len(rows) >= max_rows - 1:
            break

    if pdfs and len(rows) < max_rows:
        publication = pdfs[0]
        abstract = clean_text(publication.get("abstract") or "")
        core = choose_best_sentence(abstract) if abstract else "A new open-access Venezuela publication adds detailed evidence beyond daily reporting"
        if is_noisy_text(core):
            core = "New deep-dive research adds evidence beyond daily reporting"
        sentence = f"{core.rstrip('. ')}. It strengthens decision quality beyond headline-driven signals."
        rows.append(
            {
                "subheading": "Deep-dive evidence update",
                "sentence": clean_text(sentence) + ("" if clean_text(sentence).endswith(".") else "."),
                "theme": "research",
                "publishedAt": publication.get("publishedAt") or "",
            }
        )

    return rows[:max_rows]


def main():
    with open(LATEST_JSON, "r", encoding="utf-8") as handle:
        latest = json.load(handle)

    items = flatten_items(latest)
    pdfs = load_pdfs()
    rows = build_rows(items, pdfs, max_rows=6)

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
