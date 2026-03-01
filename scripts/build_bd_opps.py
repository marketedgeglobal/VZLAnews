import datetime
import json
import os
import re
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import feedparser

LATEST_JSON = "docs/data/latest.json"
FEEDS_PATH = "feeds.txt"
OUT_JSON = "docs/data/bd_opps.json"

BD_FEED_URLS = [
    "https://news.google.com/rss/search?q=Venezuela+rfp+OR+request+for+proposal+OR+tender+OR+procurement",
    "https://news.google.com/rss/search?q=Venezuela+expression+of+interest+OR+EOI+OR+terms+of+reference+OR+ToR",
    "https://news.google.com/rss/search?q=Venezuela+grant+funding+opportunity+call+for+proposals",
    "https://news.google.com/rss/search?q=site:devbusiness.un.org+Venezuela+tender+OR+procurement",
    "https://news.google.com/rss/search?q=site:ungm.org+Venezuela+tender+OR+procurement",
    "https://news.google.com/rss/search?q=site:reliefweb.int+Venezuela+" +
    "\"call for proposals\"+OR+\"expression of interest\"+OR+tender",
    "https://www.bing.com/news/search?q=venezuela+rfp+request+for+proposal+tender+procurement&format=rss",
    "https://www.bing.com/news/search?q=site:devbusiness.un.org+venezuela+tender+procurement&format=rss",
    "https://www.bing.com/news/search?q=site:ungm.org+venezuela+tender+procurement&format=rss",
    "https://www.bing.com/news/search?q=venezuela+grant+funding+opportunity+call+for+proposals&format=rss",
    "https://news.google.com/rss/search?q=site:undp.org+venezuela+procurement+OR+tender+OR+expression+of+interest",
    "https://news.google.com/rss/search?q=site:unicef.org+venezuela+procurement+OR+tender+OR+rfp",
    "https://news.google.com/rss/search?q=site:paho.org+venezuela+procurement+OR+tender+OR+call+for+proposals",
    "https://news.google.com/rss/search?q=site:iadb.org+venezuela+procurement+OR+tender+OR+expression+of+interest",
    "https://news.google.com/rss/search?q=site:worldbank.org+venezuela+procurement+OR+tender+OR+request+for+proposals",
    "https://news.google.com/rss/search?q=site:caf.com+venezuela+tender+OR+procurement+OR+call+for+proposals",
    "https://news.google.com/rss/search?q=site:dgmarket.com+venezuela+tender+OR+procurement",
    "https://news.google.com/rss/search?q=site:tendersinfo.com+venezuela+tender+OR+procurement",
    "https://www.bing.com/news/search?q=site:undp.org+venezuela+procurement+tender+rfp&format=rss",
    "https://www.bing.com/news/search?q=site:worldbank.org+venezuela+procurement+tender+request+for+proposals&format=rss",
    "https://news.google.com/rss/search?q=Venezuela+licitaci%C3%B3n+OR+convocatoria+OR+expresi%C3%B3n+de+inter%C3%A9s",
    "https://www.bing.com/news/search?q=venezuela+licitacion+convocatoria+expresion+de+interes&format=rss",
    "https://www2.fundsforngos.org/category/cfp/feed/",
    "https://www2.fundsforngos.org/category/grants-and-resources/feed/",
]

OPP_TERMS = [
    "request for proposal", "rfp", "request for information", "rfi",
    "request for quotation", "rfq", "tender", "procurement", "invitation to bid", "itb",
    "call for proposals", "call for applications", "open call",
    "expression of interest", "eoi", "terms of reference", "tor",
    "grant", "funding opportunity", "services required", "consultancy", "consultant",
    "request for proposals", "requests for proposals", "call for expression of interest",
    "bid", "bidding", "solicitation", "vendor registration", "supplier registration",
    "licitación", "licitacion", "convocatoria", "convocatoria abierta",
    "expresión de interés", "expresion de interes", "términos de referencia", "terminos de referencia",
    "adquisición", "adquisicion", "contratación", "contratacion", "concurso", "subvención", "subvencion",
]

EXCLUDE_TERMS = ["opinion", "commentary", "podcast", "video", "newsletter", "profile", "interview"]

VZLA_TERMS = [
    "venezuela", "venezuelan", "venezolana", "venezolano", "caracas", "pdvsa",
    "bolivar", "bolívar", "maracaibo", "orinoco", "zulia", "guyana essequibo"
]

ACRONYM_TERMS = {"rfp", "rfi", "rfq", "itb", "eoi", "tor", "cfp", "rfqs", "eois"}

OPPORTUNITY_DOMAINS = {
    "fundsforngos.org",
    "devbusiness.un.org",
    "ungm.org",
    "dgmarket.com",
    "tendersinfo.com",
    "reliefweb.int",
}

TITLE_URL_OPP_TERMS = [
    "request for proposal", "rfp", "request for information", "rfi",
    "request for quotation", "rfq", "invitation to bid", "itb",
    "expression of interest", "eoi", "terms of reference", "tor",
    "tender", "procurement", "call for proposals", "call for applications",
    "open call", "funding opportunity", "consultancy", "consultant",
    "services required", "bid notice", "tender notice",
    "request for proposals", "requests for proposals", "call for expression of interest",
    "grant", "cfp", "solicitation", "bidding",
    "licitación", "licitacion", "convocatoria", "expresión de interés", "expresion de interes",
    "términos de referencia", "terminos de referencia", "adquisición", "adquisicion",
]

ACTION_TERMS = [
    "apply", "application", "submit", "submission", "eligibility", "bid documents",
    "evaluation criteria", "procurement notice", "tender notice", "deadline", "closing date",
    "request for proposal", "request for information", "request for quotation",
    "expression of interest", "terms of reference", "call for proposals", "call for applications",
    "request for proposals", "requests for proposals", "open call", "funding opportunity",
    "supplier registration", "vendor registration", "bid", "bidding", "solicitation",
    "postular", "postulación", "postulacion", "presentar oferta", "presentación", "presentacion",
    "fecha límite", "fecha limite", "licitación", "licitacion", "convocatoria",
]

DEADLINE_PATTERNS = [
    r"(deadline|due by|due|closing date|closes|submission deadline)\s*[:\-]?\s*(\w+\s+\d{1,2},\s+20\d{2})",
    r"(deadline|due by|due|closing date|closes|submission deadline)\s*[:\-]?\s*(20\d{2}-\d{2}-\d{2})",
    r"(fecha l[ií]mite|cierre|vence|hasta el)\s*[:\-]?\s*(\d{1,2}\s+de\s+\w+\s+de\s+20\d{2})",
    r"(fecha l[ií]mite|cierre|vence|hasta el)\s*[:\-]?\s*(20\d{2}-\d{2}-\d{2})"
]

AMOUNT_PATTERNS = [
    r"(\$|usd\s*)\s?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)(\s*(million|billion|m|bn))?",
    r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(usd|dollars)",
    r"(€)\s?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)(\s*(million|billion|m|bn))?"
]

BOILERPLATE = [
    "Comprehensive up-to-date news coverage, aggregated from sources all over the world by Google News",
    "Comprehensive up-to-date news coverage, aggregated from sources all over the world by Google News."
]

_MONTHS_ES = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def norm(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def contains_any(text: str, terms) -> bool:
    low = text.lower()
    for term in terms:
        token = term.lower().strip()
        if not token:
            continue
        if token in ACRONYM_TERMS:
            if re.search(rf"\b{re.escape(token)}\b", low):
                return True
        elif token in low:
            return True
    return False


def extract_deadline(text: str) -> str:
    for pattern in DEADLINE_PATTERNS:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return norm(match.group(2))
    return ""


def extract_amount(text: str) -> str:
    for pattern in AMOUNT_PATTERNS:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return norm(match.group(0))
    return ""


def _parse_deadline_date(raw: str) -> datetime.date | None:
    text = norm(raw)
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    es_match = re.match(r"^(\d{1,2})\s+de\s+([a-záéíóúñ]+)\s+de\s+(20\d{2})$", text.lower())
    if es_match:
        day = int(es_match.group(1))
        month_name = es_match.group(2)
        year = int(es_match.group(3))
        month = _MONTHS_ES.get(month_name)
        if month:
            try:
                return datetime.date(year, month, day)
            except ValueError:
                return None
    return None


def is_expired_deadline(raw_deadline: str, today: datetime.date) -> bool:
    parsed = _parse_deadline_date(raw_deadline)
    if parsed is None:
        return False
    return parsed < today


def guess_org(item: dict) -> str:
    publisher = norm(item.get("publisher") or "")
    if publisher:
        return publisher
    try:
        return urlparse(item.get("url", "")).netloc
    except Exception:
        return ""


def score_opp(text: str) -> int:
    low = text.lower()
    score = 0
    strong = [
        "request for proposal", "request for proposals",
        "request for information", "request for quotation",
        "rfp", "rfi", "rfq", "tender", "procurement", "itb", "tor",
        "expression of interest", "eoi", "grant", "funding opportunity",
        "call for proposals", "open call", "services required", "consultancy", "consultant",
    ]
    for term in strong:
        if term in ACRONYM_TERMS:
            if re.search(rf"\b{re.escape(term)}\b", low):
                score += 3
        elif term in low:
            score += 3
    if extract_deadline(text):
        score += 2
    if extract_amount(text):
        score += 2
    return score


def _looks_venezuela_focused(text: str) -> bool:
    return contains_any(text, VZLA_TERMS)


def _entry_date_iso(entry: dict) -> str:
    value = norm(entry.get("published") or entry.get("updated") or "")
    if not value:
        return ""
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%a, %d %b %Y %H:%M:%S %z"):
        try:
            dt = datetime.datetime.strptime(value, fmt)
            return dt.date().isoformat()
        except ValueError:
            continue
    try:
        dt = parsedate_to_datetime(value)
        return dt.date().isoformat()
    except Exception:
        return ""


def _parse_iso_date(value: str) -> datetime.date:
    text = norm(value)
    if not text:
        return datetime.date.min
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return parsedate_to_datetime(text).date()
    except Exception:
        return datetime.date.min


def split_sentences(value: str):
    parts = re.split(r"(?<=[\.\!\?])\s+(?=[A-ZÁÉÍÓÚÑ])", value.strip())
    return [part.strip() for part in parts if part.strip()]


def make_summary(item: dict, hay: str) -> str:
    org = guess_org(item)
    deadline = extract_deadline(hay)
    amount = extract_amount(hay)

    preview = norm(item.get("preview") or item.get("insight") or item.get("description") or item.get("snippet") or "")
    for boilerplate in BOILERPLATE:
        preview = preview.replace(boilerplate, "").strip()

    sentences = [sent for sent in split_sentences(preview) if len(sent) > 35]
    core = " ".join(sentences[:2]) if len(sentences) >= 2 else (sentences[0] if sentences else "")

    out = [f"Opportunity linked to Venezuela: {org} or its partners invite proposals, bids, or applications."]
    if core:
        out.append(core)

    if deadline or amount:
        tail = []
        if deadline:
            tail.append(f"Deadline: {deadline}")
        if amount:
            tail.append(f"Amount: {amount}")
        out.append("; ".join(tail) + ".")
    else:
        out.append("Open the link for eligibility, scope, and submission requirements.")

    return norm(" ".join(out[:3]))


def _items_from_latest(payload: dict) -> list[dict]:
    direct = payload.get("items")
    if isinstance(direct, list):
        return [it for it in direct if isinstance(it, dict)]

    all_items = payload.get("allItems")
    if isinstance(all_items, list):
        return [it for it in all_items if isinstance(it, dict)]

    sectors = payload.get("sectors")
    if isinstance(sectors, list):
        flattened = []
        for sector in sectors:
            if not isinstance(sector, dict):
                continue
            for item in sector.get("items") or []:
                if isinstance(item, dict):
                    flattened.append(item)
        return flattened
    return []


def _load_feed_urls(path: str = FEEDS_PATH) -> list[str]:
    if not os.path.exists(path):
        return []
    urls: list[str] = []
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
    return urls


def _bd_feed_urls(path: str = FEEDS_PATH) -> list[str]:
    urls: list[str] = []

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                low = line.lower()
                if not any(token in low for token in [
                    "rfp", "request for proposal", "tender", "procurement", "eoi",
                    "expression of interest", "grant", "funding", "call for proposals", "tor",
                ]):
                    continue
                if " - http" in line:
                    url = line.split(" - ", 1)[1].strip()
                else:
                    url = line
                if url.startswith("http"):
                    urls.append(url)

    urls.extend(BD_FEED_URLS)

    seen: set[str] = set()
    deduped: list[str] = []
    for url in urls:
        key = norm(url)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def _extract_feed_items(urls: list[str]) -> list[dict]:
    items: list[dict] = []
    for url in urls:
        try:
            feed = feedparser.parse(url, request_headers={"User-Agent": "VZLAnews/1.0"})
        except Exception:
            continue

        feed_title = norm((feed.get("feed") or {}).get("title", "")) if isinstance(feed, dict) else ""
        for entry in getattr(feed, "entries", []) or []:
            title = norm(entry.get("title", ""))
            link = norm(entry.get("link", ""))
            if not title or not link:
                continue

            summary = norm(entry.get("summary", "") or entry.get("description", ""))
            content_value = ""
            content = entry.get("content", []) or []
            if isinstance(content, list) and content:
                first = content[0] or {}
                if isinstance(first, dict):
                    content_value = norm(first.get("value", ""))

            tags = []
            raw_tags = entry.get("tags", []) or []
            if isinstance(raw_tags, list):
                for tag in raw_tags:
                    if isinstance(tag, dict):
                        term = norm(tag.get("term", ""))
                        if term:
                            tags.append(term)

            source = entry.get("source") or {}
            publisher = ""
            if isinstance(source, dict):
                publisher = norm(source.get("title", ""))
            if not publisher:
                publisher = feed_title or norm(urlparse(url).netloc)

            items.append(
                {
                    "id": norm(entry.get("id", "") or entry.get("guid", "") or link),
                    "title": title,
                    "url": link,
                    "publisher": publisher,
                    "publishedAt": _entry_date_iso(entry),
                    "preview": summary,
                    "description": summary,
                    "snippet": content_value,
                    "tags": tags,
                    "categories": tags,
                    "sector": "",
                }
            )
    return items


def main() -> None:
    items: list[dict] = []
    if os.path.exists(LATEST_JSON):
        with open(LATEST_JSON, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        items.extend(_items_from_latest(data))

    feed_urls = _bd_feed_urls(FEEDS_PATH)
    items.extend(_extract_feed_items(feed_urls))

    opportunities = []
    today = datetime.datetime.now(datetime.timezone.utc).date()
    recent_cutoff = today - datetime.timedelta(days=60)
    seen: set[str] = set()

    for item in items:
        title = norm(item.get("title"))
        url = norm(item.get("url"))
        hay = " ".join([
            title,
            norm(item.get("preview")),
            norm(item.get("insight2", {}).get("s1") if isinstance(item.get("insight2"), dict) else ""),
            norm(item.get("insight2", {}).get("s2") if isinstance(item.get("insight2"), dict) else ""),
            norm(item.get("description")),
            norm(item.get("snippet")),
            " ".join(item.get("tags") or []),
            " ".join(item.get("categories") or []),
        ])

        if not hay or len(hay) < 60:
            continue
        if not _looks_venezuela_focused(hay):
            continue

        title_url_text = f"{title} {url}"
        if not contains_any(title_url_text, TITLE_URL_OPP_TERMS):
            continue

        if not contains_any(hay, OPP_TERMS):
            continue

        deadline = extract_deadline(hay)
        amount = extract_amount(hay)
        if not (deadline or amount or contains_any(hay, ACTION_TERMS)):
            continue

        score = score_opp(hay)
        if contains_any(hay, EXCLUDE_TERMS) and score < 5:
            continue
        if score < 3:
            continue
        if is_expired_deadline(deadline, today):
            continue

        published_raw = str(item.get("publishedAt") or item.get("dateISO") or "")
        published_date = _parse_iso_date(published_raw)
        if published_date == datetime.date.min:
            continue
        if published_date < recent_cutoff:
            continue

        dedupe_key = f"{norm(item.get('url', '')).lower()}|{norm(item.get('title', '')).lower()}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        opportunities.append({
            "id": item.get("id"),
            "title": item.get("title"),
            "url": item.get("url"),
            "sector": item.get("sector"),
            "publisher": item.get("publisher") or "",
            "publishedAt": published_date.isoformat(),
            "deadline": deadline,
            "amount": amount,
            "score": score,
            "summary": make_summary(item, hay),
        })

    opportunities.sort(
        key=lambda opp: (
            _parse_iso_date(str(opp.get("publishedAt", "") or "")),
            int(opp.get("score", 0)),
            0 if norm(opp.get("deadline", "")) else 1,
            str(opp.get("deadline", "")),
        ),
        reverse=True,
    )

    top_opportunities = opportunities[:5]

    output = {
        "asOf": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "count": len(top_opportunities),
        "opportunities": top_opportunities,
    }

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(output, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
