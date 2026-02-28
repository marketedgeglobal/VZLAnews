import re

import requests
import trafilatura
from bs4 import BeautifulSoup
from readability import Document

UA = "Mozilla/5.0 (compatible; MarketEdgeVZLAnews/1.0; +https://marketedgeglobal.github.io/VZLAnews/)"

BOILERPLATE = {
    "Comprehensive up-to-date news coverage, aggregated from sources all over the world by Google News",
    "Comprehensive up-to-date news coverage, aggregated from sources all over the world by Google News.",
}

BYLINE_PATTERNS = [
    r"^\s*by\s+[a-z].{0,80}$",
    r"^\s*por\s+[a-záéíóúñ].{0,80}$",
    r"^\s*reuters\s*$",
    r"^\s*ap\s*$",
    r"^\s*afp\s*$",
]

DATELINE_PAT = r"^[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ\s\.\-]{2,40}\s+\([^\)]+\)\s+[-—]\s+"


def _clean_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _split_sentences(text: str) -> list[str]:
    normalized = _clean_spaces(text)
    if not normalized:
        return []
    parts = re.split(r"(?<=[\.\!\?])\s+(?=[A-ZÁÉÍÓÚÑ])", normalized)
    return [part.strip() for part in parts if part.strip()]


def _is_byline(paragraph: str) -> bool:
    text = _clean_spaces(paragraph)
    if not text:
        return False
    if re.search(DATELINE_PAT, text):
        return True
    low = text.lower()
    for pattern in BYLINE_PATTERNS:
        if re.match(pattern, low, flags=re.IGNORECASE):
            return True
    return False


def _looks_like_noise(paragraph: str) -> bool:
    low = paragraph.lower()
    if low.count("http") >= 2:
        return True
    if low.count(" |") >= 3 or low.count(" - ") >= 6:
        return True
    if "title:" in low or "url source:" in low or "markdown content:" in low:
        return True
    if "just a moment" in low or "performing security verification" in low:
        return True
    if "skip to content" in low:
        return True
    return any(
        marker in low
        for marker in [
            "cookies",
            "subscribe",
            "suscríb",
            "sign up",
            "iniciar sesión",
            "accept all",
            "privacy policy",
            "terms of use",
            "newsletter",
            "read more",
            "cookie policy",
            "all rights reserved",
            "reset password",
            "wrong login information",
            "security service to protect",
            "performing security verification",
        ]
    )


def _first_substantive_paragraph(paragraphs: list[str]) -> str:
    for paragraph in paragraphs:
        clean = _clean_spaces(paragraph)
        if not clean or len(clean) < 80:
            continue
        if clean in BOILERPLATE:
            continue
        if _is_byline(clean):
            continue
        if _looks_like_noise(clean):
            continue
        if len(_split_sentences(clean)) < 2:
            continue
        return clean
    return ""


def _extract_with_trafilatura(url: str) -> str:
    downloaded = trafilatura.fetch_url(url)
    if not downloaded:
        return ""
    text = trafilatura.extract(
        downloaded,
        output_format="txt",
        include_comments=False,
        include_tables=False,
    )
    return text or ""


def _extract_with_readability(url: str) -> str:
    response = requests.get(url, headers={"User-Agent": UA}, timeout=20)
    response.raise_for_status()
    doc = Document(response.text)
    html = doc.summary(html_partial=True)
    soup = BeautifulSoup(html, "lxml")
    return soup.get_text("\n")


def _extract_with_jina(url: str) -> str:
    response = requests.get(f"https://r.jina.ai/{url}", headers={"User-Agent": UA}, timeout=25)
    if response.status_code != 200:
        return ""
    return response.text or ""


def extract_preview(url: str) -> dict:
    full = ""
    source = "none"

    if not url:
        return {"preview": "", "preview_source": "none"}

    try:
        full = _extract_with_trafilatura(url)
        if full:
            source = "trafilatura"
    except Exception:
        full = ""

    if not full:
        try:
            full = _extract_with_readability(url)
            if full:
                source = "readability"
        except Exception:
            full = ""

    if not full:
        try:
            full = _extract_with_jina(url)
            if full:
                source = "jina"
        except Exception:
            full = ""

    full = full or ""
    if not full.strip():
        return {"preview": "", "preview_source": "none"}

    paragraphs = [p.strip() for p in re.split(r"\n{2,}|\r\n\r\n", full) if p.strip()]
    if len(paragraphs) < 2:
        paragraphs = [p.strip() for p in re.split(r"\n+", full) if p.strip()]

    paragraph = _first_substantive_paragraph(paragraphs)
    if not paragraph:
        fallback = _clean_spaces(full[:900])
        paragraph = fallback if not _looks_like_noise(fallback) else ""

    if not paragraph:
        return {"preview": "", "preview_source": "none"}

    sentences = _split_sentences(paragraph)
    if len(sentences) >= 3:
        preview = " ".join(sentences[:3])
    elif len(sentences) >= 2:
        preview = " ".join(sentences[:2])
    else:
        preview = paragraph

    preview = _clean_spaces(preview)
    for boilerplate in BOILERPLATE:
        preview = preview.replace(boilerplate, "").strip()

    if _looks_like_noise(preview):
        return {"preview": "", "preview_source": "none"}

    return {"preview": preview, "preview_source": source}
