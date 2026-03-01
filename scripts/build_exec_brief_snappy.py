import json, os, re, datetime
from urllib.parse import urlparse

LATEST_JSON = "docs/data/latest.json"
PDF_JSON    = "docs/data/pdf_publications_2026.json"
OUT_JSON    = "docs/data/exec_brief.json"
PDF_FALLBACKS = [
  "docs/data/pdf_publications_recent.json",
  "docs/data/pdf_publications_2025_2026.json",
]

def norm(s):
  return re.sub(r"\s+", " ", (s or "")).strip()

def host(url):
  try: return urlparse(url).netloc.lower()
  except: return ""

def split_sentences(s):
  parts = re.split(r"(?<=[\.\!\?])\s+(?=[A-ZÁÉÍÓÚÑ0-9])", (s or "").strip())
  return [p.strip() for p in parts if p.strip()]

def clean_text_block(text):
  t = norm(text)
  t = re.sub(r"https?://\S+", "", t)
  t = re.sub(r"\b(URL Source|Published Time|Markdown Content)\b:?", "", t, flags=re.I)
  t = re.sub(r"The publication adds implementation detail that clarifies pace, constraints, and expected counterpart response\.?", "", t, flags=re.I)
  t = re.sub(r"The Afternoon Wire.*$", "", t, flags=re.I)
  t = re.sub(r"See All Newsletters.*$", "", t, flags=re.I)
  t = re.sub(r"AP QUIZZES.*$", "", t, flags=re.I)
  t = re.sub(r"Test Your News I\.Q.*$", "", t, flags=re.I)
  t = re.sub(r"\*\s*\[[^\]]+\]\([^\)]+\)", "", t)
  t = re.sub(r"=+", " ", t)
  t = re.sub(r"\s+", " ", t).strip(" .;:-")
  return t

def substance(item):
  i2 = item.get("insight2")
  if isinstance(i2, dict):
    cand = clean_text_block((i2.get("s1","") + " " + i2.get("s2","")).strip())
    if cand: return cand
  return clean_text_block(item.get("preview") or item.get("description") or item.get("snippet") or "")

def get_list(item, key):
  v = item.get(key)
  if isinstance(v, list): return [norm(x) for x in v if norm(x)]
  return []

def pick_entities(item):
  # Prefer explicit entities list; fallback to tags/categories
  ents = get_list(item, "entities")
  if ents: return ents[:3]
  tags = get_list(item, "eventTypes") + get_list(item, "tags") + get_list(item, "categories")
  # Clean noisy tags
  bad = {"sanctions","energy","security","positive","neutral","negative"}
  tags = [t for t in tags if t.lower() not in bad]
  return tags[:3]

def pick_event_tags(item):
  ev = get_list(item, "eventTypes")
  if ev: return ev[:2]
  # Heuristic fallback: use a few tags
  return get_list(item, "tags")[:2]

def choose_best_sentence(text):
  # Choose the first sentence that looks like "something happened"
  # Reject meta phrases and boilerplate
  rejects = [
    "recent reporting", "this cycle", "comprehensive up-to-date news coverage",
    "why this matters", "retrieved", "tier", "copy citation"
  ]
  for s in split_sentences(norm(text)):
    low = s.lower()
    if any(r in low for r in rejects):
      continue
    # Prefer sentences with action verbs / change signals
    if re.search(r"\b(approved|announced|signed|suspended|launched|met|agreed|allowed|imposed|lifted|expanded|cut|raised|reform|review|invest|sanction|license|export|production|inflation|debt|election)\b", low):
      return s
  # Fallback: first non-trivial sentence
  for s in split_sentences(norm(text)):
    if len(s) >= 55:
      return s
  return clean_text_block(text)[:220]

def is_noisy_sentence(s):
  low = (s or "").lower()
  bad = [
    "january", "february", "march", "url source", "published time", "markdown content",
    "the publication reports movement", "(...)", "afternoon wire", "newsletter", "ap quizzes"
  ]
  if any(x in low for x in bad):
    return True
  if len(s or "") < 45:
    return True
  return False

def infer_theme(item, text):
  t = (text or "").lower()
  # Use event tags if present
  ev = " ".join([e.lower() for e in pick_event_tags(item)])
  t = t + " " + ev

  if any(k in t for k in ["sanction","ofac","license","compliance","designation"]): return "sanctions"
  if any(k in t for k in ["election","opposition","government","assembly","cne","decree","law","regulation"]): return "governance"
  if any(k in t for k in ["inflation","fx","exchange","debt","bond","reserve","gdp","budget"]): return "macro"
  if any(k in t for k in ["oil","pdvsa","export","refinery","crude","opec","gas","hydrocarbon"]): return "energy"
  if any(k in t for k in ["food","health","water","dengue","migration","school","poverty","humanitarian"]): return "social"
  if any(k in t for k in ["tender","procurement","rfp","rfi","rfq","grant","eoi","bid"]): return "opportunity"
  return "macro"

SNAPPY = {
  "sanctions":   ["Sanctions posture shifts", "Compliance risk re-prices", "Licensing signals move"],
  "governance":  ["Policy direction changes", "Regulatory signal spikes", "Governance calculus shifts"],
  "macro":       ["Macro conditions move", "Fiscal reality shows", "FX and inflation pressures change"],
  "energy":      ["Oil flows change", "PDVSA signal shifts", "Energy policy resets"],
  "social":      ["Social pressure points", "Humanitarian needs intensify", "Services and livelihoods shift"],
  "opportunity": ["Live funding or tender", "Procurement window opens", "New bid pipeline emerges"]
}

SO_WHAT = {
  "sanctions":   "This changes counterparty risk and deal feasibility.",
  "governance":  "This affects policy continuity and implementation risk.",
  "macro":       "This shifts operating assumptions for pricing and stability.",
  "energy":      "This influences export revenue and contract risk.",
  "social":      "This shapes humanitarian priorities and delivery constraints.",
  "opportunity": "This creates near-term BD entry points if requirements match."
}

def pick_snappy(theme, seed):
  opts = SNAPPY.get(theme, ["Key development"])
  idx = sum(ord(c) for c in (seed or "")) % len(opts)
  return opts[idx]

def build_one_sentence(item):
  text = substance(item)
  best = clean_text_block(choose_best_sentence(text))
  title_fallback = clean_text_block(item.get("title") or "")
  if is_noisy_sentence(best) and len(title_fallback) >= 25:
    best = title_fallback
  if is_noisy_sentence(best):
    best = "A material development was reported with immediate policy and market implications"

  theme = infer_theme(item, f"{title_fallback} {best}")
  ents = pick_entities(item)
  evs = pick_event_tags(item)

  # Build a one-sentence brief:
  # Actor/context (entities) + what happened (best sentence) + so-what.
  ctx = ""
  if ents:
    ctx = f"{', '.join(ents[:2])}: "
  elif evs:
    ctx = f"{', '.join(evs[:2])}: "

  core = best.rstrip(".")
  if len(core) > 190:
    core = core[:188].rsplit(" ", 1)[0].rstrip(".,;: ")
  so  = SO_WHAT.get(theme, "This matters for risk and opportunity in Venezuela.")

  # Keep it ONE sentence using a semicolon
  out = f"{ctx}{core}; {so}"
  out = norm(out)

  # Cap length for executive scanning
  if len(out) > 320:
    out = out[:318].rstrip("; ").rstrip(".") + "."
  else:
    if not out.endswith("."): out += "."
  return theme, out

def choose_items(items, limit=60):
  # Prefer items with substance and recency; avoid thin items
  scored = []
  for it in items:
    sub = substance(it)
    if len(sub) < 80:
      continue
    dt = it.get("publishedAt") or it.get("dateISO") or ""
    rec = 2 if isinstance(dt, str) and dt.startswith("2026-") else 0
    score = len(sub) + (200 * rec)
    scored.append((score, it))
  scored.sort(key=lambda x: x[0], reverse=True)
  return [it for _, it in scored[:limit]]

def extract_items(latest):
  direct = latest.get("items") or latest.get("allItems") or []
  if direct:
    return direct
  out = []
  for sec in latest.get("sectors") or []:
    sec_name = sec.get("name") or ""
    for it in sec.get("items") or []:
      if sec_name and not it.get("sector"):
        it = dict(it)
        it["sector"] = sec_name
      out.append(it)
  return out

def load_pdfs():
  candidates = [PDF_JSON] + PDF_FALLBACKS
  for path in candidates:
    if not os.path.exists(path):
      continue
    try:
      with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
      pubs = payload.get("publications") or []
      if pubs:
        return pubs
    except:
      continue
  return []

def build_rows(items, pdfs, max_rows=6):
  rows = []
  used_themes = set()

  for it in choose_items(items, limit=60):
    theme, sentence = build_one_sentence(it)
    # Force variety
    if theme in used_themes and len(used_themes) < 5:
      continue

    subheading = pick_snappy(theme, it.get("title") or sentence[:80])
    rows.append({
      "subheading": subheading,
      "sentence": sentence,
      "sources": [
        {"title": it.get("title",""), "url": it.get("url","")}
      ],
      "theme": theme,
      "publishedAt": it.get("publishedAt") or it.get("dateISO") or ""
    })
    used_themes.add(theme)
    if len(rows) >= max_rows - 1:
      break

  # Add one deep-dive anchor row if present
  if pdfs:
    p = pdfs[0]
    abs_txt = norm(p.get("abstract") or "")
    core = choose_best_sentence(abs_txt) if abs_txt else "A new open-access Venezuela publication adds detailed evidence beyond daily reporting"
    sentence = f"Deep-dive evidence: {core.rstrip('.')}; it strengthens decision quality beyond daily headlines."
    rows.append({
      "subheading": "Deep-dive evidence update",
      "sentence": norm(sentence if sentence.endswith(".") else sentence + "."),
      "sources": [{"title": p.get("title","PDF publication"), "url": p.get("url","")}],
      "theme": "research",
      "publishedAt": p.get("publishedAt") or "2026"
    })

  # Add a second source link when you have it: pick a second item in same theme
  # Optional: enrich later; keep simple now.
  return rows[:max_rows]

def main():
  with open(LATEST_JSON, "r", encoding="utf-8") as f:
    latest = json.load(f)
  items = extract_items(latest)
  pdfs = load_pdfs()

  rows = build_rows(items, pdfs, max_rows=6)

  out = {
    "asOf": datetime.datetime.utcnow().isoformat() + "Z",
    "title": "Executive Rapid Brief",
    "rows": rows
  }

  os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
  with open(OUT_JSON, "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
  main()
