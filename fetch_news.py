#!/usr/bin/env python3
"""Fetch latest news from Venezuela RSS feeds and update README.md."""

import feedparser
from datetime import datetime, timezone

FEEDS = [
    {
        "name": "El Nacional",
        "url": "https://el-nacional.com/feed/",
    },
    {
        "name": "Efecto Cocuyo",
        "url": "https://efectococuyo.com/feed/",
    },
    {
        "name": "El Pitazo",
        "url": "https://elpitazo.net/feed/",
    },
    {
        "name": "TalCual Digital",
        "url": "https://talcualdigital.com/feed/",
    },
    {
        "name": "Google News – Venezuela",
        "url": (
            "https://news.google.com/rss/search"
            "?q=Venezuela&hl=es-419&gl=VE&ceid=VE:es-419"
        ),
    },
]

MAX_ITEMS_PER_FEED = 5
README_PATH = "README.md"


def fetch_feed(feed_info: dict) -> list[dict]:
    """Parse a single RSS feed and return a list of article dicts."""
    parsed = feedparser.parse(feed_info["url"])
    articles = []
    for entry in parsed.entries[:MAX_ITEMS_PER_FEED]:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()
        published = entry.get("published", "")
        if title and link:
            articles.append(
                {
                    "title": title,
                    "link": link,
                    "published": published,
                    "source": feed_info["name"],
                }
            )
    return articles


def build_markdown(sections: dict[str, list[dict]]) -> str:
    """Build the README markdown content."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# 🇻🇪 VZLAnews – Latest News from Venezuela",
        "",
        f"> Last updated: **{now}**",
        "",
        "Automated digest (updated every 6 hours) of news and updates from Venezuela, "
        "aggregated from multiple sources.",
        "",
        "---",
        "",
    ]

    for source_name, articles in sections.items():
        if not articles:
            continue
        lines.append(f"## {source_name}")
        lines.append("")
        for article in articles:
            pub = f" — {article['published']}" if article["published"] else ""
            lines.append(f"- [{article['title']}]({article['link']}){pub}")
        lines.append("")

    lines += [
        "---",
        "",
        "*This report is generated automatically by "
        "[fetch_news.py](fetch_news.py) via "
        "[GitHub Actions](.github/workflows/update_news.yml).*",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    sections: dict[str, list[dict]] = {}
    for feed_info in FEEDS:
        print(f"Fetching {feed_info['name']} …")
        try:
            articles = fetch_feed(feed_info)
        except Exception as exc:  # noqa: BLE001
            import traceback
            print(
                f"  [WARN] Failed to fetch {feed_info['name']}: "
                f"{type(exc).__name__}: {exc}"
            )
            traceback.print_exc()
            articles = []
        sections[feed_info["name"]] = articles
        print(f"  Got {len(articles)} articles.")

    markdown = build_markdown(sections)
    with open(README_PATH, "w", encoding="utf-8") as fh:
        fh.write(markdown)
    print(f"\nREADME updated: {README_PATH}")


if __name__ == "__main__":
    main()
