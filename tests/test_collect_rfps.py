"""
Tests for scripts/collect_rfps.py
"""

import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import yaml

# Make the scripts package importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import collect_rfps as cr

# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

NOW = datetime(2026, 2, 20, 12, 0, 0, tzinfo=timezone.utc)


def make_entry(
    title="Test entry",
    link="https://example.com/article",
    summary="",
    published=None,
    source_url="https://example.com/rss",
    source_domain="example.com",
) -> dict:
    return {
        "title": title,
        "link": link,
        "summary": summary,
        "published": published,
        "source_url": source_url,
        "source_domain": source_domain,
    }


def minimal_cfg() -> dict:
    return {
        "country": {"name": "Venezuela"},
        "max_age_days": 7,
        "max_results": 35,
        "require_country_match": True,
        "country_terms": ["venezuela", "caracas"],
        "geo_context_terms": ["caribbean"],
        "exclude_terms": ["football", "soccer"],
        "business_signal_terms": ["tender", "procurement"],
        "sectors": {
            "extractives_mining": {
                "label": "Extractives & Mining",
                "include": ["oil", "gas", "pdvsa"],
                "opportunity": ["licensing", "tender"],
                "risk": ["spill", "sanctions"],
            },
            "food_agriculture": {
                "label": "Food & Agriculture",
                "include": ["food security", "agriculture"],
                "opportunity": ["distribution"],
                "risk": ["shortage"],
            },
        },
        "scoring": {
            "weights": {
                "country_match": 0.20,
                "sector_relevance": 0.30,
                "business_signals": 0.25,
                "recency": 0.15,
                "source_priority": 0.10,
            },
            "recency_decay": "linear",
            "multi_sector_bonus": 0.10,
        },
        "source_weights": {
            "worldbank.org": 1.4,
            "reliefweb.int": 1.4,
        },
        "deduplication": {
            "strategy": "title_url",
            "title_similarity_threshold": 0.90,
        },
        "brief_sections": [
            "Extractives & Mining",
            "Food & Agriculture",
            "Cross-cutting / Policy / Risk",
        ],
        "flags": {
            "opportunity_flag_terms": ["tender", "procurement"],
            "risk_flag_terms": ["sanctions", "outbreak"],
        },
    }


# ---------------------------------------------------------------------------
# load_feeds
# ---------------------------------------------------------------------------

class TestLoadFeeds:
    def test_plain_url(self, tmp_path):
        f = tmp_path / "feeds.txt"
        f.write_text("https://example.com/rss\n")
        assert cr.load_feeds(str(f)) == ["https://example.com/rss"]

    def test_label_url_format(self, tmp_path):
        f = tmp_path / "feeds.txt"
        f.write_text("Reuters – Venezuela - https://reuters.com/rss\n")
        assert cr.load_feeds(str(f)) == ["https://reuters.com/rss"]

    def test_comments_ignored(self, tmp_path):
        f = tmp_path / "feeds.txt"
        f.write_text("# comment\nhttps://example.com/rss\n")
        assert cr.load_feeds(str(f)) == ["https://example.com/rss"]

    def test_blank_lines_ignored(self, tmp_path):
        f = tmp_path / "feeds.txt"
        f.write_text("\nhttps://example.com/rss\n\n")
        assert cr.load_feeds(str(f)) == ["https://example.com/rss"]

    def test_multiple_feeds(self, tmp_path):
        f = tmp_path / "feeds.txt"
        f.write_text("https://a.com/rss\nhttps://b.com/rss\n")
        result = cr.load_feeds(str(f))
        assert result == ["https://a.com/rss", "https://b.com/rss"]


# ---------------------------------------------------------------------------
# filter_entries
# ---------------------------------------------------------------------------

class TestFilterEntries:
    def test_age_filter_removes_old_entries(self):
        cfg = minimal_cfg()
        old = make_entry(
            title="Venezuela oil news",
            published=NOW - timedelta(days=10),
        )
        entries = [old]
        result = cr.filter_entries(entries, cfg, NOW)
        assert result == []

    def test_age_filter_keeps_recent_entries(self):
        cfg = minimal_cfg()
        recent = make_entry(
            title="Venezuela oil news",
            published=NOW - timedelta(days=3),
        )
        result = cr.filter_entries([recent], cfg, NOW)
        assert len(result) == 1

    def test_age_filter_keeps_undated(self):
        cfg = minimal_cfg()
        undated = make_entry(title="Venezuela news", published=None)
        result = cr.filter_entries([undated], cfg, NOW)
        assert len(result) == 1

    def test_exclude_terms_filter(self):
        cfg = minimal_cfg()
        noisy = make_entry(
            title="Venezuela football championship",
            published=NOW - timedelta(days=1),
        )
        result = cr.filter_entries([noisy], cfg, NOW)
        assert result == []

    def test_country_match_required(self):
        cfg = minimal_cfg()
        irrelevant = make_entry(
            title="Brazil economy news",
            published=NOW - timedelta(days=1),
        )
        result = cr.filter_entries([irrelevant], cfg, NOW)
        assert result == []

    def test_country_match_passes(self):
        cfg = minimal_cfg()
        relevant = make_entry(
            title="Venezuela economy grows",
            published=NOW - timedelta(days=1),
        )
        result = cr.filter_entries([relevant], cfg, NOW)
        assert len(result) == 1

    def test_geo_context_passes(self):
        cfg = minimal_cfg()
        geo = make_entry(
            title="Caribbean trade summit",
            published=NOW - timedelta(days=1),
        )
        result = cr.filter_entries([geo], cfg, NOW)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# deduplicate
# ---------------------------------------------------------------------------

class TestDeduplicate:
    def test_removes_same_url(self):
        e1 = make_entry(title="Venezuela news A", link="https://example.com/1")
        e2 = make_entry(title="Venezuela news B", link="https://example.com/1")
        result = cr.deduplicate([e1, e2])
        assert len(result) == 1

    def test_removes_similar_titles(self):
        e1 = make_entry(title="Venezuela oil production rises sharply this quarter")
        e2 = make_entry(
            title="Venezuela oil production rises sharply this quarter today",
            link="https://example.com/2",
        )
        result = cr.deduplicate([e1, e2], threshold=0.90)
        assert len(result) == 1

    def test_keeps_distinct_entries(self):
        e1 = make_entry(title="Venezuela oil news", link="https://example.com/1")
        e2 = make_entry(title="Brazil agriculture report", link="https://example.com/2")
        result = cr.deduplicate([e1, e2])
        assert len(result) == 2

    def test_empty_links_no_crash(self):
        e1 = make_entry(title="Venezuela news", link="")
        e2 = make_entry(title="Venezuela news", link="")
        result = cr.deduplicate([e1, e2], threshold=0.95)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# score_entry
# ---------------------------------------------------------------------------

class TestScoreEntry:
    def test_score_in_range(self):
        cfg = minimal_cfg()
        e = make_entry(
            title="Venezuela oil sanctions procurement tender",
            published=NOW - timedelta(days=1),
            source_domain="worldbank.org",
        )
        score = cr.score_entry(e, cfg, NOW)
        assert 0.0 <= score <= 1.0

    def test_high_priority_source_boosts_score(self):
        cfg = minimal_cfg()
        base_entry = make_entry(
            title="Venezuela oil news",
            published=NOW - timedelta(days=1),
            source_domain="unknown.com",
        )
        priority_entry = make_entry(
            title="Venezuela oil news",
            published=NOW - timedelta(days=1),
            source_domain="worldbank.org",
        )
        score_base = cr.score_entry(base_entry, cfg, NOW)
        score_priority = cr.score_entry(priority_entry, cfg, NOW)
        assert score_priority > score_base

    def test_recent_entry_scores_higher_than_old(self):
        cfg = minimal_cfg()
        recent = make_entry(
            title="Venezuela oil update",
            published=NOW - timedelta(days=1),
        )
        old = make_entry(
            title="Venezuela oil update",
            published=NOW - timedelta(days=6),
        )
        assert cr.score_entry(recent, cfg, NOW) > cr.score_entry(old, cfg, NOW)

    def test_business_signals_boost_score(self):
        cfg = minimal_cfg()
        plain = make_entry(
            title="Venezuela economy",
            published=NOW - timedelta(days=1),
        )
        signal = make_entry(
            title="Venezuela tender procurement contract",
            published=NOW - timedelta(days=1),
        )
        assert cr.score_entry(signal, cfg, NOW) > cr.score_entry(plain, cfg, NOW)


# ---------------------------------------------------------------------------
# detect_flags
# ---------------------------------------------------------------------------

class TestDetectFlags:
    def test_opportunity_flag(self):
        cfg = minimal_cfg()
        e = make_entry(title="Venezuela tender for oil procurement")
        flags = cr.detect_flags(e, cfg)
        assert "🟢 Opportunity" in flags

    def test_risk_flag(self):
        cfg = minimal_cfg()
        e = make_entry(title="Venezuela sanctions imposed on PDVSA")
        flags = cr.detect_flags(e, cfg)
        assert "🔴 Risk" in flags

    def test_no_flags(self):
        cfg = minimal_cfg()
        e = make_entry(title="Venezuela general news")
        flags = cr.detect_flags(e, cfg)
        assert flags == []


# ---------------------------------------------------------------------------
# detect_sector_label
# ---------------------------------------------------------------------------

class TestDetectSectorLabel:
    def test_extractives_detected(self):
        cfg = minimal_cfg()
        e = make_entry(title="PDVSA oil gas production Venezuela")
        assert cr.detect_sector_label(e, cfg) == "Extractives & Mining"

    def test_food_detected(self):
        cfg = minimal_cfg()
        e = make_entry(title="Venezuela food security agriculture program")
        assert cr.detect_sector_label(e, cfg) == "Food & Agriculture"

    def test_fallback_label(self):
        cfg = minimal_cfg()
        e = make_entry(title="Venezuela policy update")
        label = cr.detect_sector_label(e, cfg)
        assert label == "Cross-cutting / Policy / Risk"


# ---------------------------------------------------------------------------
# build_markdown
# ---------------------------------------------------------------------------

class TestBuildMarkdown:
    def test_contains_required_sections(self):
        cfg = minimal_cfg()
        run_meta = {
            "run_at": NOW.isoformat(),
            "fetched": 10,
            "filtered": 5,
            "deduplicated": 4,
            "selected": 3,
            "output_file": "docs/index.md",
            "metadata_file": "data/last_run.json",
        }
        entries = [
            {
                **make_entry(
                    title="Venezuela oil tender",
                    published=NOW - timedelta(days=1),
                    source_domain="worldbank.org",
                ),
                "score": 0.75,
            }
        ]
        md = cr.build_markdown(entries, cfg, run_meta)
        assert "## Pipeline Metrics" in md
        assert "## Scoring Summary" in md
        assert "## Run Metadata" in md
        assert "## Top Results" in md

    def test_contains_entry_title(self):
        cfg = minimal_cfg()
        run_meta = {
            "run_at": NOW.isoformat(),
            "fetched": 1,
            "filtered": 1,
            "deduplicated": 1,
            "selected": 1,
            "output_file": "docs/index.md",
            "metadata_file": "data/last_run.json",
        }
        entries = [
            {
                **make_entry(
                    title="PDVSA Oil Tender Venezuela",
                    published=NOW - timedelta(days=1),
                ),
                "score": 0.80,
            }
        ]
        md = cr.build_markdown(entries, cfg, run_meta)
        assert "PDVSA Oil Tender Venezuela" in md

    def test_no_entries_shows_placeholder(self):
        cfg = minimal_cfg()
        run_meta = {
            "run_at": NOW.isoformat(),
            "fetched": 0,
            "filtered": 0,
            "deduplicated": 0,
            "selected": 0,
            "output_file": "docs/index.md",
            "metadata_file": "data/last_run.json",
        }
        md = cr.build_markdown([], cfg, run_meta)
        assert "_No entries scored this run._" in md


# ---------------------------------------------------------------------------
# Integration: run() with mocked feed fetching
# ---------------------------------------------------------------------------

class TestRunIntegration:
    def test_run_creates_output_files(self, tmp_path):
        # Minimal config & feeds files
        cfg = minimal_cfg()
        cfg_path = tmp_path / "config.yml"
        feeds_path = tmp_path / "feeds.txt"
        cfg_path.write_text(yaml.dump(cfg))
        feeds_path.write_text("https://example.com/rss\n")

        # Override output paths
        docs_dir = tmp_path / "docs"
        data_dir = tmp_path / "data"
        docs_dir.mkdir()
        data_dir.mkdir()
        output_path = docs_dir / "index.md"
        metadata_path = data_dir / "last_run.json"

        mock_entry = {
            "title": "Venezuela oil production tender",
            "link": "https://example.com/1",
            "summary": "Venezuela PDVSA tender procurement",
            "published": NOW - timedelta(days=1),
            "source_url": "https://example.com/rss",
            "source_domain": "example.com",
        }

        with (
            patch.object(cr, "fetch_feed", return_value=[mock_entry]),
            patch.object(cr, "DOCS_DIR", str(docs_dir)),
            patch.object(cr, "DATA_DIR", str(data_dir)),
            patch.object(cr, "OUTPUT_PATH", str(output_path)),
            patch.object(cr, "METADATA_PATH", str(metadata_path)),
        ):
            cr.run(config_path=str(cfg_path), feeds_path=str(feeds_path))

        assert output_path.exists()
        assert metadata_path.exists()

        meta = json.loads(metadata_path.read_text())
        assert meta["fetched"] == 1
        assert "run_at" in meta

    def test_idempotency(self, tmp_path):
        """Running twice without new data should not change docs/index.md."""
        cfg = minimal_cfg()
        cfg_path = tmp_path / "config.yml"
        feeds_path = tmp_path / "feeds.txt"
        cfg_path.write_text(yaml.dump(cfg))
        feeds_path.write_text("https://example.com/rss\n")

        docs_dir = tmp_path / "docs"
        data_dir = tmp_path / "data"
        docs_dir.mkdir()
        data_dir.mkdir()
        output_path = docs_dir / "index.md"
        metadata_path = data_dir / "last_run.json"

        fixed_now = NOW

        mock_entry = {
            "title": "Venezuela news",
            "link": "https://example.com/1",
            "summary": "Venezuela news summary",
            "published": fixed_now - timedelta(days=1),
            "source_url": "https://example.com/rss",
            "source_domain": "example.com",
        }

        kwargs = dict(
            config_path=str(cfg_path),
            feeds_path=str(feeds_path),
        )

        with (
            patch.object(cr, "fetch_feed", return_value=[mock_entry]),
            patch.object(cr, "DOCS_DIR", str(docs_dir)),
            patch.object(cr, "DATA_DIR", str(data_dir)),
            patch.object(cr, "OUTPUT_PATH", str(output_path)),
            patch.object(cr, "METADATA_PATH", str(metadata_path)),
            patch("collect_rfps.datetime") as mock_dt,
        ):
            mock_dt.now.return_value = fixed_now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            cr.run(**kwargs)
            content_after_first = output_path.read_text()
            mtime_after_first = output_path.stat().st_mtime

            cr.run(**kwargs)
            mtime_after_second = output_path.stat().st_mtime

        assert mtime_after_first == mtime_after_second, (
            "docs/index.md should not be re-written when content is unchanged"
        )
