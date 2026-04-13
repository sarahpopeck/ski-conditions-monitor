# capstoneAirflow/tests/test_reddit_pipeline.py

import pandas as pd

from runner import (
    aggregate_reddit_resort_day_signals,
    build_source_item_key_from_parts,
    normalize_reddit_items,
    prepare_reddit_new_rows,
)


def test_build_source_item_key_for_comment():
    key = build_source_item_key_from_parts("post123", "comment456")
    assert key == "post123_comment456"


def test_build_source_item_key_for_post():
    key = build_source_item_key_from_parts("post123", None)
    assert key == "post123_post"


def test_build_source_item_key_returns_none_if_missing():
    key = build_source_item_key_from_parts(None, None)
    assert key is None


def test_normalize_reddit_items_creates_expected_columns():
    items = [
        {
            "post_id": "p1",
            "comment_id": "c1",
            "record_type": "comment",
            "subreddit": "icecoast",
            "author": "user1",
            "title": "Great day",
            "text": "Conditions were awesome",
            "permalink": "/r/test",
            "url": "http://reddit.com",
            "created_utc_iso": "2026-02-24T12:00:00+00:00",
            "scraped_at_iso": "2026-02-24T13:00:00+00:00",
            "parent_id": "p1",
            "parent_type": "post",
        }
    ]

    df = normalize_reddit_items(items)

    assert len(df) == 1
    assert "source_item_key" in df.columns
    assert df.iloc[0]["source_item_key"] == "p1_c1"


def test_prepare_reddit_new_rows_filters_existing_keys(tmp_path):
    items = [
        {
            "post_id": "p1",
            "comment_id": "c1",
            "record_type": "comment",
        },
        {
            "post_id": "p2",
            "comment_id": None,
            "record_type": "post",
        },
    ]

    raw_path = tmp_path / "reddit.json"
    pd.DataFrame(items).to_json(raw_path, orient="records")

    existing_keys = {"p1_c1"}

    new_df = prepare_reddit_new_rows(str(raw_path), existing_keys)

    assert len(new_df) == 1
    assert new_df.iloc[0]["source_item_key"] == "p2_post"


def test_aggregate_reddit_resort_day_signals_basic_case():
    df = pd.DataFrame(
        {
            "source_item_key": ["1", "2", "3"],
            "report_type": ["field_report", "forecast_opinion", "field_report"],
            "resort": ["Loon", "Loon", "Loon"],
            "resolved_date": ["2026-02-24", "2026-02-24", "2026-02-24"],
            "sentiment_label": ["positive", "negative", "neutral"],
        }
    )

    out = aggregate_reddit_resort_day_signals(df)

    assert len(out) == 1
    row = out.iloc[0]

    assert row["resort"] == "Loon"
    assert row["field_report_count"] == 2
    assert row["forecast_opinion_count"] == 1
    assert row["positive_count"] == 1
    assert row["negative_count"] == 1
    assert row["neutral_count"] == 1
    assert row["signal_present"] == 1


def test_aggregate_reddit_resort_day_signals_filters_invalid_rows():
    df = pd.DataFrame(
        {
            "source_item_key": ["1", "2"],
            "report_type": ["other", "field_report"],
            "resort": [None, "Loon"],
            "resolved_date": ["2026-02-24", None],
            "sentiment_label": ["positive", "negative"],
        }
    )

    out = aggregate_reddit_resort_day_signals(df)

    assert out.empty