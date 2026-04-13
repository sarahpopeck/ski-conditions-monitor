import sys
from pathlib import Path
import json
import pandas as pd
import pytest


# ============================================================
# FIX IMPORT PATH
# Makes runner.py (one level up) importable
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]  # capstoneAirflow/
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# SHARED FIXTURES
# ============================================================

@pytest.fixture
def sample_reportpal_payload() -> dict:
    return {
        "updated": "2026-02-24T07:00:00-05:00",
        "currentConditions": {
            "resortwide": {
                "numTrailsTotal": 100,
                "numTrailsOpen": 80,
                "numLiftsTotal": 10,
                "numLiftsOpen": 8,
            }
        },
        "comments": {
            "comment": [
                {"text": "Packed powder with good coverage."}
            ]
        },
    }


@pytest.fixture
def sample_killington_trails_payload() -> list[dict]:
    return [
        # valid open alpine winter trail
        {
            "season": "winter",
            "type": "alpine_trail",
            "include": True,
            "status": "open",
            "updated": "2026-02-24T08:00:00-05:00",
        },
        # valid closed alpine winter trail
        {
            "season": "winter",
            "type": "alpine_trail",
            "include": True,
            "status": "closed",
            "updated": "2026-02-24T08:30:00-05:00",
        },
        # should be excluded (summer)
        {
            "season": "summer",
            "type": "alpine_trail",
            "include": True,
            "status": "open",
            "updated": "2026-02-24T08:45:00-05:00",
        },
        # should be excluded (wrong type)
        {
            "season": "winter",
            "type": "nordic_trail",
            "include": True,
            "status": "open",
            "updated": "2026-02-24T09:00:00-05:00",
        },
        # should be excluded (include=False)
        {
            "season": "winter",
            "type": "alpine_trail",
            "include": False,
            "status": "open",
            "updated": "2026-02-24T09:15:00-05:00",
        },
    ]


@pytest.fixture
def sample_killington_lifts_payload() -> list[dict]:
    return [
        {"status": "open", "updated": "2026-02-24T08:00:00-05:00"},
        {"status": "expected", "updated": "2026-02-24T08:15:00-05:00"},
        {"status": "closed", "updated": "2026-02-24T08:30:00-05:00"},
        {"status": "hold", "updated": "2026-02-24T08:45:00-05:00"},
    ]


@pytest.fixture
def sample_hourly_temps() -> pd.Series:
    # used for freeze-thaw tests
    return pd.Series([-2, -1, 1, 2, -1, -2, 1])

@pytest.fixture
def sample_openmeteo_payload() -> dict:
    return {
        "timezone": "America/New_York",
        "hourly": {
            "time": [
                "2026-02-24T09:00",
                "2026-02-24T10:00",
                "2026-02-24T11:00",
                "2026-02-23T12:00",
                "2026-02-22T12:00",
            ],
            "temperature_2m": [-5.0, -3.0, -1.0, 2.0, -2.0],
            "cloudcover": [10, 20, 30, 40, 50],
            "wind_speed_10m": [15.0, 20.0, 18.0, 10.0, 12.0],
            "wind_gusts_10m": [25.0, 30.0, 28.0, 40.0, 35.0],
            "precipitation": [0.0, 0.0, 1.0, 5.0, 0.0],
            "snowfall": [0.0, 0.5, 1.0, 0.0, 0.5],
        },
        "daily": {
            "time": ["2026-02-24"],
        },
    }


@pytest.fixture
def sample_reddit_items() -> list[dict]:
    return [
        {
            "post_id": "p1",
            "comment_id": "c1",
            "record_type": "comment",
            "subreddit": "icecoast",
            "author": "user1",
            "title": "Loon conditions",
            "text": "Good packed powder today at Loon",
            "permalink": "/r/icecoast/comments/1",
            "url": "https://reddit.com/1",
            "created_utc_iso": "2026-02-24T12:00:00+00:00",
            "scraped_at_iso": "2026-02-24T13:00:00+00:00",
            "parent_id": "p1",
            "parent_type": "post",
        },
        {
            "post_id": "p2",
            "comment_id": None,
            "record_type": "post",
            "subreddit": "icecoast",
            "author": "user2",
            "title": "Sugarloaf report",
            "text": "Windy but still decent skiing",
            "permalink": "/r/icecoast/comments/2",
            "url": "https://reddit.com/2",
            "created_utc_iso": "2026-02-24T14:00:00+00:00",
            "scraped_at_iso": "2026-02-24T15:00:00+00:00",
            "parent_id": None,
            "parent_type": None,
        },
    ]


@pytest.fixture
def sample_labeled_reddit_df():
    import pandas as pd
    return pd.DataFrame(
        {
            "source_item_key": ["p1_c1", "p2_post"],
            "post_id": ["p1", "p2"],
            "comment_id": ["c1", None],
            "record_type": ["comment", "post"],
            "subreddit": ["icecoast", "icecoast"],
            "author": ["user1", "user2"],
            "title": ["Loon conditions", "Sugarloaf report"],
            "text": ["Good packed powder today at Loon", "Windy but still decent skiing"],
            "permalink": ["/r/icecoast/comments/1", "/r/icecoast/comments/2"],
            "url": ["https://reddit.com/1", "https://reddit.com/2"],
            "created_utc_iso": ["2026-02-24T12:00:00+00:00", "2026-02-24T14:00:00+00:00"],
            "scraped_at_iso": ["2026-02-24T13:00:00+00:00", "2026-02-24T15:00:00+00:00"],
            "report_type": ["field_report", "forecast_opinion"],
            "resort": ["Loon", "Sugarloaf"],
            "resolved_date": ["2026-02-24", "2026-02-24"],
            "date_resolution_source": ["fallback_post_date", "fallback_post_date"],
            "sentiment_label": ["positive", "neutral"],
            "labeling_model": ["mistral-large-latest", "mistral-large-latest"],
            "labeled_at": pd.to_datetime(["2026-02-24T16:00:00+00:00", "2026-02-24T16:00:00+00:00"]),
        }
    )


@pytest.fixture
def temp_json_file(tmp_path: Path):
    def _write(payload, filename="payload.json"):
        fp = tmp_path / filename
        fp.write_text(json.dumps(payload), encoding="utf-8")
        return str(fp)
    return _write