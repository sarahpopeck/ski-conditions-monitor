# capstoneAirflow/tests/test_resort_parsers.py

from runner import (
    parse_killington_lifts,
    parse_killington_snow_report,
    parse_killington_trails,
    parse_resort_family_boyne,
    parse_resort_family_killington,
    parse_resort_family_mountainpowder,
)


def test_parse_resort_family_boyne_returns_expected_fields(sample_reportpal_payload):
    row = parse_resort_family_boyne(
        payload=sample_reportpal_payload,
        resort="Loon",
        raw_path="/tmp/Loon_reportpal.json",
    )

    assert row["resort"] == "Loon"
    assert row["raw_path"] == "/tmp/Loon_reportpal.json"
    assert row["trails_open"] == 80
    assert row["trails_total"] == 100
    assert row["open_trails_pct"] == 80
    assert row["lifts_open"] == 8
    assert row["lifts_total"] == 10
    assert row["open_lifts_pct"] == 80
    assert row["mountain_report_text"] == "Packed powder with good coverage."
    assert row["report_date"] is not None
    assert row["resort_updated_at"] is not None


def test_parse_killington_lifts_counts_open_and_expected(sample_killington_lifts_payload):
    result = parse_killington_lifts(sample_killington_lifts_payload)

    assert result["lifts_total"] == 4
    assert result["lifts_open"] == 2
    assert result["lifts_updated_at"] is not None


def test_parse_killington_trails_filters_and_counts(sample_killington_trails_payload):
    result = parse_killington_trails(sample_killington_trails_payload)

    # Only 2 records should survive filtering:
    # - winter + alpine_trail + include=True
    assert result["trails_total"] == 2
    assert result["trails_open"] == 1
    assert result["trails_updated_at"] is not None


def test_parse_killington_snow_report_extracts_text_and_timestamp():
    payload = [
        {
            "report": "Firm groomers with spring conditions later.",
            "updated": 1708771200,
        }
    ]

    result = parse_killington_snow_report(payload)

    assert result["mountain_report_text"] == "Firm groomers with spring conditions later."
    assert result["snow_report_updated_at"] is not None
    assert result["snow_report_updated_at"].endswith("+00:00")


def test_parse_resort_family_killington_combines_sources(
    sample_killington_lifts_payload,
    sample_killington_trails_payload,
):
    snow_payload = [
        {
            "report": "Good surfaces with some wind exposure.",
            "updated": 1708771200,
        }
    ]

    row = parse_resort_family_killington(
        lifts_payload=sample_killington_lifts_payload,
        trails_payload=sample_killington_trails_payload,
        snow_payload=snow_payload,
        resort="Killington",
        raw_path="/tmp/combined.json",
    )

    assert row["resort"] == "Killington"
    assert row["raw_path"] == "/tmp/combined.json"
    assert row["trails_open"] == 1
    assert row["trails_total"] == 2
    assert row["open_trails_pct"] == 50
    assert row["lifts_open"] == 2
    assert row["lifts_total"] == 4
    assert row["open_lifts_pct"] == 50
    assert row["mountain_report_text"] == "Good surfaces with some wind exposure."
    assert row["report_date"] is not None
    assert row["resort_updated_at"] is not None


def test_parse_resort_family_mountainpowder_returns_expected_fields():
    payload = {
        "Resorts": [
            {
                "SnowReport": {
                    "TotalTrails": 120,
                    "TotalOpenTrails": 90,
                    "TotalLifts": 12,
                    "TotalOpenLifts": 9,
                    "Report": "Packed powder and machine groomed.",
                    "LastUpdate": "2026-02-24T07:00:00-05:00",
                }
            }
        ]
    }

    row = parse_resort_family_mountainpowder(
        payload=payload,
        resort="Sugarbush",
        raw_path="/tmp/Sugarbush_feed.json",
    )

    assert row["resort"] == "Sugarbush"
    assert row["raw_path"] == "/tmp/Sugarbush_feed.json"
    assert row["trails_open"] == 90
    assert row["trails_total"] == 120
    assert row["open_trails_pct"] == 75
    assert row["lifts_open"] == 9
    assert row["lifts_total"] == 12
    assert row["open_lifts_pct"] == 75
    assert row["mountain_report_text"] == "Packed powder and machine groomed."
    assert row["report_date"] is not None
    assert row["resort_updated_at"] is not None