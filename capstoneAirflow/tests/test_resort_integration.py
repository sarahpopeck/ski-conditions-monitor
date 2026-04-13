from runner import (
    parse_resort_family_boyne,
    parse_resort_family_killington,
    validate_reportpal,
)


def test_reportpal_validation_to_normalized_row(sample_reportpal_payload):
    ok, reason = validate_reportpal(sample_reportpal_payload)
    assert ok is True
    assert reason == "ok"

    row = parse_resort_family_boyne(
        payload=sample_reportpal_payload,
        resort="Loon",
        raw_path="/tmp/Loon_reportpal.json",
    )

    assert row["resort"] == "Loon"
    assert row["report_date"] is not None
    assert row["trails_open"] == 80
    assert row["trails_total"] == 100
    assert row["open_trails_pct"] == 80
    assert row["lifts_open"] == 8
    assert row["lifts_total"] == 10
    assert row["open_lifts_pct"] == 80
    assert row["mountain_report_text"] == "Packed powder with good coverage."


def test_killington_family_combines_three_payloads(
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
        raw_path="/tmp/lifts.json | /tmp/trails.json | /tmp/snow.json",
    )

    assert row["resort"] == "Killington"
    assert row["report_date"] is not None
    assert row["trails_open"] == 1
    assert row["trails_total"] == 2
    assert row["lifts_open"] == 2
    assert row["lifts_total"] == 4
    assert row["mountain_report_text"] == "Good surfaces with some wind exposure."