# capstoneAirflow/tests/test_resort_validation.py

from runner import (
    validate_killington,
    validate_mtnpowder,
    validate_openmeteo,
    validate_reportpal,
)


def test_validate_reportpal_valid_payload(sample_reportpal_payload):
    ok, reason = validate_reportpal(sample_reportpal_payload)
    assert ok is True
    assert reason == "ok"


def test_validate_reportpal_missing_current_conditions():
    payload = {"updated": "2026-02-24T07:00:00-05:00"}
    ok, reason = validate_reportpal(payload)
    assert ok is False
    assert "missing key currentConditions" in reason


def test_validate_killington_valid_list(sample_killington_trails_payload):
    ok, reason = validate_killington(sample_killington_trails_payload)
    assert ok is True
    assert reason == "ok"


def test_validate_killington_empty_list():
    ok, reason = validate_killington([])
    assert ok is False
    assert reason == "killington: empty list"


def test_validate_mtnpowder_valid_payload():
    payload = {
        "Resorts": [
            {
                "SnowReport": {
                    "TotalTrails": 100,
                    "TotalOpenTrails": 80,
                    "TotalLifts": 10,
                    "TotalOpenLifts": 8,
                    "Report": "Good conditions",
                    "LastUpdate": "2026-02-24T07:00:00-05:00",
                }
            }
        ]
    }
    ok, reason = validate_mtnpowder(payload)
    assert ok is True
    assert reason == "ok"


def test_validate_mtnpowder_missing_resorts():
    payload = {}
    ok, reason = validate_mtnpowder(payload)
    assert ok is False
    assert reason == "mtnpowder: missing Resorts"


def test_validate_openmeteo_valid_payload():
    payload = {
        "hourly": {"time": [], "temperature_2m": []},
        "daily": {"time": []},
    }
    ok, reason = validate_openmeteo(payload)
    assert ok is True
    assert reason == "ok"


def test_validate_openmeteo_missing_daily():
    payload = {
        "hourly": {"time": [], "temperature_2m": []},
    }
    ok, reason = validate_openmeteo(payload)
    assert ok is False
    assert reason == "openmeteo: missing daily"