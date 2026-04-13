from runner import build_openmeteo_features_daily, parse_openmeteo_hourly


def test_openmeteo_payload_to_daily_features(sample_openmeteo_payload):
    hourly_df = parse_openmeteo_hourly(
        payload=sample_openmeteo_payload,
        resort="Loon",
        raw_path="/tmp/Loon_forecast_20260224_0700.json",
    )

    assert not hourly_df.empty
    assert "time_local" in hourly_df.columns
    assert "hour_local" in hourly_df.columns

    daily_df = build_openmeteo_features_daily(hourly_df)

    assert not daily_df.empty
    assert "avg_temp_ski_hours_c" in daily_df.columns
    assert "snowfall_ski_day_cm" in daily_df.columns
    assert "wind_hold_risk_flag" in daily_df.columns