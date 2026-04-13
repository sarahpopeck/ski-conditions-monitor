import pandas as pd

from runner import (
    build_openmeteo_features_daily,
    count_freeze_thaw_cycles,
    parse_openmeteo_hourly,
    rain_proxy_mm,
)


def test_rain_proxy_mm_returns_precip_when_temp_above_freezing():
    assert rain_proxy_mm(2.0, 5.5) == 5.5


def test_rain_proxy_mm_returns_zero_when_temp_below_freezing():
    assert rain_proxy_mm(-1.0, 5.5) == 0.0


def test_rain_proxy_mm_returns_zero_for_none_inputs():
    assert rain_proxy_mm(None, 5.5) == 0.0
    assert rain_proxy_mm(2.0, None) == 0.0


def test_count_freeze_thaw_cycles_counts_crossings(sample_hourly_temps):
    # Series: [-2, -1, 1, 2, -1, -2, 1]
    # Crossings:
    # -1 -> 1  (1)
    #  2 -> -1 (2)
    # -2 -> 1  (3)
    assert count_freeze_thaw_cycles(sample_hourly_temps) == 3


def test_count_freeze_thaw_cycles_returns_zero_for_short_series():
    assert count_freeze_thaw_cycles(pd.Series([])) == 0
    assert count_freeze_thaw_cycles(pd.Series([1])) == 0


def test_parse_openmeteo_hourly_returns_expected_columns():
    payload = {
        "timezone": "America/New_York",
        "hourly": {
            "time": [
                "2026-02-24T09:00",
                "2026-02-24T10:00",
                "2026-02-24T11:00",
            ],
            "temperature_2m": [-5.0, -4.0, -3.0],
            "cloudcover": [10, 20, 30],
            "wind_speed_10m": [15.0, 20.0, 18.0],
            "wind_gusts_10m": [25.0, 30.0, 28.0],
            "precipitation": [0.0, 0.0, 1.0],
            "snowfall": [0.0, 0.5, 1.0],
        },
    }

    df = parse_openmeteo_hourly(
        payload=payload,
        resort="Loon",
        raw_path="/tmp/Loon_forecast_20260224_0700.json",
    )

    expected_cols = [
        "resort",
        "forecast_run_at",
        "time_local",
        "target_ski_date",
        "hour_local",
        "temperature_2m_c",
        "cloudcover_pct",
        "wind_speed_10m_kmh",
        "wind_gusts_10m_kmh",
        "precipitation_mm",
        "snowfall_cm",
    ]

    assert list(df.columns) == expected_cols
    assert len(df) == 3
    assert (df["resort"] == "Loon").all()
    assert df["forecast_run_at"].notna().all()
    assert df["target_ski_date"].notna().all()
    assert list(df["hour_local"]) == [9, 10, 11]


def test_parse_openmeteo_hourly_returns_empty_df_for_empty_payload():
    payload = {
        "timezone": "America/New_York",
        "hourly": {
            "time": [],
            "temperature_2m": [],
            "cloudcover": [],
            "wind_speed_10m": [],
            "wind_gusts_10m": [],
            "precipitation": [],
            "snowfall": [],
        },
    }

    df = parse_openmeteo_hourly(
        payload=payload,
        resort="Loon",
        raw_path="/tmp/Loon_forecast_20260224_0700.json",
    )

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_build_openmeteo_features_daily_returns_expected_feature_values():
    hourly_df = pd.DataFrame(
        {
            "resort": ["Loon"] * 5,
            "forecast_run_at": ["2026-02-24T12:00:00+00:00"] * 5,
            "time_local": pd.to_datetime(
                [
                    "2026-02-24 09:00:00-05:00",
                    "2026-02-24 10:00:00-05:00",
                    "2026-02-24 11:00:00-05:00",
                    "2026-02-23 12:00:00-05:00",
                    "2026-02-22 12:00:00-05:00",
                ]
            ),
            "target_ski_date": [
                pd.Timestamp("2026-02-24").date(),
                pd.Timestamp("2026-02-24").date(),
                pd.Timestamp("2026-02-24").date(),
                pd.Timestamp("2026-02-23").date(),
                pd.Timestamp("2026-02-22").date(),
            ],
            "hour_local": [9, 10, 11, 12, 12],
            "temperature_2m_c": [-5.0, -3.0, -1.0, 2.0, -2.0],
            "cloudcover_pct": [10, 20, 30, 40, 50],
            "wind_speed_10m_kmh": [15.0, 20.0, 18.0, 10.0, 12.0],
            "wind_gusts_10m_kmh": [25.0, 30.0, 28.0, 40.0, 35.0],
            "precipitation_mm": [0.0, 0.0, 1.0, 5.0, 0.0],
            "snowfall_cm": [0.0, 0.5, 1.0, 0.0, 0.5],
        }
    )

    out = build_openmeteo_features_daily(hourly_df)

    assert len(out) == 3  # one row for each distinct target_ski_date
    row_2026_02_24 = out[out["target_ski_date"] == pd.Timestamp("2026-02-24").date()].iloc[0]

    assert row_2026_02_24["resort"] == "Loon"
    assert row_2026_02_24["avg_temp_ski_hours_c"] == -3.0
    assert row_2026_02_24["min_temp_ski_hours_c"] == -5.0
    assert row_2026_02_24["max_temp_ski_hours_c"] == -1.0
    assert row_2026_02_24["avg_cloudcover_ski_hours_pct"] == 20.0
    assert row_2026_02_24["avg_wind_speed_ski_hours_kmh"] == (15.0 + 20.0 + 18.0) / 3
    assert row_2026_02_24["max_wind_gust_ski_hours_kmh"] == 30.0
    assert row_2026_02_24["snowfall_ski_day_cm"] == 1.5
    assert row_2026_02_24["rain_ski_day_mm"] == 0.0  # temp <= 0 during ski hours
    assert row_2026_02_24["snowfall_prev_day_cm"] == 0.0
    assert row_2026_02_24["rain_prev_day_mm"] == 5.0
    assert row_2026_02_24["hours_above_freezing_prev_day"] == 1
    assert row_2026_02_24["ice_risk_flag"] in [True, False]


def test_build_openmeteo_features_daily_sets_wind_hold_flag():
    hourly_df = pd.DataFrame(
        {
            "resort": ["Loon"],
            "forecast_run_at": ["2026-02-24T12:00:00+00:00"],
            "time_local": pd.to_datetime(["2026-02-24 10:00:00-05:00"]),
            "target_ski_date": [pd.Timestamp("2026-02-24").date()],
            "hour_local": [10],
            "temperature_2m_c": [-3.0],
            "cloudcover_pct": [20],
            "wind_speed_10m_kmh": [30.0],
            "wind_gusts_10m_kmh": [70.0],
            "precipitation_mm": [0.0],
            "snowfall_cm": [2.0],
        }
    )

    out = build_openmeteo_features_daily(hourly_df)
    row = out.iloc[0]

    assert bool(row["wind_hold_risk_flag"]) is True


def test_build_openmeteo_features_daily_returns_empty_schema_for_empty_input():
    out = build_openmeteo_features_daily(pd.DataFrame())

    assert isinstance(out, pd.DataFrame)
    assert out.empty
    expected_cols = [
        "resort",
        "forecast_run_at",
        "target_ski_date",
        "avg_temp_ski_hours_c",
        "min_temp_ski_hours_c",
        "max_temp_ski_hours_c",
        "avg_cloudcover_ski_hours_pct",
        "avg_wind_speed_ski_hours_kmh",
        "max_wind_gust_ski_hours_kmh",
        "snowfall_ski_day_cm",
        "rain_ski_day_mm",
        "snowfall_prev_day_cm",
        "rain_prev_day_mm",
        "hours_above_freezing_prev_day",
        "freeze_thaw_cycles_prev_48h",
        "ice_risk_flag",
        "wind_hold_risk_flag",
    ]
    assert list(out.columns) == expected_cols