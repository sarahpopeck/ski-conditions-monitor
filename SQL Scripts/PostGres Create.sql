
CREATE TABLE IF NOT EXISTS resort_status_daily (
    resort               text        NOT NULL,
    report_date          date        NOT NULL,
    raw_path             text,
    resort_updated_at    timestamptz NOT NULL,
    trails_open          integer,
    trails_total         integer,
    open_trails_pct      integer,
    lifts_open           integer,
    lifts_total          integer,
    open_lifts_pct       integer,
    mountain_report_text text,
    PRIMARY KEY (resort, report_date, resort_updated_at)
);

CREATE TABLE IF NOT EXISTS openmeteo_hourly (
    resort                      TEXT        NOT NULL,
    forecast_run_at             TIMESTAMPTZ NOT NULL,
    time_local                  TIMESTAMPTZ NOT NULL,
    target_ski_date             DATE        NOT NULL,
    hour_local                  INTEGER     NOT NULL,

    temperature_2m_c            NUMERIC,
    cloudcover_pct              NUMERIC,
    wind_speed_10m_kmh          NUMERIC,
    wind_gusts_10m_kmh          NUMERIC,
    precipitation_mm            NUMERIC,
    snowfall_cm                 NUMERIC,

    PRIMARY KEY (resort, forecast_run_at, time_local)
    );
    
   CREATE TABLE IF NOT EXISTS openmeteo_features_daily (
    resort                          TEXT        NOT NULL,
    forecast_run_at                 TIMESTAMPTZ NOT NULL,
    target_ski_date                 DATE        NOT NULL,

    avg_temp_ski_hours_c            NUMERIC,
    min_temp_ski_hours_c            NUMERIC,
    max_temp_ski_hours_c            NUMERIC,
    avg_cloudcover_ski_hours_pct    NUMERIC,
    avg_wind_speed_ski_hours_kmh    NUMERIC,
    max_wind_gust_ski_hours_kmh     NUMERIC,

    snowfall_ski_day_cm             NUMERIC,
    rain_ski_day_mm                 NUMERIC,

    snowfall_prev_day_cm            NUMERIC,
    rain_prev_day_mm                NUMERIC,
    hours_above_freezing_prev_day   INTEGER,
    freeze_thaw_cycles_prev_48h     INTEGER,

    ice_risk_flag                   INTEGER,
    wind_hold_risk_flag             INTEGER,

    PRIMARY KEY (resort, forecast_run_at, target_ski_date)
);


select * from openmeteo_hourly oh; 

truncate table openmeteo_features_daily;

select count(*) from openmeteo_features_daily;



SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY resort, target_ski_date
               ORDER BY forecast_run_at DESC
           ) AS rn
    FROM openmeteo_features_daily
) t
WHERE rn = 1
ORDER BY resort, target_ski_date;