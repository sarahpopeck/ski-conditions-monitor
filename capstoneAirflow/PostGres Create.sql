
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



CREATE TABLE IF NOT EXISTS reddit_labeled (
    source_item_key         TEXT PRIMARY KEY,

    post_id                 TEXT,
    comment_id              TEXT,
    record_type             TEXT NOT NULL,

    subreddit               TEXT,
    author                  TEXT,
    title                   TEXT,
    text                    TEXT,
    permalink               TEXT,
    url                     TEXT,

    created_utc_iso         TIMESTAMPTZ,
    scraped_at_iso          TIMESTAMPTZ,

    report_type             TEXT NOT NULL,   -- field_report / forecast_opinion / other
    resort                  TEXT,
    resolved_date           DATE,
    date_resolution_source  TEXT,            -- explicit_text / relative_text / fallback_post_date / unknown
    sentiment_label         TEXT,            -- positive / negative / neutral

    labeling_model          TEXT,
    labeled_at              TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS reddit_resort_day_signals (
    resort                  TEXT NOT NULL,
    ski_date                DATE NOT NULL,

    field_report_count      INTEGER NOT NULL,
    forecast_opinion_count  INTEGER NOT NULL,

    positive_count          INTEGER NOT NULL,
    negative_count          INTEGER NOT NULL,
    neutral_count           INTEGER NOT NULL,

    net_sentiment           INTEGER NOT NULL,
    signal_present          INTEGER NOT NULL,

    updated_at              TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (resort, ski_date)
);



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

CREATE TABLE IF NOT EXISTS reddit_resort_day_signals (
    resort                  TEXT NOT NULL,
    ski_date                DATE NOT NULL,

    field_report_count      INTEGER NOT NULL,
    forecast_opinion_count  INTEGER NOT NULL,

    positive_count          INTEGER NOT NULL,
    negative_count          INTEGER NOT NULL,
    neutral_count           INTEGER NOT NULL,

    net_sentiment           INTEGER NOT NULL,
    weighted_signal_score   INTEGER NOT NULL,
    signal_present          INTEGER NOT NULL,

    updated_at              TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (resort, ski_date)
);



