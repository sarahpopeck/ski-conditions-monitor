from datetime import datetime
import re

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from psycopg2.extras import execute_values

import sys
sys.path.append("/opt/airflow/project")
from runner import (  # noqa: E402
    JOBS,
    build_openmeteo_features_daily,
    cmd_extract,
    cmd_validate,
    load_raw,
    parse_openmeteo_hourly,
)

JOB_IDS = [
    "Loon:forecast",
    "SundayRiver:forecast",
    "Sugarloaf:forecast",
    "Killington:forecast",
    "Pico:forecast",
    "Sugarbush:forecast",
    "Stratton:forecast",
]

local_tz = pendulum.timezone("America/New_York")


def slug(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", s).lower()


def choose_task_id(job_id: str, run_task_id: str, skip_task_id: str, **context) -> str:
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    job_filter = conf.get("job_filter")
    if job_filter is None or job_filter == "":
        return run_task_id
    return run_task_id if job_filter == job_id else skip_task_id


with DAG(
    dag_id="ski_openmeteo_ingest",
    start_date=datetime(2026, 2, 16, tzinfo=local_tz),
    schedule="0 7,19 * * *",
    catchup=False,
    default_args={"retries": 2},
    tags=["ski", "openmeteo", "forecast"],
    params={"job_filter": None},
) as dag:

    @task
    def extract(job_id: str) -> str:
        return cmd_extract(job_id)

    @task
    def validate(job_id: str, file_path: str) -> str:
        cmd_validate(job_id, file_path)
        return file_path

    @task
    def load_to_db(job_id: str, file_path: str) -> None:
        job = JOBS[job_id]
        payload = load_raw(file_path)
        hourly_df = parse_openmeteo_hourly(payload=payload, resort=job.resort, raw_path=file_path)
        daily_df = build_openmeteo_features_daily(hourly_df)

        hook = PostgresHook(postgres_conn_id="ski_pg")
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS openmeteo_hourly (
                resort text NOT NULL,
                forecast_run_at timestamptz NOT NULL,
                time_local timestamptz NOT NULL,
                target_ski_date date NOT NULL,
                hour_local int NOT NULL,
                temperature_2m_c numeric,
                cloudcover_pct numeric,
                wind_speed_10m_kmh numeric,
                wind_gusts_10m_kmh numeric,
                precipitation_mm numeric,
                snowfall_cm numeric,
                PRIMARY KEY (resort, forecast_run_at, time_local)
            );
            """
        )
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS openmeteo_features_daily (
                resort text NOT NULL,
                forecast_run_at timestamptz NOT NULL,
                target_ski_date date NOT NULL,
                avg_temp_ski_hours_c numeric,
                min_temp_ski_hours_c numeric,
                max_temp_ski_hours_c numeric,
                avg_cloudcover_ski_hours_pct numeric,
                avg_wind_speed_ski_hours_kmh numeric,
                max_wind_gust_ski_hours_kmh numeric,
                snowfall_ski_day_cm numeric,
                rain_ski_day_mm numeric,
                snowfall_prev_day_cm numeric,
                rain_prev_day_mm numeric,
                hours_above_freezing_prev_day int,
                freeze_thaw_cycles_prev_48h int,
                ice_risk_flag boolean,
                wind_hold_risk_flag boolean,
                PRIMARY KEY (resort, forecast_run_at, target_ski_date)
            );
            """
        )

        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                if not hourly_df.empty:
                    hdf = hourly_df.drop_duplicates(subset=["resort", "forecast_run_at", "time_local"])
                    hvals = [
                        (
                            row["resort"],
                            row["forecast_run_at"],
                            row["time_local"].to_pydatetime() if hasattr(row["time_local"], "to_pydatetime") else row["time_local"],
                            row["target_ski_date"],
                            int(row["hour_local"]),
                            row["temperature_2m_c"],
                            row["cloudcover_pct"],
                            row["wind_speed_10m_kmh"],
                            row["wind_gusts_10m_kmh"],
                            row["precipitation_mm"],
                            row["snowfall_cm"],
                        )
                        for _, row in hdf.iterrows()
                    ]
                    execute_values(
                        cur,
                        """
                        INSERT INTO openmeteo_hourly (
                            resort, forecast_run_at, time_local, target_ski_date, hour_local,
                            temperature_2m_c, cloudcover_pct, wind_speed_10m_kmh,
                            wind_gusts_10m_kmh, precipitation_mm, snowfall_cm
                        ) VALUES %s
                        ON CONFLICT (resort, forecast_run_at, time_local) DO NOTHING;
                        """,
                        hvals,
                        page_size=500,
                    )

                if not daily_df.empty:
                    ddf = daily_df.drop_duplicates(subset=["resort", "forecast_run_at", "target_ski_date"])
                    dvals = [
                        (
                            row["resort"],
                            row["forecast_run_at"],
                            row["target_ski_date"],
                            row["avg_temp_ski_hours_c"],
                            row["min_temp_ski_hours_c"],
                            row["max_temp_ski_hours_c"],
                            row["avg_cloudcover_ski_hours_pct"],
                            row["avg_wind_speed_ski_hours_kmh"],
                            row["max_wind_gust_ski_hours_kmh"],
                            row["snowfall_ski_day_cm"],
                            row["rain_ski_day_mm"],
                            row["snowfall_prev_day_cm"],
                            row["rain_prev_day_mm"],
                            row["hours_above_freezing_prev_day"],
                            row["freeze_thaw_cycles_prev_48h"],
                            int(row["ice_risk_flag"]) if row["ice_risk_flag"] is not None else None,
                            int(row["wind_hold_risk_flag"]) if row["wind_hold_risk_flag"] is not None else None,
                        )
                        for _, row in ddf.iterrows()
                    ]
                    execute_values(
                        cur,
                        """
                        INSERT INTO openmeteo_features_daily (
                            resort, forecast_run_at, target_ski_date,
                            avg_temp_ski_hours_c, min_temp_ski_hours_c, max_temp_ski_hours_c,
                            avg_cloudcover_ski_hours_pct, avg_wind_speed_ski_hours_kmh,
                            max_wind_gust_ski_hours_kmh, snowfall_ski_day_cm, rain_ski_day_mm,
                            snowfall_prev_day_cm, rain_prev_day_mm, hours_above_freezing_prev_day,
                            freeze_thaw_cycles_prev_48h, ice_risk_flag, wind_hold_risk_flag
                        ) VALUES %s
                        ON CONFLICT (resort, forecast_run_at, target_ski_date) DO NOTHING;
                        """,
                        dvals,
                        page_size=200,
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    jobs_by_resort = {}
    for jid in JOB_IDS:
        resort, dataset = jid.split(":", 1)
        jobs_by_resort.setdefault(resort, []).append((jid, dataset))

    for resort, jobs in jobs_by_resort.items():
        with TaskGroup(group_id=slug(resort)):
            for jid, dataset in jobs:
                ds = slug(dataset)
                group = slug(resort)
                run_anchor_local = f"run__{ds}"
                skip_local = f"skip__{ds}"

                branch = BranchPythonOperator(
                    task_id=f"choose__{ds}",
                    python_callable=choose_task_id,
                    op_kwargs={
                        "job_id": jid,
                        "run_task_id": f"{group}.{run_anchor_local}",
                        "skip_task_id": f"{group}.{skip_local}",
                    },
                )

                run_anchor = EmptyOperator(task_id=run_anchor_local)
                skip = EmptyOperator(task_id=skip_local)
                fp = extract.override(task_id=f"extract__{ds}")(jid)
                ok_fp = validate.override(task_id=f"validate__{ds}")(jid, fp)
                load = load_to_db.override(task_id=f"load__{ds}")(jid, ok_fp)

                branch >> [run_anchor, skip]
                run_anchor >> fp >> ok_fp >> load
