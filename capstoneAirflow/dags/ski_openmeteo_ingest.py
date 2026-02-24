from datetime import datetime
import re
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

import sys
sys.path.append("/opt/airflow/project")
from runner import cmd_extract, cmd_validate, JOBS, load_raw  # noqa: E402


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


def _to_float_or_none(x):
    try:
        return None if x is None else float(x)
    except Exception:
        return None


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
        hook = PostgresHook(postgres_conn_id="ski_pg")
        ingested_at = pendulum.now("UTC").to_iso8601_string()

        # -----------------------------
        # Tables: hourly + daily
        # -----------------------------
        hook.run("""
        CREATE TABLE IF NOT EXISTS openmeteo_hourly (
          ingested_at              timestamptz NOT NULL,
          resort                   text        NOT NULL,
          raw_path                 text        NOT NULL,
          time_utc                 timestamptz NOT NULL,

          temperature_2m           numeric,
          apparent_temperature     numeric,
          precipitation            numeric,
          snowfall                 numeric,
          snow_depth               numeric,
          wind_speed_10m           numeric,
          wind_gusts_10m           numeric,
          relativehumidity_2m      numeric,
          freezing_level_height    numeric,
          cloudcover               numeric,
          surface_pressure         numeric,

          UNIQUE(resort, time_utc)
        );
        """)

        hook.run("""
        CREATE TABLE IF NOT EXISTS openmeteo_daily (
          ingested_at              timestamptz NOT NULL,
          resort                   text        NOT NULL,
          raw_path                 text        NOT NULL,
          date_utc                 date        NOT NULL,

          temperature_2m_max       numeric,
          temperature_2m_min       numeric,
          snowfall_sum             numeric,
          precipitation_sum        numeric,
          wind_speed_10m_max       numeric,

          UNIQUE(resort, date_utc)
        );
        """)

        # -----------------------------
        # Parse HOURLY
        # -----------------------------
        hourly = payload.get("hourly") or {}
        h_time = hourly.get("time") or []

        # Each variable is an array aligned with time[]
        def h_arr(key):
            return hourly.get(key) or []

        temperature_2m = h_arr("temperature_2m")
        apparent_temperature = h_arr("apparent_temperature")
        precipitation = h_arr("precipitation")
        snowfall = h_arr("snowfall")
        snow_depth = h_arr("snow_depth")
        wind_speed_10m = h_arr("wind_speed_10m")
        wind_gusts_10m = h_arr("wind_gusts_10m")
        relativehumidity_2m = h_arr("relativehumidity_2m")
        freezing_level_height = h_arr("freezing_level_height")
        cloudcover = h_arr("cloudcover")
        surface_pressure = h_arr("surface_pressure")

        hourly_rows = []
        n = len(h_time)
        # Defensive: only iterate over the shortest aligned length
        n = min(
            n,
            len(temperature_2m),
            len(apparent_temperature),
            len(precipitation),
            len(snowfall),
            len(snow_depth),
            len(wind_speed_10m),
            len(wind_gusts_10m),
            len(relativehumidity_2m),
            len(freezing_level_height),
            len(cloudcover),
            len(surface_pressure),
        )

        for i in range(n):
            # Open-Meteo returns ISO strings; parse with pendulum to UTC
            # If timezone is "auto", times include offset; pendulum.parse handles it.
            t = pendulum.parse(h_time[i]).in_timezone("UTC")
            hourly_rows.append([
                ingested_at, job.resort, file_path, t.to_iso8601_string(),
                _to_float_or_none(temperature_2m[i]),
                _to_float_or_none(apparent_temperature[i]),
                _to_float_or_none(precipitation[i]),
                _to_float_or_none(snowfall[i]),
                _to_float_or_none(snow_depth[i]),
                _to_float_or_none(wind_speed_10m[i]),
                _to_float_or_none(wind_gusts_10m[i]),
                _to_float_or_none(relativehumidity_2m[i]),
                _to_float_or_none(freezing_level_height[i]),
                _to_float_or_none(cloudcover[i]),
                _to_float_or_none(surface_pressure[i]),
            ])

        if hourly_rows:
            hook.insert_rows(
                table="openmeteo_hourly",
                rows=hourly_rows,
                target_fields=[
                    "ingested_at", "resort", "raw_path", "time_utc",
                    "temperature_2m", "apparent_temperature", "precipitation", "snowfall",
                    "snow_depth", "wind_speed_10m", "wind_gusts_10m", "relativehumidity_2m",
                    "freezing_level_height", "cloudcover", "surface_pressure",
                ],
                replace=False,
                commit_every=1000,
            )

        # -----------------------------
        # Parse DAILY
        # -----------------------------
        daily = payload.get("daily") or {}
        d_time = daily.get("time") or []

        def d_arr(key):
            return daily.get(key) or []

        temperature_2m_max = d_arr("temperature_2m_max")
        temperature_2m_min = d_arr("temperature_2m_min")
        snowfall_sum = d_arr("snowfall_sum")
        precipitation_sum = d_arr("precipitation_sum")
        wind_speed_10m_max = d_arr("wind_speed_10m_max")

        daily_rows = []
        m = len(d_time)
        m = min(
            m,
            len(temperature_2m_max),
            len(temperature_2m_min),
            len(snowfall_sum),
            len(precipitation_sum),
            len(wind_speed_10m_max),
        )

        for i in range(m):
            # daily time is YYYY-MM-DD
            d = pendulum.parse(d_time[i]).date()
            daily_rows.append([
                ingested_at, job.resort, file_path, d.to_date_string(),
                _to_float_or_none(temperature_2m_max[i]),
                _to_float_or_none(temperature_2m_min[i]),
                _to_float_or_none(snowfall_sum[i]),
                _to_float_or_none(precipitation_sum[i]),
                _to_float_or_none(wind_speed_10m_max[i]),
            ])

        if daily_rows:
            hook.insert_rows(
                table="openmeteo_daily",
                rows=daily_rows,
                target_fields=[
                    "ingested_at", "resort", "raw_path", "date_utc",
                    "temperature_2m_max", "temperature_2m_min",
                    "snowfall_sum", "precipitation_sum", "wind_speed_10m_max",
                ],
                replace=False,
                commit_every=1000,
            )

    # -----------------------------
    # TaskGroups (same style as your other DAG)
    # -----------------------------
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

                run_anchor_id = f"{group}.{run_anchor_local}"
                skip_id = f"{group}.{skip_local}"

                branch = BranchPythonOperator(
                    task_id=f"choose__{ds}",
                    python_callable=choose_task_id,
                    op_kwargs={
                        "job_id": jid,
                        "run_task_id": run_anchor_id,
                        "skip_task_id": skip_id,
                    },
                )

                run_anchor = EmptyOperator(task_id=run_anchor_local)
                skip = EmptyOperator(task_id=skip_local)

                fp = extract.override(task_id=f"extract__{ds}")(jid)
                ok_fp = validate.override(task_id=f"validate__{ds}")(jid, fp)
                load = load_to_db.override(task_id=f"load__{ds}")(jid, ok_fp)

                branch >> [run_anchor, skip]
                run_anchor >> fp >> ok_fp >> load