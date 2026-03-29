from datetime import datetime
import re

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

import sys
sys.path.append("/opt/airflow/project")
from runner import (  # noqa: E402
    JOBS,
    cmd_extract,
    cmd_validate,
    load_raw,
    parse_resort_family_boyne,
    parse_resort_family_killington,
    parse_resort_family_mountainpowder,
)

JOB_FAMILIES = {
    "Loon": ["Loon:reportpal"],
    "SundayRiver": ["SundayRiver:reportpal"],
    "Sugarloaf": ["Sugarloaf:reportpal"],
    "Killington": ["Killington:lifts", "Killington:trails", "Killington:snow_reports"],
    "Pico": ["Pico:lifts", "Pico:trails", "Pico:snow_reports"],
    "Sugarbush": ["Sugarbush:feed"],
    "Stratton": ["Stratton:feed"],
}

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
    dag_id="ski_ingest_dq",
    start_date=datetime(2026, 2, 16, tzinfo=local_tz),
    schedule="0 7,19 * * *",
    catchup=False,
    default_args={"retries": 2},
    tags=["ski", "resort", "dq"],
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
    def load_single_resort_row(job_id: str, file_path: str) -> None:
        job = JOBS[job_id]
        payload = load_raw(file_path)

        if job.kind == "reportpal":
            row = parse_resort_family_boyne(payload=payload, resort=job.resort, raw_path=file_path)
        elif job.kind == "mtnpowder":
            row = parse_resort_family_mountainpowder(payload=payload, resort=job.resort, raw_path=file_path)
        else:
            raise ValueError(f"Unsupported single-payload kind: {job.kind}")

        hook = PostgresHook(postgres_conn_id="ski_pg")
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS resort_status_daily (
                resort text NOT NULL,
                report_date date NOT NULL,
                raw_path text,
                resort_updated_at timestamptz NOT NULL,
                trails_open int,
                trails_total int,
                open_trails_pct int,
                lifts_open int,
                lifts_total int,
                open_lifts_pct int,
                mountain_report_text text,
                PRIMARY KEY (resort, report_date, resort_updated_at)
            );
            """
        )
        hook.run(
            """
            INSERT INTO resort_status_daily (
                resort, report_date, raw_path, resort_updated_at,
                trails_open, trails_total, open_trails_pct,
                lifts_open, lifts_total, open_lifts_pct,
                mountain_report_text
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (resort, report_date, resort_updated_at) DO NOTHING;
            """,
            parameters=[
                row["resort"], row["report_date"], row["raw_path"], row["resort_updated_at"],
                row["trails_open"], row["trails_total"], row["open_trails_pct"],
                row["lifts_open"], row["lifts_total"], row["open_lifts_pct"],
                row["mountain_report_text"],
            ],
        )

    @task
    def load_killington_family(resort: str, lifts_fp: str, trails_fp: str, snow_fp: str) -> None:
        row = parse_resort_family_killington(
            lifts_payload=load_raw(lifts_fp),
            trails_payload=load_raw(trails_fp),
            snow_payload=load_raw(snow_fp),
            resort=resort,
            raw_path=" | ".join([lifts_fp, trails_fp, snow_fp]),
        )

        hook = PostgresHook(postgres_conn_id="ski_pg")
        hook.run(
            """
            CREATE TABLE IF NOT EXISTS resort_status_daily (
                resort text NOT NULL,
                report_date date NOT NULL,
                raw_path text,
                resort_updated_at timestamptz NOT NULL,
                trails_open int,
                trails_total int,
                open_trails_pct int,
                lifts_open int,
                lifts_total int,
                open_lifts_pct int,
                mountain_report_text text,
                PRIMARY KEY (resort, report_date, resort_updated_at)
            );
            """
        )
        hook.run(
            """
            INSERT INTO resort_status_daily (
                resort, report_date, raw_path, resort_updated_at,
                trails_open, trails_total, open_trails_pct,
                lifts_open, lifts_total, open_lifts_pct,
                mountain_report_text
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (resort, report_date, resort_updated_at) DO NOTHING;
            """,
            parameters=[
                row["resort"], row["report_date"], row["raw_path"], row["resort_updated_at"],
                row["trails_open"], row["trails_total"], row["open_trails_pct"],
                row["lifts_open"], row["lifts_total"], row["open_lifts_pct"],
                row["mountain_report_text"],
            ],
        )

    for resort, job_ids in JOB_FAMILIES.items():
        with TaskGroup(group_id=slug(resort)):
            if len(job_ids) == 1:
                jid = job_ids[0]
                run_anchor_local = "run"
                skip_local = "skip"
                group = slug(resort)
                branch = BranchPythonOperator(
                    task_id="choose",
                    python_callable=choose_task_id,
                    op_kwargs={
                        "job_id": jid,
                        "run_task_id": f"{group}.{run_anchor_local}",
                        "skip_task_id": f"{group}.{skip_local}",
                    },
                )
                run_anchor = EmptyOperator(task_id=run_anchor_local)
                skip = EmptyOperator(task_id=skip_local)
                fp = extract.override(task_id="extract")(jid)
                ok_fp = validate.override(task_id="validate")(jid, fp)
                load = load_single_resort_row.override(task_id="load")(jid, ok_fp)
                branch >> [run_anchor, skip]
                run_anchor >> fp >> ok_fp >> load
            else:
                group = slug(resort)
                branch = BranchPythonOperator(
                    task_id="choose",
                    python_callable=choose_task_id,
                    op_kwargs={
                        "job_id": f"{resort}:lifts",
                        "run_task_id": f"{group}.run",
                        "skip_task_id": f"{group}.skip",
                    },
                )
                run_anchor = EmptyOperator(task_id="run")
                skip = EmptyOperator(task_id="skip")

                lifts_fp = extract.override(task_id="extract_lifts")(f"{resort}:lifts")
                lifts_ok = validate.override(task_id="validate_lifts")(f"{resort}:lifts", lifts_fp)
                trails_fp = extract.override(task_id="extract_trails")(f"{resort}:trails")
                trails_ok = validate.override(task_id="validate_trails")(f"{resort}:trails", trails_fp)
                snow_fp = extract.override(task_id="extract_snow_reports")(f"{resort}:snow_reports")
                snow_ok = validate.override(task_id="validate_snow_reports")(f"{resort}:snow_reports", snow_fp)

                load = load_killington_family.override(task_id="load")(resort, lifts_ok, trails_ok, snow_ok)
                branch >> [run_anchor, skip]
                run_anchor >> [lifts_fp, trails_fp, snow_fp]
                lifts_fp >> lifts_ok
                trails_fp >> trails_ok
                snow_fp >> snow_ok
                [lifts_ok, trails_ok, snow_ok] >> load
