from datetime import datetime
import pendulum

from airflow import DAG
from airflow.decorators import task

# import from your runner.py (mounted into /opt/airflow/project)
import sys
sys.path.append("/opt/airflow/project")
from runner import cmd_extract, cmd_validate  # noqa: E402
import re
from airflow.utils.task_group import TaskGroup

JOB_IDS = [
    "Loon:reportpal",
    "SundayRiver:reportpal",
    "Sugarloaf:reportpal",

    "Killington:trails",
    "Killington:lifts",
    "Killington:snow_reports",

    "Pico:trails",
    "Pico:lifts",
    "Pico:snow_reports",

    "Sugarbush:feed",
    "Stratton:feed",
]

local_tz = pendulum.timezone("America/New_York")

with DAG(
    dag_id="ski_ingest_dq",
    start_date=datetime(2026, 2, 16, tzinfo=local_tz),
    schedule="0 7,19 * * *",   # 7am + 7pm NY time (EST/EDT auto)
    catchup=False,
    default_args={"retries": 2},
    tags=["ski", "raw", "dq"],
) as dag:

    @task
    def extract(job_id: str) -> str:
        # returns filepath
        return cmd_extract(job_id)

    @task
    def validate(job_id: str, file_path: str) -> None:
        # raises exception if invalid -> task fails -> retry
        cmd_validate(job_id, file_path)

    def slug(s: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_.-]+", "_", s)

    # group jobs by resort
    jobs_by_resort = {}
    for job_id in JOB_IDS:
        resort, dataset = job_id.split(":", 1)
        jobs_by_resort.setdefault(resort, []).append((job_id, dataset))

    # create tasks grouped per resort, with readable ids
    for resort, jobs in jobs_by_resort.items():
        with TaskGroup(group_id=slug(resort)):
            for job_id, dataset in jobs:
                ds = slug(dataset)
                fp = extract.override(task_id=f"extract__{ds}")(job_id)
                validate.override(task_id=f"validate__{ds}")(job_id, fp)

