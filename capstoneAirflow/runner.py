import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


# ---------- Open-Meteo Resort Coordinates ----------

RESORT_COORDS = {
    "Loon": (44.0369, -71.6290),
    "SundayRiver": (44.4709, -70.8567),
    "Sugarloaf": (45.0314, -70.3131),
    "Killington": (43.6266, -72.7960),
    "Pico": (43.6615, -72.8429),
    "Sugarbush": (44.1353, -72.8936),
    "Stratton": (43.1134, -72.9068),
}


# ---------- Job config ----------

@dataclass(frozen=True)
class Job:
    resort: str
    dataset: str
    kind: str            # "reportpal" | "killington" | "mtnpowder" | "openmeteo"
    url: str
    params: Optional[Dict[str, Any]] = None


# NOTE: job_id format = "Resort:dataset"
JOBS: Dict[str, Job] = {
    # Reportpal
    "Loon:reportpal": Job(
        "Loon", "reportpal", "reportpal",
        "https://www.loonmtn.com/api/reportpal",
        {"resortName": "lm", "useReportPal": "true"},
    ),
    "SundayRiver:reportpal": Job(
        "SundayRiver", "reportpal", "reportpal",
        "https://www.sundayriver.com/api/reportpal",
        {"resortName": "sr", "useReportPal": "true"},
    ),
    "Sugarloaf:reportpal": Job(
        "Sugarloaf", "reportpal", "reportpal",
        "https://www.sugarloaf.com/api/reportpal",
        {"resortName": "sl", "useReportPal": "true"},
    ),

    # Killington
    "Killington:trails": Job(
        "Killington", "trails", "killington",
        "https://api.killington.com/api/v1/dor/drupal/trails",
    ),
    "Killington:lifts": Job(
        "Killington", "lifts", "killington",
        "https://api.killington.com/api/v1/dor/drupal/lifts",
    ),
    "Killington:snow_reports": Job(
        "Killington", "snow_reports", "killington",
        "https://api.killington.com/api/v1/dor/drupal/snow-reports",
        {"sort": "date", "direction": "desc"},
    ),

    # Pico
    "Pico:trails": Job(
        "Pico", "trails", "killington",
        "https://api.picomountain.com/api/v1/dor/drupal/trails",
    ),
    "Pico:lifts": Job(
        "Pico", "lifts", "killington",
        "https://api.picomountain.com/api/v1/dor/drupal/lifts",
    ),
    "Pico:snow_reports": Job(
        "Pico", "snow_reports", "killington",
        "https://api.picomountain.com/api/v1/dor/drupal/snow-reports",
        {"sort": "date", "direction": "desc"},
    ),

    # MountainPowder feeds
    "Sugarbush:feed": Job(
        "Sugarbush", "feed", "mtnpowder",
        "https://mtnpowder.com/feed/v3.json",
        {"bearer_token": "NcCvnKYGAOLTfkvAuQm6Z03zvHUSo64ctInVBbhUcr4", "resortId[]": 70},
    ),
    "Stratton:feed": Job(
        "Stratton", "feed", "mtnpowder",
        "https://mtnpowder.com/feed/v3.json",
        {"bearer_token": "hPtaTVkbuyZQnrxvru4ApfpXnS21PJO3eTKdibDoLZE", "resortId[]": 1},
    ),

    # Open-Meteo 7-day Forecast
    "Loon:forecast": Job(
        "Loon", "forecast", "openmeteo",
        "https://api.open-meteo.com/v1/forecast",
    ),
    "SundayRiver:forecast": Job(
        "SundayRiver", "forecast", "openmeteo",
        "https://api.open-meteo.com/v1/forecast",
    ),
    "Sugarloaf:forecast": Job(
        "Sugarloaf", "forecast", "openmeteo",
        "https://api.open-meteo.com/v1/forecast",
    ),
    "Killington:forecast": Job(
        "Killington", "forecast", "openmeteo",
        "https://api.open-meteo.com/v1/forecast",
    ),
    "Pico:forecast": Job(
        "Pico", "forecast", "openmeteo",
        "https://api.open-meteo.com/v1/forecast",
    ),
    "Sugarbush:forecast": Job(
        "Sugarbush", "forecast", "openmeteo",
        "https://api.open-meteo.com/v1/forecast",
    ),
    "Stratton:forecast": Job(
        "Stratton", "forecast", "openmeteo",
        "https://api.open-meteo.com/v1/forecast",
    ),
}


# ---------- Common helpers ----------

def local_suffix() -> str:
    # Local time (EST/EDT automatically)
    return datetime.now().strftime("%Y%m%d_%H%M")


def fetch_json(job: Job) -> Any:

    if job.kind == "openmeteo":
        lat, lon = RESORT_COORDS[job.resort]

        today = date.today()
        end_date = today + timedelta(days=7)

        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join([
                "temperature_2m",
                "apparent_temperature",
                "precipitation",
                "snowfall",
                "snow_depth",
                "wind_speed_10m",
                "wind_gusts_10m",
                "relativehumidity_2m",
                "freezing_level_height",
                "cloudcover",
                "surface_pressure"
            ]),
            "daily": ",".join([
                "temperature_2m_max",
                "temperature_2m_min",
                "snowfall_sum",
                "precipitation_sum",
                "wind_speed_10m_max"
            ]),
            "start_date": today.isoformat(),
            "end_date": end_date.isoformat(),
            "timezone": "auto"
        }

        r = requests.get(job.url, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.json()

    r = requests.get(job.url, params=job.params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def save_raw(job: Job, payload: Any, base_dir: str = "/opt/airflow/project/data/raw") -> Path:
    out_dir = Path(base_dir) / job.resort.lower()
    out_dir.mkdir(parents=True, exist_ok=True)

    fp = out_dir / f"{job.resort}_{job.dataset}_{local_suffix()}.json"
    with fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    return fp


def load_raw(file_path: str) -> Any:
    return json.loads(Path(file_path).read_text(encoding="utf-8"))


# ---------- Validation (light MVP) ----------

def validate_reportpal(payload: Any) -> Tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "reportpal: payload is not an object"
    for k in ("name", "updated", "operations", "currentConditions"):
        if k not in payload:
            return False, f"reportpal: missing key {k}"
    return True, "ok"


def validate_killington(payload: Any) -> Tuple[bool, str]:
    # Killington/Pico endpoints often return a list of objects
    if isinstance(payload, list):
        if len(payload) == 0:
            return False, "killington: empty list"
        if not isinstance(payload[0], dict):
            return False, "killington: list items are not objects"
        return True, "ok"
    if isinstance(payload, dict) and len(payload) > 0:
        return True, "ok"
    return False, "killington: unexpected payload type/empty"


def validate_mtnpowder(payload: Any) -> Tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "mtnpowder: payload is not an object"
    if "LastUpdate" not in payload:
        return False, "mtnpowder: missing top-level key LastUpdate"
    if "Resorts" not in payload:
        return False, "mtnpowder: missing top-level key Resorts"
    resorts = payload["Resorts"]
    if not isinstance(resorts, list) or len(resorts) == 0:
        return False, "mtnpowder: Resorts is empty or not a list"
    return True, "ok"


def validate_openmeteo(payload: Any) -> Tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "openmeteo: payload not object"
    if "hourly" not in payload:
        return False, "openmeteo: missing hourly"
    if "daily" not in payload:
        return False, "openmeteo: missing daily"
    return True, "ok"


def validate_payload(kind: str, payload: Any) -> Tuple[bool, str]:
    if kind == "reportpal":
        return validate_reportpal(payload)
    if kind == "killington":
        return validate_killington(payload)
    if kind == "mtnpowder":
        return validate_mtnpowder(payload)
    if kind == "openmeteo":
        return validate_openmeteo(payload)
    return False, f"unknown kind: {kind}"


# ---------- CLI commands ----------

def cmd_extract(job_id: str) -> str:
    job = JOBS[job_id]
    payload = fetch_json(job)
    fp = save_raw(job, payload)
    return str(fp)


def cmd_validate(job_id: str, file_path: str) -> None:
    job = JOBS[job_id]
    payload = load_raw(file_path)
    ok, reason = validate_payload(job.kind, payload)
    if not ok:
        raise ValueError(f"INVALID {job_id}: {reason}")
    print(f"VALID {job_id}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Unified resort raw ingestion + validation")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ext = sub.add_parser("extract", help="Fetch JSON and save raw snapshot")
    p_ext.add_argument("--job", required=True, choices=JOBS.keys())

    p_val = sub.add_parser("validate", help="Validate a saved raw snapshot")
    p_val.add_argument("--job", required=True, choices=JOBS.keys())
    p_val.add_argument("--file", required=True)

    args = parser.parse_args()

    if args.command == "extract":
        return cmd_extract(args.job)
    return cmd_validate(args.job, args.file)


if __name__ == "__main__":
    sys.exit(main())
