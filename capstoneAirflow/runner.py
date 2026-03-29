import os
import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable

import pandas as pd
import requests

from langchain_core.prompts import ChatPromptTemplate
from langchain_mistralai import ChatMistralAI

from pydantic import BaseModel, Field

from dotenv import load_dotenv

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
LOCAL_TZ = "America/New_York"
SKI_HOURS_START = 9
SKI_HOURS_END = 16


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


@dataclass(frozen=True)
class Job:
    resort: str
    dataset: str
    kind: str
    url: str
    params: Optional[Dict[str, Any]] = None


JOBS: Dict[str, Job] = {
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
    "Loon:forecast": Job("Loon", "forecast", "openmeteo", "https://api.open-meteo.com/v1/forecast"),
    "SundayRiver:forecast": Job("SundayRiver", "forecast", "openmeteo", "https://api.open-meteo.com/v1/forecast"),
    "Sugarloaf:forecast": Job("Sugarloaf", "forecast", "openmeteo", "https://api.open-meteo.com/v1/forecast"),
    "Killington:forecast": Job("Killington", "forecast", "openmeteo", "https://api.open-meteo.com/v1/forecast"),
    "Pico:forecast": Job("Pico", "forecast", "openmeteo", "https://api.open-meteo.com/v1/forecast"),
    "Sugarbush:forecast": Job("Sugarbush", "forecast", "openmeteo", "https://api.open-meteo.com/v1/forecast"),
    "Stratton:forecast": Job("Stratton", "forecast", "openmeteo", "https://api.open-meteo.com/v1/forecast"),
}


def local_suffix() -> str:
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
                "cloudcover",
                "wind_speed_10m",
                "wind_gusts_10m",
                "precipitation",
                "snowfall",
            ]),
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,wind_speed_10m_max",
            "start_date": today.isoformat(),
            "end_date": end_date.isoformat(),
            "timezone": "auto",
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


# ---------- Validation ----------
def validate_reportpal(payload: Any) -> Tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "reportpal: payload is not an object"
    for k in ("updated", "currentConditions"):
        if k not in payload:
            return False, f"reportpal: missing key {k}"
    return True, "ok"


def validate_killington(payload: Any) -> Tuple[bool, str]:
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
    if "Resorts" not in payload:
        return False, "mtnpowder: missing Resorts"
    resorts = payload.get("Resorts")
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


# ---------- Shared parsing helpers ----------
def safe_pct(numerator: Any, denominator: Any) -> Optional[int]:
    try:
        if numerator is None or denominator in (None, 0, 0.0, "0"):
            return None
        return round((float(numerator) / float(denominator)) * 100)
    except Exception:
        return None


def to_iso_utc(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        ts = pd.to_datetime(value, utc=False)
        if ts.tzinfo is None:
            ts = ts.tz_localize(LOCAL_TZ, nonexistent="shift_forward", ambiguous="infer")
        return ts.tz_convert("UTC").isoformat()
    except Exception:
        return None


def to_report_date(value: Any):
    iso_utc = to_iso_utc(value)
    if iso_utc is None:
        return None
    ts = pd.Timestamp(iso_utc)
    return ts.tz_convert(LOCAL_TZ).date()


def epoch_to_iso_utc(value: Any) -> Optional[str]:
    try:
        if value in (None, ""):
            return None
        ts = pd.to_datetime(int(value), unit="s", utc=True)
        return ts.isoformat()
    except Exception:
        return None


# ---------- Ski resort parsers ----------
def parse_resort_family_boyne(payload: dict, resort: str = None, raw_path: str = None) -> dict:
    resort_updated_at = payload.get("updated")
    report_date = to_report_date(resort_updated_at)

    current_conditions = payload.get("currentConditions", {}) or {}
    resortwide = current_conditions.get("resortwide", {}) or {}

    trails_total = resortwide.get("numTrailsTotal")
    trails_open = resortwide.get("numTrailsOpen")
    lifts_total = resortwide.get("numLiftsTotal")
    lifts_open = resortwide.get("numLiftsOpen")

    comments = payload.get("comments", {}) or {}
    comment_list = comments.get("comment", []) or []
    mountain_report_text = None
    if comment_list and isinstance(comment_list[0], dict):
        mountain_report_text = comment_list[0].get("text")

    return {
        "resort": resort,
        "report_date": report_date,
        "raw_path": raw_path,
        "resort_updated_at": to_iso_utc(resort_updated_at),
        "trails_open": trails_open,
        "trails_total": trails_total,
        "open_trails_pct": safe_pct(trails_open, trails_total),
        "lifts_open": lifts_open,
        "lifts_total": lifts_total,
        "open_lifts_pct": safe_pct(lifts_open, lifts_total),
        "mountain_report_text": mountain_report_text,
    }


def parse_killington_lifts(payload: Any) -> dict:
    items = payload if isinstance(payload, list) else []
    filtered = [x for x in items if isinstance(x, dict)]
    lifts_total = len(filtered)
    lifts_open = sum(1 for x in filtered if str(x.get("status", "")).lower() in {"open", "expected"})

    updated_candidates = [to_iso_utc(x.get("updated")) for x in filtered if x.get("updated")]
    updated_candidates = [x for x in updated_candidates if x]
    lifts_updated_at = max(updated_candidates) if updated_candidates else None

    return {
        "lifts_open": lifts_open,
        "lifts_total": lifts_total,
        "lifts_updated_at": lifts_updated_at,
    }


def parse_killington_trails(payload: Any) -> dict:
    items = payload if isinstance(payload, list) else []
    filtered = []
    for x in items:
        if not isinstance(x, dict):
            continue
        if str(x.get("season", "")).lower() != "winter":
            continue
        if str(x.get("type", "")).lower() != "alpine_trail":
            continue
        if x.get("include") is False:
            continue
        filtered.append(x)

    trails_total = len(filtered)
    trails_open = sum(1 for x in filtered if str(x.get("status", "")).lower() == "open")

    updated_candidates = [to_iso_utc(x.get("updated")) for x in filtered if x.get("updated")]
    updated_candidates = [x for x in updated_candidates if x]
    trails_updated_at = max(updated_candidates) if updated_candidates else None

    return {
        "trails_open": trails_open,
        "trails_total": trails_total,
        "trails_updated_at": trails_updated_at,
    }


def parse_killington_snow_report(payload: Any) -> dict:
    items = payload if isinstance(payload, list) else []
    first = items[0] if items and isinstance(items[0], dict) else {}
    report_text = first.get("report") or first.get("Report") or first.get("text")
    snow_report_updated_at = epoch_to_iso_utc(first.get("updated")) or to_iso_utc(first.get("updated"))
    return {
        "mountain_report_text": report_text,
        "snow_report_updated_at": snow_report_updated_at,
    }


def parse_resort_family_killington(
    lifts_payload: Any,
    trails_payload: Any,
    snow_payload: Any,
    resort: str = None,
    raw_path: str = None,
) -> dict:
    lifts = parse_killington_lifts(lifts_payload)
    trails = parse_killington_trails(trails_payload)
    snow = parse_killington_snow_report(snow_payload)

    candidates = [
        lifts.get("lifts_updated_at"),
        trails.get("trails_updated_at"),
        snow.get("snow_report_updated_at"),
    ]
    candidates = [x for x in candidates if x]
    resort_updated_at = max(candidates) if candidates else None

    return {
        "resort": resort,
        "report_date": to_report_date(resort_updated_at),
        "raw_path": raw_path,
        "resort_updated_at": resort_updated_at,
        "trails_open": trails.get("trails_open"),
        "trails_total": trails.get("trails_total"),
        "open_trails_pct": safe_pct(trails.get("trails_open"), trails.get("trails_total")),
        "lifts_open": lifts.get("lifts_open"),
        "lifts_total": lifts.get("lifts_total"),
        "open_lifts_pct": safe_pct(lifts.get("lifts_open"), lifts.get("lifts_total")),
        "mountain_report_text": snow.get("mountain_report_text"),
    }


def parse_resort_family_mountainpowder(payload: dict, resort: str = None, raw_path: str = None) -> dict:
    resorts = payload.get("Resorts", []) or []
    resort_obj = resorts[0] if resorts and isinstance(resorts[0], dict) else {}
    snow_report = resort_obj.get("SnowReport", {}) or {}

    trails_total = snow_report.get("TotalTrails")
    trails_open = snow_report.get("TotalOpenTrails")
    lifts_total = snow_report.get("TotalLifts")
    lifts_open = snow_report.get("TotalOpenLifts")
    mountain_report_text = snow_report.get("Report")
    resort_updated_at = snow_report.get("LastUpdate")

    return {
        "resort": resort,
        "report_date": to_report_date(resort_updated_at),
        "raw_path": raw_path,
        "resort_updated_at": to_iso_utc(resort_updated_at),
        "trails_open": trails_open,
        "trails_total": trails_total,
        "open_trails_pct": safe_pct(trails_open, trails_total),
        "lifts_open": lifts_open,
        "lifts_total": lifts_total,
        "open_lifts_pct": safe_pct(lifts_open, lifts_total),
        "mountain_report_text": mountain_report_text,
    }


# ---------- Open-Meteo parsers ----------
def extract_forecast_run_at_from_filename(fp: str | Path):
    fp = Path(fp)
    m = re.search(r"(\d{8}_\d{4})\.json$", fp.name)
    if not m:
        return None
    ts = pd.to_datetime(m.group(1), format="%Y%m%d_%H%M")
    return ts.tz_localize(LOCAL_TZ).tz_convert("UTC").isoformat()


def rain_proxy_mm(temp_c, precip_mm):
    if temp_c is None or precip_mm is None:
        return 0.0
    try:
        return float(precip_mm) if float(temp_c) > 0 and float(precip_mm) > 0 else 0.0
    except Exception:
        return 0.0


def count_freeze_thaw_cycles(series_temp_c: pd.Series) -> int:
    s = pd.to_numeric(series_temp_c, errors="coerce").dropna().reset_index(drop=True)
    if len(s) < 2:
        return 0
    prev = s.shift(1)
    crossings = ((prev <= 0) & (s > 0)) | ((prev > 0) & (s <= 0))
    return int(crossings.sum())


def parse_openmeteo_hourly(payload: dict, resort: str, raw_path: str) -> pd.DataFrame:
    forecast_run_at = extract_forecast_run_at_from_filename(raw_path)
    timezone_name = payload.get("timezone", LOCAL_TZ)
    hourly = payload.get("hourly", {}) or {}

    df = pd.DataFrame({
        "time_raw": hourly.get("time", []) or [],
        "temperature_2m_c": hourly.get("temperature_2m", []) or [],
        "cloudcover_pct": hourly.get("cloudcover", []) or [],
        "wind_speed_10m_kmh": hourly.get("wind_speed_10m", []) or [],
        "wind_gusts_10m_kmh": hourly.get("wind_gusts_10m", []) or [],
        "precipitation_mm": hourly.get("precipitation", []) or [],
        "snowfall_cm": hourly.get("snowfall", []) or [],
    })

    if df.empty:
        return df

    df["time_local"] = pd.to_datetime(df["time_raw"])
    if df["time_local"].dt.tz is None:
        df["time_local"] = df["time_local"].dt.tz_localize(
            timezone_name,
            nonexistent="shift_forward",
            ambiguous="infer",
        )

    df["resort"] = resort
    df["forecast_run_at"] = forecast_run_at
    df["target_ski_date"] = df["time_local"].dt.date
    df["hour_local"] = df["time_local"].dt.hour

    return df[
        [
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
    ].copy()


def build_openmeteo_features_daily(hourly_df: pd.DataFrame) -> pd.DataFrame:
    if hourly_df.empty:
        return pd.DataFrame(columns=[
            "resort", "forecast_run_at", "target_ski_date",
            "avg_temp_ski_hours_c", "min_temp_ski_hours_c", "max_temp_ski_hours_c",
            "avg_cloudcover_ski_hours_pct", "avg_wind_speed_ski_hours_kmh", "max_wind_gust_ski_hours_kmh",
            "snowfall_ski_day_cm", "rain_ski_day_mm", "snowfall_prev_day_cm", "rain_prev_day_mm",
            "hours_above_freezing_prev_day", "freeze_thaw_cycles_prev_48h", "ice_risk_flag", "wind_hold_risk_flag",
        ])

    df = hourly_df.copy()
    numeric_cols = [
        "temperature_2m_c", "cloudcover_pct", "wind_speed_10m_kmh",
        "wind_gusts_10m_kmh", "precipitation_mm", "snowfall_cm",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["rain_proxy_mm"] = [rain_proxy_mm(t, p) for t, p in zip(df["temperature_2m_c"], df["precipitation_mm"])]
    df["date_ts"] = pd.to_datetime(df["target_ski_date"])

    keys = ["resort", "forecast_run_at", "target_ski_date"]
    out_rows: List[Dict[str, Any]] = []

    for (resort, forecast_run_at, target_ski_date), day_df in df.groupby(keys, dropna=False):
        ski_hours_df = day_df[(day_df["hour_local"] >= SKI_HOURS_START) & (day_df["hour_local"] <= SKI_HOURS_END)]
        prev_day = pd.Timestamp(target_ski_date) - pd.Timedelta(days=1)
        prev_2day_start = pd.Timestamp(target_ski_date) - pd.Timedelta(days=2)
        prev_day_df = df[df["date_ts"] == prev_day]
        prev_48h_df = df[(df["date_ts"] >= prev_2day_start) & (df["date_ts"] < pd.Timestamp(target_ski_date))]

        max_wind_gust = ski_hours_df["wind_gusts_10m_kmh"].max() if not ski_hours_df.empty else None
        freeze_cycles = count_freeze_thaw_cycles(prev_48h_df["temperature_2m_c"]) if not prev_48h_df.empty else 0
        hours_above_freezing_prev_day = int((prev_day_df["temperature_2m_c"] > 0).sum()) if not prev_day_df.empty else 0
        rain_prev_day_mm = float(prev_day_df["rain_proxy_mm"].sum()) if not prev_day_df.empty else 0.0

        out_rows.append({
            "resort": resort,
            "forecast_run_at": forecast_run_at,
            "target_ski_date": target_ski_date,
            "avg_temp_ski_hours_c": ski_hours_df["temperature_2m_c"].mean() if not ski_hours_df.empty else None,
            "min_temp_ski_hours_c": ski_hours_df["temperature_2m_c"].min() if not ski_hours_df.empty else None,
            "max_temp_ski_hours_c": ski_hours_df["temperature_2m_c"].max() if not ski_hours_df.empty else None,
            "avg_cloudcover_ski_hours_pct": ski_hours_df["cloudcover_pct"].mean() if not ski_hours_df.empty else None,
            "avg_wind_speed_ski_hours_kmh": ski_hours_df["wind_speed_10m_kmh"].mean() if not ski_hours_df.empty else None,
            "max_wind_gust_ski_hours_kmh": max_wind_gust,
            "snowfall_ski_day_cm": float(day_df["snowfall_cm"].sum()) if not day_df.empty else 0.0,
            "rain_ski_day_mm": float(day_df["rain_proxy_mm"].sum()) if not day_df.empty else 0.0,
            "snowfall_prev_day_cm": float(prev_day_df["snowfall_cm"].sum()) if not prev_day_df.empty else 0.0,
            "rain_prev_day_mm": rain_prev_day_mm,
            "hours_above_freezing_prev_day": hours_above_freezing_prev_day,
            "freeze_thaw_cycles_prev_48h": freeze_cycles,
            "ice_risk_flag": bool(rain_prev_day_mm > 0 and freeze_cycles > 0),
            "wind_hold_risk_flag": bool(max_wind_gust is not None and max_wind_gust >= 65),
        })

    return pd.DataFrame(out_rows)


# ---------- CLI ----------
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

    try:
        if args.command == "extract":
            path = cmd_extract(args.job)
            print(path)
            return 0
        cmd_validate(args.job, args.file)
        return 0
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 1

# ============================================================
# REDDIT / APIFY FUNCTIONS START HERE
# ============================================================

def get_project_root(start_path: Optional[Path] = None) -> Path:
    """
    Walk upward until we find a directory containing runner.py.
    Fallback to cwd if not found.
    """
    start = start_path or Path.cwd()
    for p in [start] + list(start.parents):
        if (p / "runner.py").exists():
            return p
    return Path.cwd()


def load_project_env() -> None:
    """
    Load .env from the same project directory as runner.py.
    Safe to call multiple times.
    """
    try:
        from dotenv import load_dotenv
    except Exception:
        return

    env_path = Path(__file__).resolve().parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)


REDDIT_QUERY_TERMS = [
    "sunday river conditions",
    "sugarbush conditions",
    "killington conditions",
    "loon conditions",
    "sugarloaf conditions",
    "pico conditions",
    "stratton conditions",
]


def build_reddit_apify_payload(
    timeframe: str = "week",
    max_posts_per_query: int = 100,
    subreddit: str = "icecoast",
) -> Dict[str, Any]:
    """
    Build the Apify Reddit Scraper payload.
    """
    return {
        "includeRaw": False,
        "mode": "search",
        "search": {
            "commentsHighEngagementFilterPosts": False,
            "commentsHighEngagementMinComments": 5,
            "commentsHighEngagementMinScore": 5,
            "commentsMaxDepth": 3,
            "commentsMaxTopLevel": 30,
            "commentsMode": "all",
            "includeNsfw": False,
            "maxPostsPerQuery": max_posts_per_query,
            "queries": REDDIT_QUERY_TERMS,
            "restrictToSubreddit": subreddit,
            "selfPostsOnly": False,
            "sort": "new",
            "strictEnabled": False,
            "strictTerms": REDDIT_QUERY_TERMS,
            "timeframe": timeframe,
        },
    }


def fetch_reddit_apify_items(
    apify_token: Optional[str] = None,
    timeframe: str = "week",
    max_posts_per_query: int = 100,
    subreddit: str = "icecoast",
    timeout: int = 360,
) -> List[Dict[str, Any]]:
    """
    Call Apify and return the JSON list of reddit items.
    """
    load_project_env()

    token = apify_token or os.getenv("APIFY_TOKEN")
    if not token:
        raise ValueError("APIFY_TOKEN not found in environment or .env")

    url = (
        "https://api.apify.com/v2/acts/"
        "spry_wholemeal~reddit-scraper/"
        "run-sync-get-dataset-items"
    )

    payload = build_reddit_apify_payload(
        timeframe=timeframe,
        max_posts_per_query=max_posts_per_query,
        subreddit=subreddit,
    )

    r = requests.post(
        f"{url}?token={token}",
        json=payload,
        timeout=timeout,
    )
    r.raise_for_status()

    items = r.json()
    if not isinstance(items, list):
        raise ValueError("Apify response is not a list of reddit items")

    return items


def save_reddit_raw(
    payload: Any,
    base_dir: str = "/opt/airflow/project/data/raw/reddit",
    prefix: str = "dataset_reddit-scraper",
) -> Path:
    """
    Save raw reddit Apify response to disk.
    """
    out_dir = Path(base_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")[:-3]
    fp = out_dir / f"{prefix}_{ts}.json"

    with fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return fp


def cmd_extract_reddit(
    timeframe: str = "week",
    max_posts_per_query: int = 100,
    subreddit: str = "icecoast",
    base_dir: str = "/opt/airflow/project/data/raw/reddit",
) -> str:
    """
    Extract one reddit batch from Apify and save raw JSON.
    """
    items = fetch_reddit_apify_items(
        timeframe=timeframe,
        max_posts_per_query=max_posts_per_query,
        subreddit=subreddit,
    )
    fp = save_reddit_raw(items, base_dir=base_dir)
    return str(fp)


def load_reddit_raw(file_path: str) -> List[Dict[str, Any]]:
    payload = load_raw(file_path)
    if not isinstance(payload, list):
        raise ValueError("Expected reddit raw payload to be a list")
    return payload


def build_source_item_key_from_parts(
    post_id: Optional[str],
    comment_id: Optional[str],
) -> Optional[str]:
    """
    For comments:  post_id_comment_id
    For posts:     post_id_post
    """
    post = None if post_id is None else str(post_id).strip()
    comment = None if comment_id is None else str(comment_id).strip()

    if comment:
        return f"{post}_{comment}"
    if post:
        return f"{post}_post"
    return None


def normalize_reddit_items(items: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Normalize one Apify batch into the columns needed downstream.
    No within-batch dedup here by design.
    """
    df = pd.DataFrame(items).copy()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "source_item_key",
                "post_id",
                "comment_id",
                "record_type",
                "subreddit",
                "author",
                "title",
                "text",
                "permalink",
                "url",
                "created_utc_iso",
                "scraped_at_iso",
                "parent_id",
                "parent_type",
            ]
        )

    required_cols = [
        "post_id",
        "comment_id",
        "record_type",
        "subreddit",
        "author",
        "title",
        "text",
        "permalink",
        "url",
        "created_utc_iso",
        "scraped_at_iso",
        "parent_id",
        "parent_type",
    ]
    for c in required_cols:
        if c not in df.columns:
            df[c] = None

    df["source_item_key"] = df.apply(
        lambda row: build_source_item_key_from_parts(
            row.get("post_id"),
            row.get("comment_id"),
        ),
        axis=1,
    )

    df = df[
        [
            "source_item_key",
            "post_id",
            "comment_id",
            "record_type",
            "subreddit",
            "author",
            "title",
            "text",
            "permalink",
            "url",
            "created_utc_iso",
            "scraped_at_iso",
            "parent_id",
            "parent_type",
        ]
    ].copy()

    return df


def normalize_reddit_file(file_path: str) -> pd.DataFrame:
    items = load_reddit_raw(file_path)
    return normalize_reddit_items(items)


def write_df_json_records(df: pd.DataFrame, out_path: str) -> str:
    """
    Save dataframe as JSON records for passing between DAG tasks via file path.
    """
    out_fp = Path(out_path)
    out_fp.parent.mkdir(parents=True, exist_ok=True)
    df.to_json(out_fp, orient="records", force_ascii=False, indent=2)
    return str(out_fp)


def read_df_json_records(file_path: str) -> pd.DataFrame:
    fp = Path(file_path)
    if not fp.exists():
        raise FileNotFoundError(file_path)
    return pd.read_json(fp, orient="records")


def prepare_reddit_new_rows(
    raw_file_path: str,
    existing_keys: Iterable[str],
) -> pd.DataFrame:
    """
    Normalize the batch and keep only rows not already present in reddit_labeled.
    """
    df = normalize_reddit_file(raw_file_path)
    existing_key_set = set(k for k in existing_keys if k is not None)

    if df.empty:
        return df

    new_df = df[~df["source_item_key"].isin(existing_key_set)].copy()
    new_df = new_df[new_df["source_item_key"].notna()].reset_index(drop=True)
    return new_df


def _build_reddit_labeling_prompt():
    try:
        from langchain_core.prompts import ChatPromptTemplate
    except Exception as e:
        raise ImportError("langchain_core is required for reddit labeling") from e

    return ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
You are labeling Reddit posts and comments about ski conditions.

Return:
- report_type
- resort
- resolved_date
- date_resolution_source
- sentiment_label

Definitions:
- field_report: describes actual experienced or observed ski conditions on a real ski day
- forecast_opinion: discusses expected or upcoming ski conditions
- other: not a useful ski-conditions signal

Resort rules:
- Only choose from:
  Loon, SundayRiver, Sugarloaf, Killington, Pico, Sugarbush, Stratton
- Use text first, then title
- Do not guess without evidence

Date rules:
- resolved_date should be the ski day being discussed
- Use explicit dates first
- Use relative expressions like today, yesterday, tomorrow, this weekend relative to the created timestamp
- For field_report with no explicit or relative date, use fallback_post_date
- For forecast_opinion with vague timing and no clear date, return null and use unknown

Sentiment rules:
- positive = conditions described favorably
- negative = conditions described unfavorably
- neutral = mixed, unclear, or not really about conditions

Important:
- Judge ski conditions only, not unrelated tone
- Be conservative
                """.strip(),
            ),
            (
                "human",
                """
Created timestamp: {created_utc_iso}

Post title:
{title}

Reddit text:
{text}
                """.strip(),
            ),
        ]
    )


def _build_mistral_reddit_chain(
    model_name: str = "mistral-large-latest",
    timeout: int = 30,
):
    try:
        from pydantic import BaseModel, Field
        from langchain_mistralai import ChatMistralAI
    except Exception as e:
        raise ImportError(
            "pydantic and langchain_mistralai are required for reddit labeling"
        ) from e

    load_project_env()
    mistral_api_key = os.getenv("MISTRAL_API_KEY")
    if not mistral_api_key:
        raise ValueError("MISTRAL_API_KEY not found in environment or .env")

    class RedditLabel(BaseModel):
        report_type: str = Field(description="field_report / forecast_opinion / other")
        resort: Optional[str] = Field(default=None)
        resolved_date: Optional[str] = Field(default=None)
        date_resolution_source: str = Field(
            description="explicit_text / relative_text / fallback_post_date / unknown"
        )
        sentiment_label: str = Field(description="positive / negative / neutral")

    prompt = _build_reddit_labeling_prompt()

    llm = ChatMistralAI(
        model=model_name,
        temperature=0,
        api_key=mistral_api_key,
        timeout=timeout,
    )

    structured_llm = llm.with_structured_output(RedditLabel)
    return prompt | structured_llm


def label_reddit_rows_with_mistral(
    df: pd.DataFrame,
    model_name: str = "mistral-large-latest",
    timeout: int = 30,
    progress_every: int = 25,
) -> pd.DataFrame:
    """
    Label only the new reddit rows.
    Returns item-level labeled dataframe ready for reddit_labeled insert.
    """
    if df.empty:
        out = df.copy()
        out["report_type"] = pd.Series(dtype="object")
        out["resort"] = pd.Series(dtype="object")
        out["resolved_date"] = pd.Series(dtype="object")
        out["date_resolution_source"] = pd.Series(dtype="object")
        out["sentiment_label"] = pd.Series(dtype="object")
        out["labeling_model"] = pd.Series(dtype="object")
        out["labeled_at"] = pd.Series(dtype="datetime64[ns, UTC]")
        return out

    chain = _build_mistral_reddit_chain(
        model_name=model_name,
        timeout=timeout,
    )

    labeled_rows: List[Dict[str, Any]] = []
    total = len(df)

    for i, (_, row) in enumerate(df.iterrows(), 1):
        title = row["title"] if pd.notna(row["title"]) else ""
        text = row["text"] if pd.notna(row["text"]) else ""
        created_utc_iso = row["created_utc_iso"] if pd.notna(row["created_utc_iso"]) else ""

        result = chain.invoke(
            {
                "created_utc_iso": created_utc_iso,
                "title": title,
                "text": text,
            }
        )

        labeled_rows.append(
            {
                "source_item_key": row["source_item_key"],
                "report_type": result.report_type,
                "resort": result.resort,
                "resolved_date": result.resolved_date,
                "date_resolution_source": result.date_resolution_source,
                "sentiment_label": result.sentiment_label,
            }
        )

        if progress_every and i % progress_every == 0:
            print(f"[reddit_labeling] processed {i}/{total}")

    labels_df = pd.DataFrame(labeled_rows)
    out = df.merge(labels_df, on="source_item_key", how="left")
    out["labeling_model"] = model_name
    out["labeled_at"] = pd.Timestamp.utcnow()
    return out


def aggregate_reddit_resort_day_signals(reddit_labeled_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build sparse resort-day signals from labeled item-level rows.

    Valid rows:
    - report_type != other
    - resort not null
    - resolved_date not null
    """
    if reddit_labeled_df.empty:
        return pd.DataFrame(
            columns=[
                "resort",
                "ski_date",
                "field_report_count",
                "forecast_opinion_count",
                "positive_count",
                "negative_count",
                "neutral_count",
                "net_sentiment",
                "weighted_signal_score",
                "signal_present",
                "updated_at",
            ]
        )

    valid_df = reddit_labeled_df[
        (reddit_labeled_df["report_type"] != "other")
        & (reddit_labeled_df["resort"].notna())
        & (reddit_labeled_df["resolved_date"].notna())
    ].copy()

    if valid_df.empty:
        return pd.DataFrame(
            columns=[
                "resort",
                "ski_date",
                "field_report_count",
                "forecast_opinion_count",
                "positive_count",
                "negative_count",
                "neutral_count",
                "net_sentiment",
                "weighted_signal_score",
                "signal_present",
                "updated_at",
            ]
        )

    sentiment_map = {
        "positive": 1,
        "negative": -1,
        "neutral": 0,
    }
    report_weight_map = {
        "field_report": 2,
        "forecast_opinion": 1,
    }

    valid_df["sentiment_score"] = valid_df["sentiment_label"].map(sentiment_map).fillna(0)
    valid_df["report_weight"] = valid_df["report_type"].map(report_weight_map).fillna(0)
    valid_df["weighted_contribution"] = (
        valid_df["sentiment_score"] * valid_df["report_weight"]
    )

    valid_df["resolved_date"] = pd.to_datetime(valid_df["resolved_date"]).dt.date

    agg_df = (
        valid_df.groupby(["resort", "resolved_date"], as_index=False)
        .agg(
            field_report_count=("report_type", lambda s: int((s == "field_report").sum())),
            forecast_opinion_count=("report_type", lambda s: int((s == "forecast_opinion").sum())),
            positive_count=("sentiment_label", lambda s: int((s == "positive").sum())),
            negative_count=("sentiment_label", lambda s: int((s == "negative").sum())),
            neutral_count=("sentiment_label", lambda s: int((s == "neutral").sum())),
            net_sentiment=("sentiment_score", "sum"),
            weighted_signal_score=("weighted_contribution", "sum"),
        )
        .rename(columns={"resolved_date": "ski_date"})
    )

    agg_df["signal_present"] = 1
    agg_df["updated_at"] = pd.Timestamp.utcnow()

    int_cols = [
        "field_report_count",
        "forecast_opinion_count",
        "positive_count",
        "negative_count",
        "neutral_count",
        "net_sentiment",
        "weighted_signal_score",
        "signal_present",
    ]
    for c in int_cols:
        agg_df[c] = agg_df[c].astype(int)

    return agg_df


def validate_reddit_labeled_df(df: pd.DataFrame) -> Tuple[bool, str]:
    required_cols = [
        "source_item_key",
        "record_type",
        "report_type",
        "sentiment_label",
        "labeling_model",
        "labeled_at",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        return False, f"reddit_labeled_df missing columns: {missing}"
    return True, "ok"


def validate_reddit_agg_df(df: pd.DataFrame) -> Tuple[bool, str]:
    required_cols = [
        "resort",
        "ski_date",
        "field_report_count",
        "forecast_opinion_count",
        "positive_count",
        "negative_count",
        "neutral_count",
        "net_sentiment",
        "weighted_signal_score",
        "signal_present",
        "updated_at",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        return False, f"reddit_agg_df missing columns: {missing}"
    return True, "ok"

if __name__ == "__main__":
    raise SystemExit(main())


