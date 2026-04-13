import os
import json
import uuid
from datetime import datetime, date, timedelta
from decimal import Decimal

import numpy as np
import ollama
import psycopg2
import redis
from sentence_transformers import SentenceTransformer
import pandas as pd

# --------------------------
# Config
# --------------------------
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

RESORT_MODELS = {
    "Killington": ["phi4-mini", "qwen3:4b"],
    "Pico": ["qwen3:4b", "phi4-mini"],
    "Sugarloaf": ["phi4-mini", "qwen3:4b"],
    "SundayRiver": ["qwen3:4b", "phi4-mini"],
    "Loon": ["phi4-mini", "qwen3:4b"],
    "Sugarbush": ["qwen3:4b", "phi4-mini"],
    "Stratton": ["phi4-mini", "qwen3:4b"],
}

RESORT_SUMMARY_MODEL = "phi4-mini"

REDIS_HOST = "localhost"
REDIS_PORT = 6379
INDEX_NAME = "ski_index"
CHUNK_SIZE = 300
OVERLAP = 50

DB_PARAMS = {
    "dbname": "ski_pipeline",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": 5432,
}

CORPUS_FOLDER = "Ski Analyst Resources"

# --------------------------
# Clients
# --------------------------
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
embedding_model = SentenceTransformer(EMBEDDING_MODEL)


# --------------------------
# Postgres Connection
# --------------------------
def get_connection():
    return psycopg2.connect(**DB_PARAMS)


# --------------------------
# JSON Helpers
# --------------------------
def make_json_safe(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    return obj


def safe_json_dump(data):
    safe = make_json_safe(data)
    if isinstance(safe, (dict, list)):
        return json.dumps(safe, ensure_ascii=False)
    return str(safe)


# --------------------------
# Embedding / RAG Helpers
# --------------------------
def get_embedding(text: str) -> np.ndarray:
    vec = embedding_model.encode(text)
    return np.array(vec).astype(np.float32)


def split_text(text: str):
    words = text.split()
    chunks = []
    step = max(1, CHUNK_SIZE - OVERLAP)
    for i in range(0, len(words), step):
        chunk = " ".join(words[i:i + CHUNK_SIZE]).strip()
        if chunk:
            chunks.append(chunk)
    return chunks


def create_index():
    try:
        r.ft(INDEX_NAME).info()
        return
    except Exception:
        pass

    from redis.commands.search.field import TagField, TextField, VectorField
    from redis.commands.search.index_definition import IndexDefinition, IndexType

    schema = (
        TagField("resort"),
        TagField("day"),
        TextField("source"),
        TextField("chunk"),
        VectorField(
            "embedding",
            "HNSW",
            {
                "TYPE": "FLOAT32",
                "DIM": 384,
                "DISTANCE_METRIC": "COSINE",
            },
        ),
    )

    r.ft(INDEX_NAME).create_index(
        fields=schema,
        definition=IndexDefinition(prefix=["ski:"], index_type=IndexType.HASH),
    )


def ingest_corpus(corpus_folder=CORPUS_FOLDER):
    """
    Ingest TXT, PDF, and DOCX files into Redis vector index.
    """
    create_index()

    if not os.path.isdir(corpus_folder):
        raise FileNotFoundError(f"Corpus folder not found: {corpus_folder}")

    files = [
        f for f in os.listdir(corpus_folder)
        if f.lower().endswith((".pdf", ".txt", ".docx"))
    ]

    for file in files:
        path = os.path.join(corpus_folder, file)
        content = None

        try:
            if file.lower().endswith(".txt"):
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()

            elif file.lower().endswith(".pdf"):
                import fitz
                doc = fitz.open(path)
                content = "\n".join(page.get_text() for page in doc)

            elif file.lower().endswith(".docx"):
                from docx import Document
                doc = Document(path)
                content = "\n".join(p.text for p in doc.paragraphs if p.text.strip())

        except Exception as e:
            print(f"Failed to read {file}: {e}")
            continue

        if not content or not content.strip():
            continue

        for chunk in split_text(content):
            emb = get_embedding(chunk)
            key = f"ski:corpus:{uuid.uuid4()}"
            r.hset(
                key,
                mapping={
                    "resort": "all",
                    "day": "all",
                    "source": file,
                    "chunk": chunk,
                    "embedding": emb.tobytes(),
                },
            )


def search_chunks(query: str, top_k: int = 4):
    """
    Vector retrieval from Redis.
    """
    create_index()
    query_vec = get_embedding(query)

    q = f'*=>[KNN {top_k} @embedding $vec AS score]'

    try:
        from redis.commands.search.query import Query

        redis_query = (
            Query(q)
            .sort_by("score")
            .return_fields("source", "chunk", "score")
            .paging(0, top_k)
            .dialect(2)
        )

        results = r.ft(INDEX_NAME).search(
            redis_query,
            query_params={"vec": query_vec.tobytes()},
        )

        context = []
        for doc in getattr(results, "docs", []):
            context.append(
                {
                    "source": getattr(doc, "source", ""),
                    "chunk": getattr(doc, "chunk", ""),
                    "score": getattr(doc, "score", None),
                }
            )
        return context

    except Exception as e:
        print(f"Redis vector search failed: {e}")
        return []


# --------------------------
# Prompt Helpers
# --------------------------
STRICT_POLICY_TEXT = """
You are a ski conditions analyst.

Evaluate whether a given resort-day is a good day to go skiing.

Use structured resort and weather data as the primary evidence.
Use retrieved ski-policy context as supporting guidance.
Use Reddit only as a weak confirmation signal.
Ignore missing factors and use only available evidence.

Rate these factors as Low, Medium, or High:
- Terrain availability
- Snow quality
- Ice risk
- Lift hold risk
- Temperature comfort
- Rain impact

Return exactly this format:
Terrain availability: Low | Medium | High
Snow quality: Low | Medium | High
Ice risk: Low | Medium | High
Lift hold risk: Low | Medium | High
Temperature comfort: Low | Medium | High
Rain impact: Low | Medium | High
Final decision: Go | No Go
Confidence: Low | Medium | High
Rationale: 3 to 5 sentences.
""".strip()


def build_prompt(resort: str, day: str, day_data: dict, retrieved_context: list):
    daily = day_data.get("openmeteo_daily") or {}
    conditions = day_data.get("conditions_snapshot") or {}
    reddit = day_data.get("reddit_signal") or {}

    context_block = ""
    if retrieved_context:
        joined = "\n\n".join(
            f"[Source: {c.get('source', '')}]\n{c.get('chunk', '')}"
            for c in retrieved_context
        )
        context_block = f"\nSupporting knowledge base context:\n{joined}\n"

    reddit_block = ""
    if reddit and reddit.get("signal_present") in (1, True, "1"):
        reddit_block = f"""
Reddit daily signal:
- Signal present: {reddit.get("signal_present")}
- Weighted signal score: {reddit.get("weighted_signal_score", "N/A")}
""".strip()

    prompt = f"""
{STRICT_POLICY_TEXT}
{context_block}



Terrain and operations:
- Trails open: {conditions.get("trails_open", "N/A")}
- Open trails pct: {conditions.get("open_trails_pct", "N/A")}
- Lifts open: {conditions.get("lifts_open", "N/A")}
- Open lifts pct: {conditions.get("open_lifts_pct", "N/A")}

Weather and comfort:
- Avg temp during ski hours (C): {daily.get("avg_temp_ski_hours_c", "N/A")}
- Min temp during ski hours (C): {daily.get("min_temp_ski_hours_c", "N/A")}
- Max temp during ski hours (C): {daily.get("max_temp_ski_hours_c", "N/A")}
- Wind chill (C): {daily.get("wind_chill_c", "N/A")}

Wind and operations risk:
- Avg wind speed during ski hours (km/h): {daily.get("avg_wind_speed_ski_hours_kmh", "N/A")}
- Max wind gust during ski hours (km/h): {daily.get("max_wind_gust_ski_hours_kmh", "N/A")}
- Wind hold risk flag: {daily.get("wind_hold_risk_flag", "N/A")}

Snow, rain, and surface quality:
- Snowfall on ski day (cm): {daily.get("snowfall_ski_day_cm", "N/A")}
- Rain on ski day (mm): {daily.get("rain_ski_day_mm", "N/A")}
- Snowfall previous day (cm): {daily.get("snowfall_prev_day_cm", "N/A")}
- Rain previous day (mm): {daily.get("rain_prev_day_mm", "N/A")}
- Hours above freezing previous day: {daily.get("hours_above_freezing_prev_day", "N/A")}
- Freeze-thaw cycles previous 48h: {daily.get("freeze_thaw_cycles_prev_48h", "N/A")}
- Ice risk flag: {daily.get("ice_risk_flag", "N/A")}

{reddit_block}
""".strip()

    return prompt


# --------------------------
# DB Fetchers
# --------------------------
def fetch_reddit_signal(resort: str, ski_date: date):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                resort,
                ski_date,
                signal_present,
                weighted_signal_score
            FROM reddit_resort_day_signals
            WHERE resort = %s
              AND ski_date = %s
            """,
            (resort, ski_date),
        )
        row = cur.fetchone()
        if not row:
            return {}
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))
    except Exception:
        return {}
    finally:
        cur.close()
        conn.close()


def fetch_resort_full_snapshot(resort, start_ts, end_ts):
    """
    Keeps the same public function name used by app.py,
    but now also returns trail difficulty counts for the dashboard.
    """
    conn = get_connection()
    data = {}
    cursor = conn.cursor()

    # Daily weather
    try:
        cursor.execute(
            """
            WITH latest_weather AS (
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
            )
            SELECT
                resort,
                target_ski_date,
                forecast_run_at,
                avg_temp_ski_hours_c,
                min_temp_ski_hours_c,
                max_temp_ski_hours_c,
                avg_cloudcover_ski_hours_pct,
                ROUND(avg_wind_speed_ski_hours_kmh::numeric, 2) AS avg_wind_speed_ski_hours_kmh,
                max_wind_gust_ski_hours_kmh,
                snowfall_ski_day_cm,
                rain_ski_day_mm,
                snowfall_prev_day_cm,
                rain_prev_day_mm,
                hours_above_freezing_prev_day,
                freeze_thaw_cycles_prev_48h,
                ice_risk_flag,
                wind_hold_risk_flag,
                ROUND(
                    (
                        13.12
                        + 0.6215 * avg_temp_ski_hours_c
                        - 11.37 * POWER(avg_wind_speed_ski_hours_kmh, 0.16)
                        + 0.3965 * avg_temp_ski_hours_c * POWER(avg_wind_speed_ski_hours_kmh, 0.16)
                    )::numeric,
                    2
                ) AS wind_chill_c
            FROM latest_weather
            WHERE resort = %s
              AND target_ski_date = %s
            """,
            (resort, start_ts.date()),
        )
        row = cursor.fetchone()
        data["openmeteo_daily"] = (
            dict(zip([desc[0] for desc in cursor.description], row)) if row else {}
        )
    except Exception as e:
        print("openmeteo_daily error:", e)
        conn.rollback()
        data["openmeteo_daily"] = {}

    # Hourly weather for charts in frontend
    try:
        cursor.execute(
            """
            SELECT
                resort,
                forecast_run_at,
                time_local,
                target_ski_date,
                hour_local,
                temperature_2m_c,
                cloudcover_pct,
                wind_speed_10m_kmh,
                wind_gusts_10m_kmh,
                precipitation_mm,
                snowfall_cm
            FROM openmeteo_hourly
            WHERE resort = %s
              AND time_local >= %s
              AND time_local < %s
            ORDER BY time_local ASC
            """,
            (resort, start_ts, end_ts),
        )
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]

        if rows:
            hourly_df = pd.DataFrame(rows, columns=cols)

            hourly_df["time_local"] = pd.to_datetime(hourly_df["time_local"])
            hourly_df["forecast_run_at"] = pd.to_datetime(hourly_df["forecast_run_at"])

            hourly_df = hourly_df.sort_values(["time_local", "forecast_run_at"])
            hourly_df = hourly_df.drop_duplicates(subset=["time_local"], keep="last")

            data["openmeteo_hourly"] = hourly_df.to_dict(orient="records")
        else:
            data["openmeteo_hourly"] = []
    except Exception as e:
        print("openmeteo_hourly error:", e)
        conn.rollback()
        data["openmeteo_hourly"] = []

    # Latest resort-day snapshot + difficulty counts
    try:
        cursor.execute(
            """
            WITH latest_resort AS (
                SELECT
                    resort,
                    report_date,
                    resort_updated_at,
                    trails_open,
                    trails_total,
                    open_trails_pct,
                    lifts_open,
                    lifts_total,
                    open_lifts_pct,
                    mountain_report_text,
                    novice_open,
                    novice_total,
                    intermediate_open,
                    intermediate_total,
                    advanced_open,
                    advanced_total,
                    expert_open,
                    expert_total,
                    ROW_NUMBER() OVER (
                        PARTITION BY resort, report_date
                        ORDER BY resort_updated_at DESC
                    ) AS rn
                FROM resort_status_daily
                WHERE report_date = %s
            )
            SELECT
                resort,
                report_date,
                resort_updated_at,
                trails_open,
                trails_total,
                open_trails_pct,
                lifts_open,
                lifts_total,
                open_lifts_pct,
                mountain_report_text,
                novice_open,
                novice_total,
                intermediate_open,
                intermediate_total,
                advanced_open,
                advanced_total,
                expert_open,
                expert_total
            FROM latest_resort
            WHERE resort = %s
              AND rn = 1
            """,
            (start_ts.date(), resort),
        )
        row = cursor.fetchone()

        snapshot = dict(zip([desc[0] for desc in cursor.description], row)) if row else {}

        data["conditions_snapshot"] = snapshot

        data["trails_by_difficulty"] = {
            "novice_open": snapshot.get("novice_open"),
            "novice_total": snapshot.get("novice_total"),
            "intermediate_open": snapshot.get("intermediate_open"),
            "intermediate_total": snapshot.get("intermediate_total"),
            "advanced_open": snapshot.get("advanced_open"),
            "advanced_total": snapshot.get("advanced_total"),
            "expert_open": snapshot.get("expert_open"),
            "expert_total": snapshot.get("expert_total"),
        } if snapshot else {}

    except Exception as e:
        print("resort snapshot error:", e)
        conn.rollback()
        data["conditions_snapshot"] = {}
        data["trails_by_difficulty"] = {}

    cursor.close()
    conn.close()

    # Reddit daily signal
    try:
        data["reddit_signal"] = fetch_reddit_signal(resort, start_ts.date())
    except Exception as e:
        print("reddit_signal error:", e)
        data["reddit_signal"] = {}

    return data

# --------------------------
# LLM Output Helpers
# --------------------------
def generate_analysis(resort, day, day_data, model):
    """
    Main strict prompt call for one analyst model.
    """
    daily = day_data.get("openmeteo_daily", {}) or {}
    conditions = day_data.get("conditions_snapshot", {}) or {}

    retrieval_query = f"""
Ski day decision policy for evaluating resort conditions.
Resort: {resort}
Date: {day}
Open trails pct: {conditions.get('open_trails_pct', 'N/A')}
Open lifts pct: {conditions.get('open_lifts_pct', 'N/A')}
Snowfall ski day cm: {daily.get('snowfall_ski_day_cm', 'N/A')}
Rain ski day mm: {daily.get('rain_ski_day_mm', 'N/A')}
Rain previous day mm: {daily.get('rain_prev_day_mm', 'N/A')}
Freeze thaw cycles prev 48h: {daily.get('freeze_thaw_cycles_prev_48h', 'N/A')}
Wind gust kmh: {daily.get('max_wind_gust_ski_hours_kmh', 'N/A')}
Wind hold risk flag: {daily.get('wind_hold_risk_flag', 'N/A')}
Ice risk flag: {daily.get('ice_risk_flag', 'N/A')}
""".strip()

    context = search_chunks(retrieval_query, top_k=4)
    prompt = build_prompt(resort, day, day_data, context)

    response = ollama.chat(
        model=model,
        messages=[{"role": "user", "content": prompt}],
    )
    return response["message"]["content"]


def summarize_resort(resort, analyst_outputs):
    """
    Keep frontend contract same: returns a single summary string.
    """
    combined = "\n\n".join(
        [f"Analyst {i+1} output:\n{out}" for i, out in enumerate(analyst_outputs)]
    )

    prompt = f"""
You are combining multiple ski analyst outputs for {resort}.

Each analyst already used the same factor structure.
Synthesize them into one final answer.

Rules:
- Preserve the same exact output format.
- Choose the most defensible factor labels based on analyst agreement.
- If analysts disagree slightly, use the majority judgment.
- If analysts disagree materially, lower confidence.
- Do not invent new factors.
- Keep the rationale to 3 to 5 sentences.

Return exactly this format:
Terrain availability: Low | Medium | High
Snow quality: Low | Medium | High
Ice risk: Low | Medium | High
Lift hold risk: Low | Medium | High
Temperature comfort: Low | Medium | High
Rain impact: Low | Medium | High
Final decision: Go | No Go
Confidence: Low | Medium | High
Rationale: 3 to 5 sentences.

Analyst outputs:
{combined}
""".strip()

    response = ollama.chat(
        model=RESORT_SUMMARY_MODEL,
        messages=[{"role": "user", "content": prompt}],
    )
    return response["message"]["content"]


def pick_best_resort(resort_results):
    """
    Kept for compatibility with app.py global comparison flow.
    """
    combined = "\n\n".join(
        [f"{r['resort']}:\n{r['final_resort_decision']}" for r in resort_results]
    )

    prompt = f"""
You are comparing multiple ski resort recommendations for the same day.

Each resort already has a structured recommendation.
Choose the single best resort for skiing that day.

Return exactly:
Best resort: <resort name>
Confidence: Low | Medium | High
Rationale: 3 sentences

Resort decisions:
{combined}
""".strip()

    response = ollama.chat(
        model=RESORT_SUMMARY_MODEL,
        messages=[{"role": "user", "content": prompt}],
    )
    return response["message"]["content"]


# --------------------------
# Public Evaluation Functions
# --------------------------
def evaluate_resort_day(resort, day, day_data):
    """
    Same function signature expected by app.py.
    """
    models = RESORT_MODELS.get(
        resort,
        ["phi4-mini", "qwen3:4b"],
    )

    analyst_outputs = [
        generate_analysis(resort, day, day_data, m)
        for m in models
    ]

    final_decision = summarize_resort(resort, analyst_outputs)

    return {
        "resort": resort,
        "day": day,
        "analyst_outputs": analyst_outputs,
        "final_resort_decision": final_decision,
    }


def run_full_daily_analysis(day, all_resort_data):
    """
    Same function signature expected by app.py.
    """
    results = []
    for resort, data in all_resort_data.items():
        day_data = data.get(day, {})
        results.append(evaluate_resort_day(resort, day, day_data))

    best = pick_best_resort(results)

    return {
        "per_resort": results,
        "best_resort_decision": best,
    }
