import os
import json
import uuid
import numpy as np
import redis
import ollama
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sentence_transformers import SentenceTransformer

QUESTION = "Is this a good day to go skiing?"
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

RESORT_MODELS = {
    "Killington": ["mistral:latest", "llama2", "phi3:latest", "mistral:latest"],
    "Pico": ["llama2", "mistral:latest", "phi3:latest", "llama2"],
    "Sugarloaf": ["mistral:latest", "phi3:latest", "llama2", "phi3:latest"],
    "SundayRiver": ["llama2", "phi3:latest", "mistral:latest", "llama2"],
    "Loon": ["mistral:latest", "llama2", "phi3:latest", "phi3:latest"],
    "Sugarbush": ["phi3:latest", "mistral:latest", "llama2", "mistral:latest"],
    "Stratton": ["llama2", "phi3:latest", "mistral:latest", "llama2"],
}

RESORT_SUMMARY_MODEL = "phi3:latest"

REDIS_HOST = "localhost"
REDIS_PORT = 6379
INDEX_NAME = "ski_index"
CHUNK_SIZE = 300
OVERLAP = 50

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://airflow:airflow@localhost:5432/airflow")
engine = create_engine(POSTGRES_URL)
Session = sessionmaker(bind=engine)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
embedding_model = SentenceTransformer(EMBEDDING_MODEL)


def make_json_safe(obj):
    from decimal import Decimal
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
    return json.dumps(safe) if isinstance(safe, (dict, list)) else str(safe)


def get_embedding(text):
    vec = embedding_model.encode(text)
    return np.array(vec).astype(np.float32)


def split_text(text):
    words = text.split()
    chunks = []
    for i in range(0, len(words), CHUNK_SIZE - OVERLAP):
        chunks.append(" ".join(words[i:i + CHUNK_SIZE]))
    return chunks


def create_index():
    try:
        r.ft(INDEX_NAME).info()
        return
    except:
        pass
    from redis.commands.search.field import TagField, TextField, VectorField
    from redis.commands.search.indexDefinition import IndexDefinition, IndexType
    schema = (
        TagField("resort"),
        TagField("day"),
        TextField("source"),
        TextField("chunk"),
        VectorField(
            "embedding",
            "HNSW",
            {"TYPE": "FLOAT32", "DIM": 384, "DISTANCE_METRIC": "COSINE"},
        ),
    )
    r.ft(INDEX_NAME).create_index(
        fields=schema,
        definition=IndexDefinition(prefix=["ski:"], index_type=IndexType.HASH),
    )


def ingest_corpus(corpus_folder="Ski Analyst Resources"):
    create_index()
    files = [f for f in os.listdir(corpus_folder) if f.endswith((".pdf", ".txt"))]
    for file in files:
        path = os.path.join(corpus_folder, file)
        try:
            if file.endswith(".txt"):
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
            else:
                import fitz
                doc = fitz.open(path)
                content = "\n".join([page.get_text() for page in doc])
        except Exception as e:
            print(f"Failed to read {file}: {e}")
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


def search_chunks(query, top_k=6):
    create_index()
    query_vec = get_embedding(query)
    results = r.ft(INDEX_NAME).search("*", query_params={"vec": query_vec.tobytes()})
    context = []
    if hasattr(results, "docs"):
        for doc in results.docs:
            context.append(
                {"source": getattr(doc, "source", ""), "chunk": getattr(doc, "chunk", ""), "score": getattr(doc, "score", 0)}
            )
    return sorted(context, key=lambda x: x["score"], reverse=True)[:top_k]


def generate_analysis(resort, weather_summary, model):
    context = search_chunks(weather_summary)
    context_str = "\n\n".join([f"[Source: {c['source']}]\n{c['chunk']}" for c in context])
    prompt = f"""
    
You are a ski conditions analyst for {resort}.

Weather and conditions summary:
{weather_summary}



Question:
{QUESTION}

Instructions:
1. YES or NO at the top.
2. 3-5 sentence reasoning based on the summary and trails.
3. Reference temps, snowfall, wind, and trail availability if present.
4. Do NOT repeat raw chunks from ingested documents.
"""
    response = ollama.chat(model=model, messages=[{"role": "user", "content": prompt}])
    return response["message"]["content"]


def summarize_resort(resort, analyst_outputs):
    combined = "\n\n".join([f"Analyst {i+1}:\n{out}" for i, out in enumerate(analyst_outputs)])
    prompt = f"""
Multiple analysts evaluated {resort}.

{combined}

Return:
1) Final YES or NO
2) Exactly 3 sentences explanation
3) Confidence
"""
    response = ollama.chat(model=RESORT_SUMMARY_MODEL, messages=[{"role": "user", "content": prompt}])
    return response["message"]["content"]


def fetch_resort_full_snapshot(resort, start_ts, end_ts):
    session = Session()
    data = {}

    # Get conditions snapshot
    table = f"feat_{resort.lower()}_conditions_snapshot"
    try:
        row = session.execute(text(f"SELECT * FROM {table} ORDER BY ingested_at DESC LIMIT 1")).fetchone()
        data["conditions_snapshot"] = dict(row._mapping) if row else {}
    except:
        data["conditions_snapshot"] = {}

    # Trails
    table = f"feat_{resort.lower()}_open_trails_by_difficulty"
    try:
        row = session.execute(text(f"SELECT * FROM {table} ORDER BY ingested_at DESC LIMIT 1")).fetchone()
        data["trails_by_difficulty"] = dict(row._mapping) if row else {}
    except:
        data["trails_by_difficulty"] = {}

    # Daily forecast
    try:
        row = session.execute(
            text("SELECT * FROM openmeteo_daily WHERE resort = :resort AND date_utc = :date LIMIT 1"),
            {"resort": resort, "date": start_ts.date()},
        ).fetchone()
        data["openmeteo_daily"] = dict(row._mapping) if row else {}
    except:
        data["openmeteo_daily"] = {}

    # Hourly forecast
    try:
        rows = session.execute(
            text(
                "SELECT * FROM openmeteo_hourly WHERE resort = :resort AND time_utc >= :start AND time_utc < :end ORDER BY time_utc ASC"
            ),
            {"resort": resort, "start": start_ts, "end": end_ts},
        ).fetchall()
        data["openmeteo_hourly"] = [dict(r._mapping) for r in rows] if rows else []
    except:
        data["openmeteo_hourly"] = []

    session.close()
    return data


def evaluate_resort_day(resort, day, day_data):
    weather_summary = f"Daily forecast: {day_data.get('openmeteo_daily', {})}\nHourly: {day_data.get('openmeteo_hourly', {})}"
    models = RESORT_MODELS.get(resort, ["mistral:latest", "llama2", "phi3:latest", "mistral:latest"])
    analyst_outputs = [generate_analysis(resort, weather_summary, m) for m in models]
    final_decision = summarize_resort(resort, analyst_outputs)
    return {"resort": resort, "day": day, "analyst_outputs": analyst_outputs, "final_resort_decision": final_decision}


def run_full_daily_analysis(day, all_resort_data):
    results = []
    for resort, data in all_resort_data.items():
        day_data = data.get(day, {})
        results.append(evaluate_resort_day(resort, day, day_data))
    best = pick_best_resort(results)
    return {"per_resort": results, "best_resort_decision": best}


def pick_best_resort(resort_results):
    combined = "\n\n".join([f"{r['resort']}:\n{r['final_resort_decision']}" for r in resort_results])
    prompt = f"""
Multiple resorts evaluated.

{combined}

Return:
1) Best resort
2) 3 sentence explanation
3) Confidence
"""
    response = ollama.chat(model=RESORT_SUMMARY_MODEL, messages=[{"role": "user", "content": prompt}])
    return response["message"]["content"]
