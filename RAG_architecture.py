import os
import json
import uuid
import numpy as np
import redis
import ollama
from datetime import datetime, date
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sentence_transformers import SentenceTransformer

# Configuration
QUESTION = "Is this a good day to go skiing?"
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

RESORT_MODELS = {
    "Killington": ["mistral:latest", "llama2"],
    "Pico": ["llama2", "mistral:latest"],
    "Sugarloaf": ["mistral:latest", "phi3:latest"],
    "SundayRiver": ["llama2", "phi3:latest"],
    "Loon": ["mistral:latest", "llama2"],
    "Sugarbush": ["phi3:latest", "mistral:latest"],
    "Stratton": ["llama2", "phi3:latest"],
}

RESORT_SUMMARY_MODEL = "phi3:latest"

REDIS_HOST = "localhost"
REDIS_PORT = 6379
INDEX_NAME = "ski_index"

CHUNK_SIZE = 300
OVERLAP = 50

# postgres connection

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://airflow:airflow@localhost:5432/airflow"
)

engine = create_engine(POSTGRES_URL)
Session = sessionmaker(bind=engine)

# Redis connection and embedding model

embedding_model = SentenceTransformer(EMBEDDING_MODEL)

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=False
)

# Transforms the JSON so that it is safe to ingest

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


# Create embeddings

def get_embedding(text):
    vec = embedding_model.encode(text)
    return np.array(vec).astype(np.float32)


def split_text(text):
    words = text.split()
    chunks = []
    for i in range(0, len(words), CHUNK_SIZE - OVERLAP):
        chunks.append(" ".join(words[i:i + CHUNK_SIZE]))
    return chunks


# Create indexes

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
            {
                "TYPE": "FLOAT32",
                "DIM": 384,
                "DISTANCE_METRIC": "COSINE"
            }
        ),
    )

    r.ft(INDEX_NAME).create_index(
        fields=schema,
        definition=IndexDefinition(prefix=["ski:"], index_type=IndexType.HASH)
    )


# Ingest daily data

def ingest_day_data(resort, day, day_data):

    allowed_keys = [
        "conditions_snapshot",
        "trails_by_difficulty",
        "openmeteo_daily",
        "openmeteo_hourly",
    ]

    filtered = {k: day_data[k] for k in allowed_keys if k in day_data}

    for source_name, raw_data in filtered.items():

        text = safe_json_dump(raw_data)

        if not text:
            continue

        for chunk in split_text(text):

            embedding = get_embedding(chunk)

            key = f"ski:{resort}:{day}:{uuid.uuid4()}"

            r.hset(
                key,
                mapping={
                    "resort": resort,
                    "day": day,
                    "source": source_name,
                    "chunk": chunk,
                    "embedding": embedding.tobytes()
                }
            )


# Search embeddings

def search_embeddings(resort, day, query, top_k=6):

    create_index()

    query_vec = get_embedding(query)

    results = r.ft(INDEX_NAME).search(
        f"@resort:{{{resort}}} @day:{{{day}}}",
        query_params={"vec": query_vec.tobytes()}
    )

    if not results or not hasattr(results, "docs"):
        return []

    context = []

    for doc in results.docs:
        context.append({
            "source": getattr(doc, "source", ""),
            "chunk": getattr(doc, "chunk", ""),
            "score": getattr(doc, "score", 0)
        })

    return sorted(context, key=lambda x: x["score"], reverse=True)[:top_k]


# LLM analysis - we use 2 llms to each make decisions, and a third to summarize

def generate_analysis(resort, context, model):

    context_str = "\n\n".join(
        [f"[Source: {c['source']}]\n{c['chunk']}" for c in context]
    )

    prompt = f"""
You are a ski conditions analyst for {resort}.

Context:
{context_str}

Question:
{QUESTION}

Return:
1) YES or NO
2) 3-5 sentence explanation
"""

    response = ollama.chat(
        model=model,
        messages=[{"role": "user", "content": prompt}]
    )

    return response["message"]["content"]


def summarize_resort(resort, analyst_outputs):

    combined = "\n\n".join(
        [f"Analyst {i+1}:\n{out}" for i, out in enumerate(analyst_outputs)]
    )

    prompt = f"""
Two analysts evaluated {resort}.

{combined}

Return:
1) Final YES or NO
2) Exactly 3 sentences explanation
3) Confidence
"""

    response = ollama.chat(
        model=RESORT_SUMMARY_MODEL,
        messages=[{"role": "user", "content": prompt}]
    )

    return response["message"]["content"]


# Main function to evaluate each resort for each day

def evaluate_resort_day(resort, day, day_data):

    ingest_day_data(resort, day, day_data)

    context = search_embeddings(resort, day, QUESTION)

    models = RESORT_MODELS.get(resort, ["mistral:latest"])

    analyst_outputs = []

    for model in models:
        analyst_outputs.append(
            generate_analysis(resort, context, model)
        )

    final_decision = summarize_resort(resort, analyst_outputs)

    return {
        "resort": resort,
        "day": day,
        "analyst_outputs": analyst_outputs,
        "final_resort_decision": final_decision
    }


# Global analysis of picking best resort of the chosen ones

def pick_best_resort(resort_results):

    combined = "\n\n".join(
        [f"{r['resort']}:\n{r['final_resort_decision']}"
         for r in resort_results]
    )

    prompt = f"""
Multiple resorts were evaluated.

{combined}

Return:
1) Best resort
2) 3 sentence explanation
3) Confidence
"""

    response = ollama.chat(
        model=RESORT_SUMMARY_MODEL,
        messages=[{"role": "user", "content": prompt}]
    )

    return response["message"]["content"]


def run_full_daily_analysis(day, all_resort_data):

    results = []

    for resort, data in all_resort_data.items():
        results.append(evaluate_resort_day(resort, day, data))

    best = pick_best_resort(results)

    return {
        "per_resort": results,
        "best_resort_decision": best
    }


#  Grab the necessary data

def fetch_resort_full_snapshot(resort, start_ts, end_ts):

    session = Session()
    rk = resort.lower()
    data = {}

    try:
        table = f"feat_{rk}_conditions_snapshot"

        row = session.execute(
            text(f"""
            SELECT *
            FROM {table}
            ORDER BY ingested_at DESC
            LIMIT 1
            """)
        ).fetchone()

        data["conditions_snapshot"] = dict(row._mapping) if row else {}

    except Exception:
        data["conditions_snapshot"] = {}

    try:
        table = f"feat_{rk}_open_trails_by_difficulty"

        row = session.execute(
            text(f"""
            SELECT *
            FROM {table}
            ORDER BY ingested_at DESC
            LIMIT 1
            """)
        ).fetchone()

        data["trails_by_difficulty"] = dict(row._mapping) if row else {}

    except Exception:
        data["trails_by_difficulty"] = {}

    try:
        row = session.execute(
            text("""
            SELECT *
            FROM openmeteo_daily
            WHERE resort = :resort
            AND date_utc = :date
            LIMIT 1
            """),
            {"resort": resort, "date": start_ts.date()}
        ).fetchone()

        data["openmeteo_daily"] = dict(row._mapping) if row else {}

    except Exception:
        data["openmeteo_daily"] = {}

    try:
        rows = session.execute(
            text("""
            SELECT *
            FROM openmeteo_hourly
            WHERE resort = :resort
            AND time_utc >= :start
            AND time_utc < :end
            ORDER BY time_utc ASC
            """),
            {"resort": resort, "start": start_ts, "end": end_ts}
        ).fetchall()

        data["openmeteo_hourly"] = [
            dict(r._mapping) for r in rows
        ]

    except Exception:
        data["openmeteo_hourly"] = []

    session.close()

    return data


__all__ = [
    "RESORT_MODELS",
    "evaluate_resort_day",
    "run_full_daily_analysis",
    "pick_best_resort",
    "fetch_resort_full_snapshot",
]
