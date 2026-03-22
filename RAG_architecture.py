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
import glob
from pathlib import Path

# The question our LLMs will answer
QUESTION = "Is this a good day to go skiing?"

# Embedding model for turning text chunks into vectors
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

# Each resort has three analyst LLMs; all use same training corpus
RESORT_MODELS = {
    "Killington": ["mistral:latest", "llama2", "phi3:latest"],
    "Pico": ["llama2", "mistral:latest", "phi3:latest"],
    "Sugarloaf": ["mistral:latest", "phi3:latest", "llama2"],
    "SundayRiver": ["llama2", "phi3:latest", "mistral:latest"],
    "Loon": ["mistral:latest", "llama2", "phi3:latest"],
    "Sugarbush": ["phi3:latest", "mistral:latest", "llama2"],
    "Stratton": ["llama2", "phi3:latest", "mistral:latest"],
}

# Model used to summarize analyst outputs into a final recommendation
RESORT_SUMMARY_MODEL = "phi3:latest"

# Redis connection info
REDIS_HOST = "localhost"
REDIS_PORT = 6379
INDEX_NAME = "ski_index"

CHUNK_SIZE = 300
OVERLAP = 50

# Postgres database
POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://airflow:airflow@localhost:5432/airflow"
)

engine = create_engine(POSTGRES_URL)
Session = sessionmaker(bind=engine)

# Connect to Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)

# Load embedding model
embedding_model = SentenceTransformer(EMBEDDING_MODEL)


def make_json_safe(obj):
    # Ensures all data is JSON-serializable before storing
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
    # Safely dump data into JSON string
    safe = make_json_safe(data)
    return json.dumps(safe) if isinstance(safe, (dict, list)) else str(safe)


def get_embedding(text):
    # Convert a chunk of text into a vector embedding
    vec = embedding_model.encode(text)
    return np.array(vec).astype(np.float32)


def split_text(text):
    # Break long text into overlapping chunks for embeddings
    words = text.split()
    chunks = []
    for i in range(0, len(words), CHUNK_SIZE - OVERLAP):
        chunks.append(" ".join(words[i:i + CHUNK_SIZE]))
    return chunks


def create_index():
    # Creates Redis vector index if not already present
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
            {"TYPE": "FLOAT32", "DIM": 384, "DISTANCE_METRIC": "COSINE"}
        ),
    )

    r.ft(INDEX_NAME).create_index(
        fields=schema,
        definition=IndexDefinition(prefix=["ski:"], index_type=IndexType.HASH)
    )


def ingest_corpus(folder_path="Ski resort conditions"):
    # Read all PDFs and TXTs, split into chunks, embed, and store in Redis
    create_index()
    files = glob.glob(f"{folder_path}/*")
    for file in files:
        text = ""
        if file.lower().endswith(".txt"):
            with open(file, "r", encoding="utf-8") as f:
                text = f.read()
        elif file.lower().endswith(".pdf"):
            import fitz
            doc = fitz.open(file)
            text = "\n".join([page.get_text() for page in doc])
        else:
            continue
        for chunk in split_text(text):
            embedding = get_embedding(chunk)
            key = f"ski:corpus:{uuid.uuid4()}"
            r.hset(
                key,
                mapping={
                    "resort": "general",
                    "day": "corpus",
                    "source": Path(file).name,
                    "chunk": chunk,
                    "embedding": embedding.tobytes()
                }
            )


def search_chunks(query, top_k=6):
    # Retrieve the most relevant chunks from Redis based on query embedding
    create_index()
    query_vec = get_embedding(query)
    q = f"*=>[KNN {top_k} @embedding $vec AS score]"
    results = r.ft(INDEX_NAME).search(
        q,
        query_params={"vec": query_vec.tobytes()}
    )

    context = []
    for doc in getattr(results, "docs", []):
        context.append({
            "source": getattr(doc, "source", ""),
            "chunk": getattr(doc, "chunk", ""),
            "score": getattr(doc, "score", 0)
        })

    return sorted(context, key=lambda x: x["score"], reverse=True)[:top_k]


def generate_analysis(resort, weather_summary, model):
    # Ask a single analyst LLM to give a recommendation based on context
    context = search_chunks(weather_summary)
    context_str = "\n\n".join([f"[Source: {c['source']}]\n{c['chunk']}" for c in context])
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
    response = ollama.chat(model=model, messages=[{"role": "user", "content": prompt}])
    return response["message"]["content"]


def summarize_resort(resort, analyst_outputs):
    # Summarizes all analyst outputs into one final recommendation
    combined = "\n\n".join([f"Analyst {i+1}:\n{out}" for i, out in enumerate(analyst_outputs)])
    prompt = f"""
Two analysts evaluated {resort}.

{combined}

Return:
1) Final YES or NO
2) Exactly 3 sentences explanation
3) Confidence
"""
    response = ollama.chat(model=RESORT_SUMMARY_MODEL, messages=[{"role": "user", "content": prompt}])
    return response["message"]["content"]


def fetch_resort_full_snapshot(resort, start_ts, end_ts):
    # Grab the latest conditions and forecast data from Postgres
    session = Session()
    rk = resort.lower()
    data = {}
    try:
        table = f"feat_{rk}_conditions_snapshot"
        row = session.execute(text(f"SELECT * FROM {table} ORDER BY ingested_at DESC LIMIT 1")).fetchone()
        data["conditions_snapshot"] = dict(row._mapping) if row else {}
    except Exception:
        data["conditions_snapshot"] = {}
    try:
        table = f"feat_{rk}_open_trails_by_difficulty"
        row = session.execute(text(f"SELECT * FROM {table} ORDER BY ingested_at DESC LIMIT 1")).fetchone()
        data["trails_by_difficulty"] = dict(row._mapping) if row else {}
    except Exception:
        data["trails_by_difficulty"] = {}
    try:
        row = session.execute(
            text("SELECT * FROM openmeteo_daily WHERE resort = :resort AND date_utc = :date LIMIT 1"),
            {"resort": resort, "date": start_ts.date()}
        ).fetchone()
        data["openmeteo_daily"] = dict(row._mapping) if row else {}
    except Exception:
        data["openmeteo_daily"] = {}
    try:
        rows = session.execute(
            text("SELECT * FROM openmeteo_hourly WHERE resort = :resort AND time_utc >= :start AND time_utc < :end ORDER BY time_utc ASC"),
            {"resort": resort, "start": start_ts, "end": end_ts}
        ).fetchall()
        data["openmeteo_hourly"] = [dict(r._mapping) for r in rows]
    except Exception:
        data["openmeteo_hourly"] = []
    session.close()
    return data


def evaluate_resort_day(resort, day_data):
    # Run all three analysts and then summarize for a given day
    weather_summary = safe_json_dump(day_data)
    models = RESORT_MODELS.get(resort, ["mistral:latest", "llama2", "phi3:latest"])
    analyst_outputs = [generate_analysis(resort, weather_summary, model) for model in models]
    final_resort_decision = summarize_resort(resort, analyst_outputs)
    return {
        "resort": resort,
        "analyst_outputs": analyst_outputs,
        "final_resort_decision": final_resort_decision
    }


def run_full_daily_analysis(day, all_resort_data):
    # Run the analysis for each resort and then pick the best overall
    results = []
    for resort, data in all_resort_data.items():
        results.append(evaluate_resort_day(resort, data[day]))
    best = pick_best_resort(results)
    return {"per_resort": results, "best_resort_decision": best}


def pick_best_resort(resort_results):
    # Compare all resorts and choose the one with the best recommendation
    combined = "\n\n".join([f"{r['resort']}:\n{r['final_resort_decision']}" for r in resort_results])
    prompt = f"""
Multiple resorts were evaluated.

{combined}

Return:
1) Best resort
2) 3 sentence explanation
3) Confidence
"""
    response = ollama.chat(model=RESORT_SUMMARY_MODEL, messages=[{"role": "user", "content": prompt}])
    return response["message"]["content"]
