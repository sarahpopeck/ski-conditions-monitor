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

# Question for AI models
QUESTION = "Is this a good day to go skiing?"

# Embedding model for semantic search
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

# Resorts and associated AI analyst models
RESORT_MODELS = {
    "Killington": ["mistral:latest", "llama2", "phi3:latest"],
    "Pico": ["llama2", "mistral:latest", "phi3:latest"],
    "Sugarloaf": ["mistral:latest", "phi3:latest", "llama2"],
    "SundayRiver": ["llama2", "phi3:latest", "mistral:latest"],
    "Loon": ["mistral:latest", "llama2", "phi3:latest"],
    "Sugarbush": ["phi3:latest", "mistral:latest", "llama2"],
    "Stratton": ["llama2", "phi3:latest", "mistral:latest"],
}

# Summarizer model
RESORT_SUMMARY_MODEL = "phi3:latest"

# Redis connection info
REDIS_HOST = "localhost"
REDIS_PORT = 6379
INDEX_NAME = "ski_index"

# Embedding chunk size
CHUNK_SIZE = 300
OVERLAP = 50

# Postgres connection (for raw resort data)
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://airflow:airflow@localhost:5432/airflow")
engine = create_engine(POSTGRES_URL)
Session = sessionmaker(bind=engine)

# Initialize Redis connection and embedding model
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
embedding_model = SentenceTransformer(EMBEDDING_MODEL)

# Ensure objects are JSON-safe
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

# Convert text to embeddings
def get_embedding(text):
    vec = embedding_model.encode(text)
    return np.array(vec).astype(np.float32)

# Split large texts into manageable chunks
def split_text(text):
    words = text.split()
    chunks = []
    for i in range(0, len(words), CHUNK_SIZE - OVERLAP):
        chunks.append(" ".join(words[i:i + CHUNK_SIZE]))
    return chunks

# Create Redis index for embeddings
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
            {"TYPE": "FLOAT32", "DIM": 384, "DISTANCE_METRIC": "COSINE"}
        ),
    )

    r.ft(INDEX_NAME).create_index(
        fields=schema,
        definition=IndexDefinition(prefix=["ski:"], index_type=IndexType.HASH)
    )

# Ingest PDFs/TXT corpus for ski advice into Redis embeddings
def ingest_corpus(corpus_folder="Ski resort conditions"):
    create_index()
    files = [f for f in os.listdir(corpus_folder) if f.endswith((".pdf", ".txt"))]
    for file in files:
        path = os.path.join(corpus_folder, file)
        try:
            if file.endswith(".txt"):
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read()
            else:
                import fitz  # PyMuPDF
                doc = fitz.open(path)
                content = "\n".join([page.get_text() for page in doc])
        except Exception as e:
            print(f"Failed to read {file}: {e}")
            continue
        for chunk in split_text(content):
            emb = get_embedding(chunk)
            key = f"ski:corpus:{uuid.uuid4()}"
            r.hset(key, mapping={"resort": "all", "day": "all", "source": file, "chunk": chunk, "embedding": emb.tobytes()})

# Search corpus embeddings
def search_chunks(query, top_k=6):
    create_index()
    query_vec = get_embedding(query)
    results = r.ft(INDEX_NAME).search(
        "*",  # match all
        query_params={"vec": query_vec.tobytes()}
    )
    context = []
    if hasattr(results, "docs"):
        for doc in results.docs:
            context.append({
                "source": getattr(doc, "source", ""),
                "chunk": getattr(doc, "chunk", ""),
                "score": getattr(doc, "score", 0)
            })
    return sorted(context, key=lambda x: x["score"], reverse=True)[:top_k]

# Generate AI analysis from a model
def generate_analysis(resort, weather_summary, model):
    context = search_chunks(weather_summary)
    context_str = "\n\n".join([f"[Source: {c['source']}]\n{c['chunk']}" for c in context])
    prompt = f"""
You are a ski conditions analyst for {resort}.

Weather and conditions summary:
{weather_summary}

Reference guidance:
{context_str}

Question:
{QUESTION}

Return:
1) YES or NO
2) 3-5 sentence explanation
"""
    response = ollama.chat(model=model, messages=[{"role": "user", "content": prompt}])
    return response["message"]["content"]

# Summarize outputs from all analysts
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

# Fetch raw data from Postgres for a resort/day
def fetch_resort_full_snapshot(resort, start_ts, end_ts):
    session = Session()
    data = {}
    tables = {
        "conditions_snapshot": f"feat_{resort.lower()}_conditions_snapshot",
        "trails_by_difficulty": f"feat_{resort.lower()}_open_trails_by_difficulty"
    }
    for key, table in tables.items():
        try:
            row = session.execute(text(f"SELECT * FROM {table} ORDER BY ingested_at DESC LIMIT 1")).fetchone()
            data[key] = dict(row._mapping) if row else {}
        except Exception:
            data[key] = {}
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
        data["openmeteo_hourly"] = [dict(r._mapping) for r in rows] if rows else []
    except Exception:
        data["openmeteo_hourly"] = []
    session.close()
    return data

# Evaluate a single resort/day
def evaluate_resort_day(resort, day, day_data):
    weather_summary = f"Daily forecast: {day_data.get('openmeteo_daily', {})}\nHourly: {day_data.get('openmeteo_hourly', {})}"
    models = RESORT_MODELS.get(resort, ["mistral:latest", "llama2", "phi3:latest"])
    analyst_outputs = [generate_analysis(resort, weather_summary, m) for m in models]
    final_decision = summarize_resort(resort, analyst_outputs)
    return {"resort": resort, "day": day, "analyst_outputs": analyst_outputs, "final_resort_decision": final_decision}

# Evaluate multiple resorts on a day
def run_full_daily_analysis(day, all_resort_data):
    results = []
    for resort, data in all_resort_data.items():
        day_data = data.get(day, {})
        results.append(evaluate_resort_day(resort, day, day_data))
    best = pick_best_resort(results)
    return {"per_resort": results, "best_resort_decision": best}

# Pick best resort across multiple resorts
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
