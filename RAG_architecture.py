import os
import json
import uuid
import numpy as np
import redis
import ollama
from datetime import datetime
from sentence_transformers import SentenceTransformer

# All of our necessary configurations

QUESTION = "Is this a good day to go skiing?"
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

# Two analysts per resort
RESORT_MODELS = {
    "Killington": ["mistral:latest", "llama2"],
    "Pico": ["llama2", "mistral:latest"],
    "Sugarloaf": ["mistral:latest", "phi3:latest"],
    "SundayRiver": ["llama2", "phi3:latest"],
    "Loon": ["mistral:latest", "llama2"],
    "Sugarbush": ["phi3:latest", "mistral:latest"],
    "Stratton": ["llama2", "phi3:latest"],
}

# Resort-level summarizer
RESORT_SUMMARY_MODEL = "phi3:latest"

# Global best-resort summarizer
GLOBAL_SUMMARY_MODEL = "phi3:latest"

REDIS_HOST = "localhost"
REDIS_PORT = 6379
INDEX_NAME = "ski_index"

CHUNK_SIZE = 300
OVERLAP = 50

# Initializing our Redis connection

embedding_model = SentenceTransformer(EMBEDDING_MODEL)

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=False
)

# Create our redis vector index

def create_index():
    try:
        r.ft(INDEX_NAME).info()
        return
    except:
        pass

    from redis.commands.search.field import (
        TextField,
        VectorField,
        TagField
    )
    from redis.commands.search.indexDefinition import (
        IndexDefinition,
        IndexType
    )

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
            }
        ),
    )

    r.ft(INDEX_NAME).create_index(
        fields=schema,
        definition=IndexDefinition(
            prefix=["ski:"],
            index_type=IndexType.HASH
        )
    )

# Chunking text and creating embeddings

def split_text(text):
    words = text.split()
    chunks = []
    for i in range(0, len(words), CHUNK_SIZE - OVERLAP):
        chunks.append(" ".join(words[i:i + CHUNK_SIZE]))
    return chunks

def get_embedding(text):
    vec = embedding_model.encode(text)
    return np.array(vec).astype(np.float32)

# ingest data into redis

def ingest_day_data(resort, day, day_data):

    for source_name, raw_data in day_data.items():

        if isinstance(raw_data, (dict, list)):
            text = json.dumps(raw_data)
        else:
            text = str(raw_data)

        chunks = split_text(text)

        for chunk in chunks:
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

# Vector search for embeddings

def search_embeddings(resort, day, query, top_k=6):

    query_embedding = get_embedding(query)

    q = (
        f"(@resort:{{{resort}}} @day:{{{day}}})=>"
        f"[KNN {top_k} @embedding $vec AS score]"
    )

    results = r.ft(INDEX_NAME).search(
        q,
        query_params={"vec": query_embedding.tobytes()},
        sort_by="score",
        return_fields=["source", "chunk", "score"],
        dialect=2
    )

    context = []

    for doc in results.docs:
        context.append({
            "source": doc.source.decode(),
            "chunk": doc.chunk.decode(),
            "score": float(doc.score)
        })

    return context

# Generate LLM response

def generate_analysis(resort, context, model):

    context_str = "\n\n".join(
        [f"[Source: {c['source']}]\n{c['chunk']}" for c in context]
    )

    prompt = f"""
You are a ski conditions analyst for {resort}.

Use ONLY the data you know.

Question:
{QUESTION}

Provide:
1) YES or NO
2) 3–5 sentences explaining why.
"""

    response = ollama.chat(
        model=model,
        messages=[{"role": "user", "content": prompt}]
    )

    return response["message"]["content"]

# Summarize the reports from the two LLMs as to their responses

def summarize_resort(resort, analyst_outputs):

    combined = "\n\n".join(
        [f"Analyst {i+1}:\n{out}" for i, out in enumerate(analyst_outputs)]
    )

    prompt = f"""
Two ski analysts evaluated {resort} today.

{combined}

Provide:
1) Final YES or NO
2) Exactly 3 sentences summarizing reasoning.
"""

    response = ollama.chat(
        model=RESORT_SUMMARY_MODEL,
        messages=[{"role": "user", "content": prompt}]
    )

    return response["message"]["content"]

# Evaluate a resort day's skiing conditions

def evaluate_resort_day(resort, day, day_data):

    ingest_day_data(resort, day, day_data)

    context = search_embeddings(resort, day, QUESTION)

    models = RESORT_MODELS.get(resort, ["mistral:latest", "llama2"])

    analyst_outputs = []

    for model in models:
        output = generate_analysis(resort, context, model)
        analyst_outputs.append(output)

    final_resort_decision = summarize_resort(resort, analyst_outputs)

    return {
        "resort": resort,
        "day": day,
        "analyst_outputs": analyst_outputs,
        "final_resort_decision": final_resort_decision
    }

# Summarizer to determine potential best resort to go to

def pick_best_resort(resort_results):

    combined = "\n\n".join(
        [f"{r['resort']}:\n{r['final_resort_decision']}"
         for r in resort_results]
    )

    prompt = f"""
Several ski resorts were evaluated.

{combined}

Choose:
1) The single BEST resort today
2) Exactly 3 sentences explaining why
3) Confidence level (Low/Medium/High)
"""

    response = ollama.chat(
        model=GLOBAL_SUMMARY_MODEL,
        messages=[{"role": "user", "content": prompt}]
    )

    return response["message"]["content"]

# Function to evaluate each resort for the next 7 days

def evaluate_all_resorts(day, all_resort_data):

    create_index()

    results = []

    for resort, data in all_resort_data.items():
        result = evaluate_resort_day(resort, day, data)
        results.append(result)

    best = pick_best_resort(results)

    return {
        "per_resort": results,
        "best_resort_decision": best
    }
