# Import libraries as necessary (LLM models, preprocessing, sentence transformers)
import os
import json
import numpy as np
import ollama
import json
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

CHUNK_SIZE = 300
OVERLAP = 50

# To obtain a rounded answer, we are using two LLMs for answers, and a third to summarize the two responses
LLM_MODELS = {
    "mistral": "mistral:latest",
    "llama2": "llama2",
}

SUMMARY_MODEL = "phi3:latest"
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

QUESTION = "Is this a good day to go skiing?"

# Load embedding model once
embedding_model = SentenceTransformer(EMBEDDING_MODEL)

# Splits long text into smaller overlapping chunks
def split_text_into_chunks(text, chunk_size=CHUNK_SIZE, overlap=OVERLAP):
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size - overlap):
        chunk = " ".join(words[i:i+chunk_size])
        chunks.append(chunk)
    return chunks

# Generates embedding
def get_embedding(text: str):
    return embedding_model.encode(text)

# Ingests mixed data sources (API data, text, etc.)
def ingest_day_data(day_data):

    vector_store = []

    for source_name, raw_data in day_data.items():

        # Convert dict or list to string format
        if isinstance(raw_data, (dict, list)):
            text = json.dumps(raw_data)
        else:
            text = str(raw_data)

        chunks = split_text_into_chunks(text)

        for chunk in chunks:
            embedding = get_embedding(chunk)

            vector_store.append({
                "source": source_name,
                "chunk": chunk,
                "embedding": embedding
            })

    return vector_store

# Searches embeddings using cosine similarity
def search_embeddings(query, vector_store, top_k=6):

    if not vector_store:
        return []

    query_embedding = get_embedding(query)
    similarities = []

    for item in vector_store:
        score = cosine_similarity(
            [query_embedding],
            [item["embedding"]]
        )[0][0]

        # Slightly weight expert and resort sources higher
        if item["source"] in ["opensnow_resort", "nws_discussion"]:
            score *= 1.10

        similarities.append({
            "source": item["source"],
            "chunk": item["chunk"],
            "similarity": score
        })

    similarities.sort(key=lambda x: x["similarity"], reverse=True)

    return similarities[:top_k]

# Function that prompts both LLM models for their ski prediction
def generate_rag_response(query, context_results, llm_model):

    context_str = "\n\n".join(
        [f"[Source: {c['source']}]\n{c['chunk']}" for c in context_results]
    )

    prompt = f"""
You are a ski conditions analyst.

You are evaluating today's ski quality using multiple data sources:

- Open-Meteo: numerical forecast model data
- National Weather Service Discussion: expert meteorologist interpretation
- Reddit Ski Reports: real-time skier sentiment
- OpenSnow Resort Data: ski-specific operational forecast

Use ONLY the provided data below.

Question:
{query}

Data:
{context_str}

Provide:
1) A clear YES or NO
2) 3–5 sentences explaining why.
"""

    response = ollama.chat(
        model=llm_model,
        messages=[{"role": "user", "content": prompt}]
    )

    return response["message"]["content"]

# Uses an LLM to summarize the two prior LLM's responses
def summarize_conclusions(mistral_answer, llama_answer):

    prompt = f"""
Two ski experts evaluated today's conditions.

Expert 1 (Mistral):
{mistral_answer}

Expert 2 (Llama2):
{llama_answer}

Provide:
- Final YES or NO
- Exactly 3 sentences summarizing the reasoning.
"""

    response = ollama.chat(
        model=SUMMARY_MODEL,
        messages=[{"role": "user", "content": prompt}]
    )

    return response["message"]["content"]

# Main evaluation function for one day
def evaluate_day(day_data):

    vector_store = ingest_day_data(day_data)

    context = search_embeddings(QUESTION, vector_store)

    mistral_answer = generate_rag_response(
        QUESTION, context, LLM_MODELS["mistral"]
    )

    llama_answer = generate_rag_response(
        QUESTION, context, LLM_MODELS["llama2"]
    )

    final_decision = summarize_conclusions(
        mistral_answer, llama_answer
    )

    return {
        "mistral": mistral_answer,
        "llama2": llama_answer,
        "final": final_decision,
        "context": context
    }
