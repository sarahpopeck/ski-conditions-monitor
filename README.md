# ski-conditions-monitor

Predicting and Exploring Optimal Ski Conditions Using Social Media Sentiment, Weather Data, and Official Resort Information

### 7-Day Ski Condition Evaluator

A Retrieval-Augmented Generation (RAG) system that analyzes short-range weather forecasts and produces ski condition recommendations using multiple large language models.

## Overview

The 7-Day Ski Condition Evaluator is a Streamlit-based application designed to assess ski suitability using structured forecast inputs and LLM-based reasoning.

The system:

- Retrieves daily forecast data

- Structures meteorological variables into a standardized format

- Evaluates ski conditions using two independent LLMs

- Produces a consolidated recommendation

- This project demonstrates how retrieval-augmented language models can be applied to environmental decision support under uncertainty.

## Project Purpose

The primary objectives of this project are to:

Explore the application of LLMs in structured decision support systems

Compare model behavior under identical weather inputs

Demonstrate retrieval-augmented prompting for domain-constrained reasoning

Provide interpretable ski recommendations instead of raw meteorological data

The system is designed as a research-oriented prototype for AI-assisted forecasting and decision analysis.

## Dataset
- Open-Meteo Weather Forecasts: Provides high-resolution, multi-model weather forecasts essential for evaluating ski conditions and forecast uncertainty.
- Reddit Ski Forums: Captures real-time, on-the-ground skier sentiment and condition reports not reflected in numerical forecasts.
- OpenSnow Resort Data: Offers resort-level snowfall forecasts and condition summaries that mirror how skiers plan real trips.
- Various open-source documents about optimal ski conditions

## Models Implemented
- Initial recommendation and ingestion system: phi4-mini and qwen3:4b

Instruction-tuned large language models used as an independent evaluator for ski conditions.

- Final Aggregation Layer

The system synthesizes outputs from both models to produce a final recommendation. This layer reduces individual model bias and provides a consolidated decision.

## Key Results

The system produces consistent, interpretable daily condition ratings across multiple
resorts, demonstrating strong alignment with reported conditions and enabling comparative
analysis. By integrating heterogeneous data sources into a unified prediction framework,
this tool reduces information search costs and improves trip planning for skiers. The approach
highlights the potential of LLM-based systems in domain-specific decision support applications.

## Repository Structure
```
ski-conditions-monitor/

|-- capstoneAirflow/

|   |-- dags/                 # DAGs

|   |-- reddit_date/          # Original, immutable Reddit data and scripts

|   |-- docker-compose.yaml   # Docker YAML

|   |-- raw/                  # Original, immutable JSON data for resorts

|   |-- tests/               # Test scripts

|-- Reddit Data/

|   |-- reddit_apify.py      # Reddit data loader

|-- Ski Analyst Resources/   # RAG ingestion material; optimal ski conditions

|-- SQL Scripts/             # Written SQL scripts for queries

|-- .gitignore

|-- Golden Dataset.xlsx     # Our Golden Truth dataset

|-- Iteration 4: Report Draft.pdf # Iteration 4 Assignment

|-- RAG_architecture.py     # RAG pipeline

|-- README.md

|-- app.py                  # Streamlit front-end interface

|-- requirements.txt
```

## Installation and Requirements

### 1. Clone the Repository
```bash
git clone https://github.com/sarahpopeck/ski-conditions-monitor.git
cd ski-conditions-monitor
```

### 2. Set Up Python Environment
```bash
python3 -m venv .venv
source .venv/bin/activate   # Mac/Linux
# .venv\Scripts\activate    # Windows
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Install and Run Ollama (for LLM inference)
```bash
curl -fsSL https://ollama.com/install.sh | sh
ollama serve
ollama pull phi4-mini
ollama pull qwen3:4b
```

### 5. Configure Environment Variables

Create a `.env` file in the project root:

```bash
APIFY_TOKEN=your_apify_token_here
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_DEFAULT_REGION=us-east-1
```

### 6. Start Backend Services (Airflow, Postgres, Redis)
```bash
cd capstoneAirflow
docker compose up --build
```

This will launch:
- Apache Airflow (data pipelines)
- PostgreSQL (data storage)
- Redis (vector index for RAG)

### 7. Run the Frontend Application
```bash
streamlit run app.py
```

Open in browser:
```
http://localhost:8501
```

### 8. (Optional) AWS Deployment
- Launch EC2 instance
- Install Docker and Docker Compose
- Clone repository
- Run:

```bash
docker compose up -d
```

Ensure IAM permissions are configured for S3 access.

### Notes
- Docker must be installed before running Airflow services
- Ollama must be running locally for LLM-based recommendations
- APIFY token is required for Reddit data ingestion

## Running the Application

Start the Streamlit app with:

[ streamlit run app.py ]

The application will launch in your default browser.

## Dependencies

Install all dependencies using:

pip install -r requirements.txt

## Future Work

1. Expand the dataset to include additional ski resorts and multi-season historical weather data across the East Coast. This would improve generalization and reduce sensitivity to location-specific patterns currently learned from a limited set of resorts.

2. Deploy the system as a production-ready API with real-time weather ingestion and monitoring. This would enable continuous updating of predictions and facilitate integration into user-facing applications, such as trip planning tools or mobile ski advisory systems.

3. Incorporate multi-modal inputs such as live camera feeds, snowpack sensor data, or satellite imagery. This would enable the system to move beyond text and tabular weather data, capturing real-world ski conditions in real-time.

## License

This project is intended for research and educational purposes.
