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
- National Weather Service Forecast Discussions: Supplies expert-written qualitative insights on forecast confidence, uncertainty, and potential hazards.
- Reddit Ski Forums: Captures real-time, on-the-ground skier sentiment and condition reports not reflected in numerical forecasts.
- OpenSnow Resort Data: Offers resort-level snowfall forecasts and condition summaries that mirror how skiers plan real trips.

## Models Implemented
- Mistral

An instruction-tuned large language model used to evaluate ski suitability based on structured forecast input.

- Llama2

An instruction-tuned large language model used as an independent evaluator for ski conditions.

- Final Aggregation Layer

The system synthesizes outputs from both models to produce a final recommendation. This layer reduces individual model bias and provides a consolidated decision.

## Key Results

TBD

## Repository Structure
TBD

## Installation and Requirements
1. Clone the Repository
git clone https://github.com/your-username/ski-conditions-monitor.git
cd ski-conditions-monitor
2. Install Dependencies
pip install -r requirements.txt

## Running the Application

Start the Streamlit app with:

[ streamlit run app.py ]

The application will launch in your default browser.

## Dependencies

Install all dependencies using:

pip install -r requirements.txt

## Future Work

TBD

## License

This project is intended for research and educational purposes.
