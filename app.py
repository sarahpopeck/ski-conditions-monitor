import streamlit as st
from rag_engine import evaluate_day

st.set_page_config(layout="wide")

st.title("7-Day Ski Condition Evaluator")

st.write("Each tab displays forecast data and LLM recommendations for that day.")

# Replace this with real API data
seven_day_data = {
    "Day 1": {},
    "Day 2": {},
    "Day 3": {},
    "Day 4": {},
    "Day 5": {},
    "Day 6": {},
    "Day 7": {},
}

tabs = st.tabs(list(seven_day_data.keys()))

for i, day in enumerate(seven_day_data.keys()):

    with tabs[i]:

        st.header(f"{day} Forecast Overview")

        day_data = seven_day_data[day]

        if not day_data:
            st.info("No data available for this day.")
            continue

        st.subheader("Raw Forecast Data")
        st.json(day_data)

        with st.spinner("Analyzing ski conditions..."):
            results = evaluate_day(day_data)

        st.subheader("Mistral Recommendation")
        st.write(results["mistral"])

        st.subheader("Llama2 Recommendation")
        st.write(results["llama2"])

        st.subheader("Final LLM Decision")
        st.success(results["final"])
