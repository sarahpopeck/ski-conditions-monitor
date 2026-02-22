import streamlit as st
from RAG_architecture import evaluate_day
from datetime import datetime, timedelta

st.set_page_config(layout="wide")

st.title("7-Day Ski Condition Evaluator")

st.write("Each tab displays forecast data and LLM recommendations for that day.")

# Generate next 7 dates starting today
today = datetime.today()
date_list = [
    (today + timedelta(days=i)).strftime("%B %d, %Y")
    for i in range(7)
]

# Replace with real API data mapped by date
seven_day_data = {date: {} for date in date_list}

tabs = st.tabs(date_list)

for i, date in enumerate(date_list):

    with tabs[i]:

        st.header(f"{date} Forecast Overview")

        day_data = seven_day_data[date]

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
