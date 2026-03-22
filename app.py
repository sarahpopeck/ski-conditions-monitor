import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px

from RAG_architecture import (
    run_full_daily_analysis,
    evaluate_resort_day,
    RESORT_MODELS,
    fetch_resort_full_snapshot,
    ingest_corpus
)

st.set_page_config(layout="wide")
st.title("Ski Intelligence Dashboard")

# Resort selection
available_resorts = list(RESORT_MODELS.keys())
selected_resorts = st.multiselect(
    "Select Resorts to Evaluate",
    available_resorts,
    default=available_resorts[5:]
)
if not selected_resorts:
    st.stop()

#today = datetime.today()

#date_list = [
#    (today + timedelta(days=i)).strftime("%Y-%m-%d")
#    for i in range(7)
# ]

# Fixed date range 2/19–2/26
date_list = [
    "2026-02-19", "2026-02-20", "2026-02-21", "2026-02-22",
    "2026-02-23", "2026-02-24", "2026-02-25", "2026-02-26"
]

@st.cache_data(show_spinner=False)
def load_resort_data(resort):
    resort_data = {}
    for day in date_list:
        start_ts = datetime.strptime(day, "%Y-%m-%d")
        end_ts = start_ts + timedelta(days=1)
        resort_data[day] = fetch_resort_full_snapshot(resort, start_ts, end_ts)
    return resort_data

all_resort_data = {resort: load_resort_data(resort) for resort in selected_resorts}

st.sidebar.header("Actions")

st.sidebar.markdown(
    "Load PDFs and TXT advice on evaluating ski conditions into the AI. "
    "This allows the analyst models to reference expert guidance for evaluating ski days."
)
if st.sidebar.button("Ingest AI Ski Conditions Corpus"):
    ingest_corpus()
    st.sidebar.success("Corpus ingested successfully!")

st.sidebar.markdown(
    "Evaluate all selected resorts for the first day and determine the best overall resort "
    "based on weather, trails, and AI recommendations."
)
if st.sidebar.button("Run Global Comparison"):
    first_day = date_list[0]
    results = run_full_daily_analysis(day=first_day, all_resort_data=all_resort_data)
    st.sidebar.success("Global analysis complete")
    st.markdown("## Best Overall Resort")
    st.write(results["best_resort_decision"])
    st.markdown("## Individual Resort Decisions")
    for r in results["per_resort"]:
        st.markdown(f"### {r['resort']}")
        st.write(r["final_resort_decision"])

st.markdown("---")
st.subheader("Resort Breakdown and Daily Forecasts")

for resort in selected_resorts:
    st.markdown(f"## {resort}")
    tabs = st.tabs(date_list)

    for i, day in enumerate(date_list):
        with tabs[i]:
            day_data = all_resort_data[resort].get(day, {})

            # Daily forecast metrics
            if day_data.get("openmeteo_daily"):
                st.markdown("### Forecast Overview")
                daily = day_data.get("openmeteo_daily", {})
                col1, col2, col3 = st.columns(3)
                col1.metric("Max Temp (°C)", daily.get("temperature_2m_max", "N/A"))
                col2.metric("Min Temp (°C)", daily.get("temperature_2m_min", "N/A"))
                col3.metric("Snowfall (cm)", daily.get("snowfall_sum", "N/A"))

            # Hourly temperature plot
            if day_data.get("openmeteo_hourly"):
                st.markdown("### Hourly Temperature Trend")
                hourly_list = day_data.get("openmeteo_hourly", [])
                if hourly_list:
                    df = pd.DataFrame(hourly_list)
                    if "time_utc" in df.columns and not df.empty:
                        df["time_utc"] = pd.to_datetime(df["time_utc"])
                        fig = px.line(df, x="time_utc", y="temperature_2m", title="Hourly Temperature")
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("Hourly temperature data not available")
                else:
                    st.info("Hourly temperature data not available")

            # Trail openings
            if day_data.get("trails_by_difficulty"):
                with st.expander("Trails Open / Difficulty"):
                    st.json(day_data.get("trails_by_difficulty", {}))

            # Conditions snapshot
            if day_data.get("conditions_snapshot"):
                with st.expander("Conditions Snapshot"):
                    st.json(day_data.get("conditions_snapshot", {}))

            # AI Analysis
            st.markdown(
                "Click below for the AI evaluation of whether this day is good for skiing. "
                "Three analyst models review the data and a summarizer provides the final recommendation."
            )
            button_key = f"ai_{resort}_{day}"
            if st.button("Run AI Analysis", key=button_key):
                analysis = evaluate_resort_day(resort, day, day_data)
                st.markdown("### AI Final Decision")
                st.write(analysis["final_resort_decision"])
                with st.expander("Individual Analyst Outputs"):
                    for idx, out in enumerate(analysis["analyst_outputs"]):
                        st.markdown(f"**Analyst {idx+1}**")
                        st.write(out)
