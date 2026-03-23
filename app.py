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

# letting user pick resorts
available_resorts = list(RESORT_MODELS.keys())
selected_resorts = st.multiselect(
    "Select Resorts",
    available_resorts,
    default=available_resorts[5:]
)

if not selected_resorts:
    st.stop()

# fixed analysis window
date_list = [
    "2026-02-24", "2026-02-25", "2026-02-26",
    "2026-02-27", "2026-02-28", "2026-03-01"
]

# caching DB pulls so app isn't slow
@st.cache_data(show_spinner=False)
def load_resort_data(resort):
    resort_data = {}

    for day in date_list:
        start_ts = datetime.strptime(day, "%Y-%m-%d")
        end_ts = start_ts + timedelta(days=1)

        resort_data[day] = fetch_resort_full_snapshot(
            resort,
            start_ts,
            end_ts
        )

    return resort_data

all_resort_data = {
    resort: load_resort_data(resort)
    for resort in selected_resorts
}

# caching AI outputs (huge speed boost)
@st.cache_data(show_spinner=False)
def cached_ai_analysis(resort, day, day_data):
    return evaluate_resort_day(resort, day, day_data)

# sidebar actions
st.sidebar.header("Actions")

st.sidebar.markdown(
    "Load PDF/TXT ski advice into the AI. "
    "This gives the models domain knowledge about what actually makes a good or bad ski day."
)

if st.sidebar.button("Ingest AI Ski Conditions Corpus"):
    with st.spinner("Ingesting ski knowledge..."):
        try:
            ingest_corpus()
            st.sidebar.success("Corpus ingested successfully!")
        except Exception as e:
            st.sidebar.error(f"Ingestion failed: {e}")

st.sidebar.markdown(
    "Run a full comparison across selected resorts for the first day. "
    "Each resort is evaluated by multiple AI analysts, and a final model selects the best option."
)

if st.sidebar.button("Run Global Comparison"):

    first_day = date_list[0]

    with st.spinner("Running global ski analysis..."):

        try:
            results = run_full_daily_analysis(
                day=first_day,
                all_resort_data=all_resort_data
            )

            st.sidebar.success("Global analysis complete")

            st.markdown("## Best Overall Resort")
            st.write(results["best_resort_decision"])

            st.markdown("## Individual Resort Decisions")

            for r in results["per_resort"]:
                st.markdown(f"### {r['resort']}")
                st.write(r["final_resort_decision"])

        except Exception as e:
            st.sidebar.error(f"Global analysis failed: {e}")

st.markdown("---")
st.subheader("Resort Breakdown and Daily Forecasts")

# main dashboard
for resort in selected_resorts:

    st.markdown(f"## {resort}")

    tabs = st.tabs(date_list)

    for i, day in enumerate(date_list):

        with tabs[i]:

            day_data = all_resort_data[resort].get(day, {})

            # daily forecast metrics
            daily = day_data.get("openmeteo_daily") or {}

            col1, col2, col3 = st.columns(3)

            col1.metric("Max Temp (°C)", daily.get("temperature_2m_max", "N/A"))
            col2.metric("Min Temp (°C)", daily.get("temperature_2m_min", "N/A"))
            col3.metric("Snowfall (cm)", daily.get("snowfall_sum", "N/A"))

            if not daily:
                st.info("No daily forecast data available")

            if "openmeteo_hourly" in day_data and day_data["openmeteo_hourly"]:
                hourly_list = day_data["openmeteo_hourly"]
                df = pd.DataFrame(hourly_list)
                if not df.empty and "time_utc" in df.columns and "temperature_2m" in df.columns:
                    df["time_utc"] = pd.to_datetime(df["time_utc"])
                    fig = px.line(df, x="time_utc", y="temperature_2m", title="Hourly Temperature")
                    st.plotly_chart(fig, width='stretch')
            else:
                st.info("Hourly temperature data not available")
            
            # trails info
            trails = day_data.get("trails_by_difficulty") or {}

            if trails:
                with st.expander("Trails Open / Difficulty"):
                    st.json(trails)
            else:
                st.info("Trail data not available")

            # conditions snapshot
            conditions = day_data.get("conditions_snapshot") or {}

            if conditions:
                with st.expander("Conditions Snapshot"):
                    st.json(conditions)
            else:
                st.info("Conditions snapshot not available")

            # AI section with description (restored)
            st.markdown(
                "Click below to run the AI evaluation for this day. "
                "Three analyst models independently assess conditions using weather data "
                "and expert ski guidance, and a fourth model summarizes their conclusions."
            )

            button_key = f"ai_{resort}_{day}"

            if st.button("Run AI Analysis", key=button_key):

                with st.spinner("Running AI analysis..."):

                    try:
                        if not day_data:
                            st.warning("No data available for this day")

                        else:
                            analysis = cached_ai_analysis(
                                resort,
                                day,
                                day_data
                            )

                            st.markdown("### AI Final Decision")
                            st.write(analysis["final_resort_decision"])

                            with st.expander("Individual Analyst Outputs"):
                                for idx, out in enumerate(analysis["analyst_outputs"]):
                                    st.markdown(f"**Analyst {idx+1}**")
                                    st.write(out)

                    except Exception as e:
                        st.error(f"AI analysis failed: {e}")
