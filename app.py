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

# Let user pick resorts
available_resorts = list(RESORT_MODELS.keys())
selected_resorts = st.multiselect(
    "Select Resorts",
    available_resorts,
    default=available_resorts[0:3]
)

if not selected_resorts:
    st.stop()

# Fixed analysis window
date_list = [
    "2026-02-24", "2026-02-25", "2026-02-26",
    "2026-02-27", "2026-02-28", "2026-03-01"
]

# Caching DB pulls
@st.cache_data(show_spinner=False)
def load_resort_data(resort):
    resort_data = {}
    for day in date_list:
        start_ts = datetime.strptime(day, "%Y-%m-%d")
        end_ts = start_ts + timedelta(days=1)
        resort_data[day] = fetch_resort_full_snapshot(resort, start_ts, end_ts)
    return resort_data

all_resort_data = {resort: load_resort_data(resort) for resort in selected_resorts}

# Caching AI outputs
@st.cache_data(show_spinner=False)
def cached_ai_analysis(resort, day, day_data):
    return evaluate_resort_day(resort, day, day_data)

# Sidebar Actions
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
    "Run a full comparison across selected resorts for the first day."
)
if st.sidebar.button("Run Global Comparison"):
    first_day = date_list[0]
    with st.spinner("Running global ski analysis..."):
        try:
            results = run_full_daily_analysis(first_day, all_resort_data)
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

# Main Dashboard
for resort in selected_resorts:
    st.markdown(f"## {resort}")
    tabs = st.tabs(date_list)

    for i, day in enumerate(date_list):
        with tabs[i]:
            day_data = all_resort_data[resort].get(day, {})

            # Daily forecast metrics
            daily = day_data.get("openmeteo_daily") or {}
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Max Temp (°C)", daily.get("max_temp_ski_hours_c", "N/A"))
            col2.metric("Min Temp (°C)", daily.get("min_temp_ski_hours_c", "N/A"))
            col3.metric("Snowfall (cm)", daily.get("snowfall_ski_day_cm", "N/A"))
            col4.metric("Freeze-Thaw Cycles", daily.get("freeze_thaw_cycles_prev_48h", "N/A"))

            col5, col6 = st.columns(2)
            col5.metric("Wind Avg (km/h)", daily.get("avg_wind_speed_ski_hours_kmh", "N/A"))
            col6.metric("Wind Gust Max (km/h)", daily.get("max_wind_gust_ski_hours_kmh", "N/A"))

            # Hourly Temperature Graph
            hourly_list = day_data.get("openmeteo_hourly") or []
            if hourly_list:
                df = pd.DataFrame(hourly_list)
                if "time_utc" in df.columns and "temperature_2m" in df.columns:
                    df["time_utc"] = pd.to_datetime(df["time_utc"])
                    fig = px.line(df, x="time_utc", y="temperature_2m", title="Hourly Temperature")
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Hourly temperature data not available")

            # Trails info
            trails = day_data.get("trails_by_difficulty") or {}
            if trails:
                with st.expander("Trails Open / Difficulty"):
                    st.json(trails)
            else:
                st.info("Trail data not available")

            # Conditions snapshot
            conditions = day_data.get("conditions_snapshot") or {}
            if conditions:
                with st.expander("Conditions Snapshot"):
                    st.json(conditions)
            else:
                st.info("Conditions snapshot not available")

            # AI section
            st.markdown(
                "Click below to run the AI evaluation for this day."
            )
            button_key = f"ai_{resort}_{day}"
            if st.button("Run AI Analysis", key=button_key):
                with st.spinner("Running AI analysis..."):
                    try:
                        if not day_data:
                            st.warning("No data available for this day")
                        else:
                            analysis = cached_ai_analysis(resort, day, day_data)
                            st.markdown("### AI Final Decision")
                            st.write(analysis["final_resort_decision"])
                            with st.expander("Individual Analyst Outputs"):
                                for idx, out in enumerate(analysis["analyst_outputs"]):
                                    st.markdown(f"**Analyst {idx+1}**")
                                    st.write(out)
                    except Exception as e:
                        st.error(f"AI analysis failed: {e}")
