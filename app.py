import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px

from RAG_architecture import (
    run_full_daily_analysis,
    pick_best_resort,
    RESORT_MODELS,
    fetch_resort_full_snapshot,
    evaluate_resort_day
)

# Basic configurations
st.set_page_config(layout="wide")

st.title("Ski Intelligence Dashboard")

available_resorts = list(RESORT_MODELS.keys())

# Resort picker
selected_resorts = st.multiselect(
    "Select Resorts",
    available_resorts,
    default=available_resorts[:2]
)

if not selected_resorts:
    st.stop()

today = datetime.today()

date_list = [
    (today + timedelta(days=i)).strftime("%Y-%m-%d")
    for i in range(7)
]

# Cache data

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

# Button for global analysis of selected resorts

with st.sidebar:

    if st.button("Run Global Comparison"):

        first_day = date_list[0]

        results = run_full_daily_analysis(
            day=first_day,
            all_resort_data=all_resort_data
        )

        st.success("Global Analysis Complete")

        st.markdown("## Best Overall")
        st.write(results["best_resort_decision"])

        st.markdown("## Resort Results")
        for r in results["per_resort"]:
            st.markdown(f"### {r['resort']}")
            st.write(r["final_resort_decision"])

# Resort breakdown

st.markdown("---")
st.subheader("Resort Breakdown")

for resort in selected_resorts:

    st.markdown(f"## {resort}")

    tabs = st.tabs(date_list)

    for i, day in enumerate(date_list):

        with tabs[i]:

            day_data = all_resort_data[resort][day]

            if not day_data:
                st.info("No data")
                continue

            # Day to day forecast

            if day_data.get("openmeteo_daily"):
                st.markdown("### Forecast")

                daily = day_data["openmeteo_daily"]

                col1, col2, col3 = st.columns(3)

                col1.metric("Max Temp", daily.get("temperature_2m_max"))
                col2.metric("Min Temp", daily.get("temperature_2m_min"))
                col3.metric("Snowfall", daily.get("snowfall_sum"))

            # Conditions raw JSON

            if day_data.get("conditions_snapshot"):
                with st.expander("Conditions"):
                    st.json(day_data["conditions_snapshot"])

            # Trails open and difficulties

            if day_data.get("trails_by_difficulty"):
                with st.expander("Trail Openings"):
                    st.json(day_data["trails_by_difficulty"])

            # Graph for hour by hour temperatures

            if day_data.get("openmeteo_hourly"):

                st.markdown("### Hourly Temperature")

                df = pd.DataFrame(day_data["openmeteo_hourly"])

                if "time_utc" in df.columns:

                    df["time_utc"] = pd.to_datetime(df["time_utc"])

                    fig = px.line(
                        df,
                        x="time_utc",
                        y="temperature_2m",
                        title="Hourly Temp Trend"
                    )

                    st.plotly_chart(fig, use_container_width=True)

            # Button to prompt AI analysis of that day at that resort

            button_key = f"ai_{resort}_{day}"

            if st.button("Run AI Analysis", key=button_key):

                analysis = evaluate_resort_day(
                    resort,
                    day,
                    day_data
                )

                st.markdown("### Final Decision")
                st.write(analysis["final_resort_decision"])

                with st.expander("Analyst Outputs"):
                    for idx, out in enumerate(analysis["analyst_outputs"]):
                        st.markdown(f"**Analyst {idx+1}**")
                        st.write(out)
