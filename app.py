import streamlit as st
from datetime import datetime, timedelta

# Import updated engine
from RAG_architecture import (
    analyze_resort_day,
    run_full_daily_analysis,
    pick_best_resort,
    RESORT_MODELS
)

st.set_page_config(layout="wide")
st.title("❄️ Ski Intelligence Dashboard")
st.markdown("### Multi-Resort 7-Day Condition Analyzer")

# Select the resort options

available_resorts = list(RESORT_MODELS.keys())

selected_resorts = st.multiselect(
    "Select Resorts",
    available_resorts,
    default=available_resorts[:3]
)

if not selected_resorts:
    st.warning("Select at least one resort.")
    st.stop()

# Generate the next 7 date strings

today = datetime.today()

date_list = [
    (today + timedelta(days=i)).strftime("%Y-%m-%d")
    for i in range(7)
]

# Placeholder for our data, TBD

all_resort_data = {
    resort: {day: {} for day in date_list}
    for resort in selected_resorts
}

# Analysis to determine best ski resort to go to

st.markdown("---")
st.subheader("Global Best Resort (AI Consensus)")

if st.button("Run Global Analysis"):

    with st.spinner("Running multi-resort analysis..."):

        results = run_full_daily_analysis(
            day=date_list[0],
            all_resort_data=all_resort_data
        )

        best = pick_best_resort(results["per_resort"])

        st.success("Analysis Complete")

        st.markdown("### Best Resort Decision")
        st.write(best)

        st.markdown("### Individual Resort Decisions")

        for r in results["per_resort"]:
            st.markdown(f"#### {r['resort']}")
            st.write(r["final_resort_decision"])

# Resort breakdown

st.markdown("---")
st.subheader("Resort Breakdown")

for resort in selected_resorts:

    st.markdown(f"## {resort}")

    tabs = st.tabs(date_list)

    for i, day in enumerate(date_list):

        with tabs[i]:

            st.markdown(f"### {day}")

            resort_data = all_resort_data.get(resort, {})
            day_data = resort_data.get(day, {})

            if not day_data:
                st.info("No forecast data loaded yet.")
                continue

            st.json(day_data)

            with st.spinner("Analyzing conditions..."):

                result = analyze_resort_day(
                    resort=resort,
                    day=day,
                    day_data=day_data
                )

            st.markdown("### Analyst Outputs")

            for idx, analyst_output in enumerate(result["analyst_outputs"]):
                st.markdown(f"**Analyst {idx+1}**")
                st.write(analyst_output)

            st.markdown("### Final Day Decision")
            st.success(result["final_resort_decision"])

    # Resort summaries

    st.markdown("### Overall Resort Summary")

    today = date_list[0]
    summary_data = all_resort_data.get(resort, {}).get(today, {})

    if summary_data:

        with st.spinner("Generating overall resort summary..."):

            summary_result = analyze_resort_day(
                resort=resort,
                day=today,
                day_data=summary_data
            )

        st.write(summary_result["final_resort_decision"])

    else:
        st.info("No summary available yet.")
