from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from runner import (
    cmd_extract_reddit,
    prepare_reddit_new_rows,
    label_reddit_rows_with_mistral,
    aggregate_reddit_resort_day_signals,
    write_df_json_records,
    read_df_json_records,
    upload_raw_file_to_s3,
)

# -------------------------------
# Config
# -------------------------------
BASE_DIR = "/opt/airflow/project/data/reddit_tmp"
Path(BASE_DIR).mkdir(parents=True, exist_ok=True)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# -------------------------------
# DAG
# -------------------------------
with DAG(
    dag_id="ski_reddit_ingest",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 19 * * *",  # 7 PM daily
    catchup=False,
    tags=["reddit", "ski"],
) as dag:

    # -------------------------------
    # 1. Extract Reddit JSON
    # -------------------------------
    def extract_reddit(**context):
        file_path = cmd_extract_reddit(
            timeframe="week",
            max_posts_per_query=100,
        )
        context["ti"].xcom_push(key="raw_file_path", value=file_path)

    extract_task = PythonOperator(
        task_id="extract_reddit_json",
        python_callable=extract_reddit,
    )

    # -------------------------------
    # 2. Upload raw extract to S3
    # -------------------------------
    def upload_raw_to_s3(**context):
        ti = context["ti"]
        raw_file_path = ti.xcom_pull(key="raw_file_path")

        s3_uri = upload_raw_file_to_s3(raw_file_path)
        print(f"[s3] uploaded raw reddit file to {s3_uri}")

        ti.xcom_push(key="raw_s3_uri", value=s3_uri)

    upload_s3_task = PythonOperator(
        task_id="upload_reddit_raw_to_s3",
        python_callable=upload_raw_to_s3,
    )

    # -------------------------------
    # 3. Prepare new rows
    # -------------------------------
    def prepare_new_rows(**context):
        ti = context["ti"]
        raw_file_path = ti.xcom_pull(key="raw_file_path")

        hook = PostgresHook(postgres_conn_id="ski_pg")

        existing_keys_df = hook.get_pandas_df(
            "SELECT source_item_key FROM reddit_labeled"
        )
        existing_keys = existing_keys_df["source_item_key"].tolist()

        new_df = prepare_reddit_new_rows(
            raw_file_path,
            existing_keys=existing_keys,
        )

        print(f"[prepare] new rows: {len(new_df)}")

        out_path = f"{BASE_DIR}/reddit_new.json"
        write_df_json_records(new_df, out_path)

        ti.xcom_push(key="new_rows_path", value=out_path)

    prepare_task = PythonOperator(
        task_id="prepare_new_reddit_rows",
        python_callable=prepare_new_rows,
    )

    # -------------------------------
    # 4. Label rows (Mistral)
    # -------------------------------
    def label_rows(**context):
        ti = context["ti"]
        path = ti.xcom_pull(key="new_rows_path")

        df = read_df_json_records(path)

        print(f"[label] rows to label: {len(df)}")

        labeled_df = label_reddit_rows_with_mistral(df)

        out_path = f"{BASE_DIR}/reddit_labeled.json"
        write_df_json_records(labeled_df, out_path)

        ti.xcom_push(key="labeled_path", value=out_path)

    label_task = PythonOperator(
        task_id="label_new_reddit_rows",
        python_callable=label_rows,
        execution_timeout=timedelta(minutes=30),
    )

    # -------------------------------
    # 5. Load + Aggregate + Upsert
    # -------------------------------
    def load_outputs(**context):
        ti = context["ti"]
        labeled_path = ti.xcom_pull(key="labeled_path")

        df = read_df_json_records(labeled_path)

        hook = PostgresHook(postgres_conn_id="ski_pg")
        conn = hook.get_conn()
        cur = conn.cursor()

        insert_labeled_sql = """
        INSERT INTO reddit_labeled (
            source_item_key,
            post_id,
            comment_id,
            record_type,
            subreddit,
            author,
            title,
            text,
            permalink,
            url,
            created_utc_iso,
            scraped_at_iso,
            report_type,
            resort,
            resolved_date,
            date_resolution_source,
            sentiment_label,
            labeling_model,
            labeled_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_item_key) DO NOTHING;
        """

        labeled_rows = []

        for _, r in df.iterrows():
            labeled_rows.append((
                r["source_item_key"],
                r["post_id"],
                r["comment_id"],
                r["record_type"],
                r["subreddit"],
                r["author"],
                r["title"],
                r["text"],
                r["permalink"],
                r["url"],
                r["created_utc_iso"],
                r["scraped_at_iso"],
                r["report_type"],
                r["resort"],
                r["resolved_date"],
                r["date_resolution_source"],
                r["sentiment_label"],
                r["labeling_model"],
                r["labeled_at"],
            ))

        print(f"[load] inserting labeled rows: {len(labeled_rows)}")

        for row in labeled_rows:
            cur.execute(insert_labeled_sql, row)

        agg_df = aggregate_reddit_resort_day_signals(df)

        print(f"[aggregate] rows: {len(agg_df)}")

        insert_agg_sql = """
        INSERT INTO reddit_resort_day_signals (
            resort,
            ski_date,
            field_report_count,
            forecast_opinion_count,
            positive_count,
            negative_count,
            neutral_count,
            net_sentiment,
            weighted_signal_score,
            signal_present,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (resort, ski_date)
        DO UPDATE SET
            field_report_count = EXCLUDED.field_report_count,
            forecast_opinion_count = EXCLUDED.forecast_opinion_count,
            positive_count = EXCLUDED.positive_count,
            negative_count = EXCLUDED.negative_count,
            neutral_count = EXCLUDED.neutral_count,
            net_sentiment = EXCLUDED.net_sentiment,
            weighted_signal_score = EXCLUDED.weighted_signal_score,
            signal_present = EXCLUDED.signal_present,
            updated_at = EXCLUDED.updated_at;
        """

        for _, r in agg_df.iterrows():
            cur.execute(insert_agg_sql, (
                r["resort"],
                r["ski_date"],
                int(r["field_report_count"]),
                int(r["forecast_opinion_count"]),
                int(r["positive_count"]),
                int(r["negative_count"]),
                int(r["neutral_count"]),
                int(r["net_sentiment"]),
                int(r["weighted_signal_score"]),
                int(r["signal_present"]),
                r["updated_at"],
            ))

        conn.commit()
        cur.close()
        conn.close()

        print("✅ Reddit pipeline load complete")

    load_task = PythonOperator(
        task_id="load_reddit_outputs",
        python_callable=load_outputs,
    )

    # -------------------------------
    # Task order
    # -------------------------------
    extract_task >> upload_s3_task >> prepare_task >> label_task >> load_task