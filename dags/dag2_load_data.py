from datetime import datetime, timedelta
import pandas as pd
from airflow.sdk import dag, task, Asset
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk.exceptions import AirflowFailException
import pymongo
from airflow.providers.mongo.hooks.mongo import MongoHook 
import logging

OUTPUT_FILE = "data/processed/reviews_data.csv"
CLEAN_DATASET = Asset(f"file://{OUTPUT_FILE}")

@dag(
    dag_id="data_loading",
    start_date = datetime(2026,1,1),
    schedule = [CLEAN_DATASET],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2)
    },
    description= "Dag that connects to MongoDB and loads data into the database."
) 
def data_loading_dag():
    @task
    def load_into_mongodb():
        df = pd.read_csv(OUTPUT_FILE)
        # Convert for MongoDB insertion
        if 'at' in df.columns:
            df['at'] = pd.to_datetime(df['at']).dt.to_pydatetime()
        records = df.to_dict(orient="records")
        
        hook = MongoHook(conn_id="mongo_default")
        client = hook.get_conn()
        db = client['airflow_task_db']
        collection = db['reviews']

        collection.delete_many({})
        result = collection.insert_many(records)
        return f"Successfully inserted {len(result.inserted_ids)} records."

    load_into_mongodb()

data_loading_dag()