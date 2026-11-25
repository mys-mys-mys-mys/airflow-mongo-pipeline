import os
from typing import cast
import re
from datetime import datetime, timedelta
import pandas as pd
from airflow.sdk import dag, task, task_group, Asset
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk.exceptions import AirflowFailException
import logging

INPUT_PATH = "data/raw/tiktok_google_play_reviews.csv"
OUTPUT_PATH = "data/processed/reviews_data.csv" 
CLEAN_DATASET = Asset(f"file://{OUTPUT_PATH}")


# Logger to show that the file is empty ltr
logger = logging.getLogger("airflow.task")

@dag(
    dag_id="data_processing",
    start_date = datetime(2026,1,1),
    schedule = "@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2)
    },
    description= "Dag that checks for files in data/raw processes them and passes to another dag."
) 
def data_processing_dag():
    # Checks in filepath every minute 
    get_file = FileSensor(
        task_id = "get_file",
        filepath = "data/raw/tiktok_google_play_reviews.csv",
        poke_interval = 60,
        mode = 'reschedule',
    )
    
    @task.branch
    def check_if_file_empty(filepath: str) -> str:
        """Checks if file is empty and if true goes to log, else - processes"""
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            return "data_processing_tasks"
        return "log_empty_file"
    
    @task()
    def log_empty_file(filepath: str):
        logger.warning(f"Input file {str} is empty or missing! File will not be processed.")

    @task_group(group_id="data_processing_tasks")
    def data_processing_tasks():
        @task
        def replace_null_values(src_path: str , out_path: str) -> str:
            """Finds literal null and empty writes and replaces with '-'"""
            df = pd.read_csv(src_path)
            for col in df:
                    # If number is empty replace with 0
                    dt = df[col].dtype 
                    if (dt == int) or (dt == float):
                        df[col] = df[col].fillna(0)
                    else:
                        df[col] = df[col].fillna("-")
            df.replace(r'^\s*$', '-', regex=True, inplace=True) # Regex for an empty string check
            df.to_csv(out_path,index=False)
            return out_path

        @task
        def sort_data(src_path: str, out_path: str) -> str:
            """Sorts by date"""
            df = pd.read_csv(src_path)
            df['at'] = pd.to_datetime(df['at'])
            parsed_dates = pd.to_datetime(df["at"], errors="coerce")
            # Added in case datetime values have incorrect format or replaced by '-'
            invalid_count = parsed_dates.isna().sum()
            if invalid_count > 0:
                logger.warning(f"{invalid_count} row(s) had unparseable 'repliedAt' values.")
            df["at"] = parsed_dates
            df.sort_values(by="at", na_position="last", inplace=True)
            df.to_csv(out_path, index=False, na_rep="-")
            return out_path

        @task(outlets=[CLEAN_DATASET])
        def remove_unnecessary_chars(src_path: str, out_path: str) -> str:
            """Formats leaving only text and punctuatuion marks"""
            df = pd.read_csv(src_path)
            def clean_text(text):
                if not isinstance(text, str):
                    return text
                # Remove everything except alphanumeric characters, standard whitespace, and punctuation
                cleaned = re.sub(r"[^\w\s\.,!?;:\'\"-]", "", text)
                return cleaned if cleaned else "-"
            df["content"] = df["content"].apply(clean_text)
            df.to_csv(out_path, index=False)
            return out_path
        
        step1_out = "data/stage1_nonull.csv"
        step2_out = "data/stage2_sorted.csv"

        # Cast because Pylance is angry
        t1 = replace_null_values(INPUT_PATH, step1_out)
        t2 = sort_data(cast(str, t1), step2_out)
        t3 = remove_unnecessary_chars(cast(str, t2), OUTPUT_PATH)

        t1 >> t2 >> t3

    check_task = check_if_file_empty(INPUT_PATH)
    log_task = log_empty_file(INPUT_PATH)
   

    get_file >> check_task
    check_task >> [log_task, data_processing_tasks()]

data_processing_dag()