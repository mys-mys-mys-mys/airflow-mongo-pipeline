FROM apache/airflow:latest-python3.12

USER airflow 

COPY requirements.txt /requirements.txt

RUN --mount=type=cache,target=/home/airflow/.cache/pip,uid=50000 \
    pip install -r /requirements.txt 

