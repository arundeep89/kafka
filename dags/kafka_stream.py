import uuid
import requests
import json
from kafka import KafkaProducer
import time
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'arundeep89',
    'start_date': datetime(2025, 5, 21, 10, 00)
}

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0] 
    return res

def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['email'] = res['email']
    return data


def stream_data():
    # localhost:9092 vs broker:29092
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('users_created', json.dumps(res).encode('utf-8'))
            time.sleep(3)
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG('kafka_stream_dag',
         description='A simple Kafka streaming DAG',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

    streaming_task