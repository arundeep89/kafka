import json
import requests
from kafka import KafkaProducer
import logging

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
    try:
        res = get_data()
        res = format_data(res) 
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        producer.send('users_created', json.dumps(res).encode('utf-8'))
    except Exception as e:
        logging.error(f'An error occured: {e}')

stream_data()