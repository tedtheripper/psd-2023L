import time 
import json 
import random 
from datetime import datetime
from kafka import KafkaProducer

def serializer(message):
    return json.dumps(message).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9094'],
    value_serializer=serializer
)

device_ids = [
    "temp_1",
    "temp_2",
    "temp_3",
    "temp_4",
    "temp_5",
]

def generate_temperature():
    return random.randrange(-50, 50)

def fetch_temperature():
    temp_value = generate_temperature()
    timestamp = datetime.timestamp(datetime.now())
    return {
        "id": random.choice(device_ids),
        "timestamp": timestamp,
        "value": temp_value
    }

if __name__ == '__main__':
    while True:
        dummy_message = fetch_temperature()
        
        print(f'Producing message @ {dummy_message["timestamp"]} | Message = {str(dummy_message)}')
        producer.send('Temperatura', dummy_message)
        time_to_sleep = random.randint(2, 5)
        time.sleep(time_to_sleep)