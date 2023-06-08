import json
from kafka import KafkaConsumer

if __name__ == '__main__':
    consumer = KafkaConsumer(
        'Alarm',
        bootstrap_servers='localhost:9094',
        auto_offset_reset='earliest',
    )
    for message in consumer:
        msg = json.loads(message.value)
        print(msg)
