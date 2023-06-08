import os
from dataclasses import dataclass
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
import numpy as np
from utils import generate_rate_of_return, send_to_kafka, InvestmentParameters
import threading
import logging

def serializer(message):
    return json.dumps(message).encode("utf-8")


bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")

producer = KafkaProducer(
    bootstrap_servers=["localhost:9094"], value_serializer=serializer
)


# Parametry dla inwestycji A-E
investments = [
    InvestmentParameters("A", 1e-3, 1),
    InvestmentParameters("B", 4e-3, 16),
    InvestmentParameters("C", 2e-3, 4),
    InvestmentParameters("D", 4e-3, 25),
    InvestmentParameters("E", 3e-3, 9),
]


def produce_rates(thread_id: int, investment_parameters: InvestmentParameters):
    print(f"Started thread: {thread_id}")
    while True:
        generated_rate = generate_rate_of_return(investment_parameters)
        send_to_kafka(producer, generated_rate)
        print(f"Sent new rate for {generated_rate.investment_name} investment | {generated_rate.value}")
        time_to_sleep = np.random.randint(1, 10)
        time.sleep(time_to_sleep)


threads = []
for i, inv in enumerate(investments):
    t = threading.Thread(
        target=produce_rates,
        args=(
            i,
            inv,
        ),
    )
    threads.append(t)
    t.start()

for t in threads:
    t.join()
