from datetime import datetime, timedelta
import json
from kafka import KafkaConsumer
from collections import Counter
import matplotlib.pyplot as plt

consumer = KafkaConsumer(
    'Alarm',
    bootstrap_servers='localhost:9094',
    auto_offset_reset='earliest',
)

timestamps = {
    "A": [],
    "B": [],
    "C": [],
    "D": [],
    "E": [],
}

fig, axs = plt.subplots(5, 1)
plt.ion()
plt.margins(x=0, y=0)

# fig.suptitle('Wykres')

def generate_plot():
    all_times = [datetime.fromtimestamp(t) for ts in timestamps.values() for t in ts]
    all_times = [t.replace(second=0) for t in all_times]

    min_time = min(all_times)
    max_time = max(all_times)
    time_series = []
    time_point = min_time
    while time_point <= max_time:
        time_series.append(time_point)
        time_point += timedelta(minutes=1)
    for i, key in enumerate(timestamps.keys()):
        ts = timestamps[key]
        times = [datetime.fromtimestamp(t) for t in ts]
        times = [t.replace(second=0) for t in times]

        counts = Counter(times)
        
        count_series = [counts[time_point] for time_point in time_series]

        axs[i].cla()
        axs[i].plot(time_series, count_series, marker='o')
        axs[i].set_xlabel('Time')
        axs[i].set_ylabel('Number of Occurrences')
        axs[i].set_title(f'Number of Alarms Per Minute: {key}')
        axs[i].tick_params(axis="x", rotation=45)
    
    plt.subplots_adjust(top=0.95, bottom=0.1, hspace=0.4)
    plt.pause(0.05)
    plt.draw()

for message in consumer:
    msg = json.loads(message.value)
    timestamps[msg["investmentName"]].append(msg["timestamp"])
    generate_plot()
