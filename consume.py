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
    for i, key in enumerate(timestamps.keys()):
        ts = timestamps[key]
        times = [datetime.strptime(str(datetime.fromtimestamp(t)), "%Y-%m-%d %H:%M:%S").replace(second=0) for t in ts]
        
        counts = Counter(times)
        if len(times) == 0:
            continue
        
        min_time = min(times)
        max_time = max(times)
        time_series = []
        time_point = min_time
        while time_point <= max_time:
            time_series.append(time_point)
            time_point += timedelta(seconds=15)
        
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
