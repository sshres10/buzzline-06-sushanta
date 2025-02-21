#####################################
# Consumer: csv_consumer_match.py
#####################################

import os
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
from utils.utils_logger import logger

def get_kafka_topic() -> str:
    return os.getenv("MATCH_STATS_TOPIC", "arsenal_liverpool_match")

def get_kafka_consumer_group_id() -> str:
    return os.getenv("MATCH_STATS_CONSUMER_GROUP_ID", "football_fans")

TOPIC = get_kafka_topic()
GROUP_ID = get_kafka_consumer_group_id()

def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

def process_message(message, match_stats):
    team = message.get("team", "Unknown")
    event = message.get("event", "Unknown")
    value = message.get("value", 0)
    
    if event == "goal":
        match_stats[team]["Goals"] += value
    elif event == "shot":
        match_stats[team]["Shots on Target"] += value
    elif event == "possession":
        match_stats[team]["Possession"] = value

def animate(i, consumer, match_stats):
    for message in consumer:
        process_message(message.value, match_stats)
        break
    
    plt.clf()
    categories = ["Goals", "Shots on Target", "Possession"]
    arsenal_values = [match_stats["Arsenal"].get(cat, 0) for cat in categories]
    liverpool_values = [match_stats["Liverpool"].get(cat, 0) for cat in categories]

    x = range(len(categories))
    plt.bar(x, arsenal_values, width=0.4, label="Arsenal", align='center', alpha=0.7)
    plt.bar(x, liverpool_values, width=0.4, label="Liverpool", align='edge', alpha=0.7)

    plt.xticks(ticks=x, labels=categories)
    plt.ylabel("Match Stats")
    plt.title("Arsenal vs. Liverpool - Live Match Tracker")
    plt.legend()
    plt.ylim(0, 100)

def main():
    logger.info("🎙️ Arsenal vs. Liverpool Consumer started.")
    consumer = create_consumer()
    match_stats = {"Arsenal": {"Goals": 0, "Shots on Target": 0, "Possession": 50},
                   "Liverpool": {"Goals": 0, "Shots on Target": 0, "Possession": 50}}
    
    fig = plt.figure(figsize=(8, 5))
    ani = animation.FuncAnimation(fig, animate, fargs=(consumer, match_stats), interval=2000)
    plt.show()
    
    logger.info("🔚 Consumer closed.")

if __name__ == "__main__":
    main()