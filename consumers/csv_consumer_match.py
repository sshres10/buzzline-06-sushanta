import os
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
from datetime import datetime
from collections import deque
from utils.utils_logger import logger

# Kafka Configuration
TOPIC = os.getenv("MATCH_STATS_TOPIC", "arsenal_liverpool_match")
GROUP_ID = os.getenv("MATCH_STATS_CONSUMER_GROUP_ID", "football_fans")

def create_consumer():
    """Creates and returns a Kafka consumer."""
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

# Match Statistics
match_stats = {
    "Arsenal": {"Goals": 0, "Shots on Target": 0, "Possession": 50},
    "Liverpool": {"Goals": 0, "Shots on Target": 0, "Possession": 50}
}

# Rolling Window for Time-Series
timestamps = deque(maxlen=20)
arsenal_goals = deque(maxlen=20)
liverpool_goals = deque(maxlen=20)
arsenal_shots = deque(maxlen=20)
liverpool_shots = deque(maxlen=20)

# Initialize Figure and Axes for Matplotlib
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

def process_message(message):
    """Processes each Kafka message and updates match statistics."""
    global timestamps, arsenal_goals, liverpool_goals, arsenal_shots, liverpool_shots

    team = message.get("team", "Unknown")
    event = message.get("event", "Unknown")
    value = message.get("value", 0)

    if event == "goal":
        match_stats[team]["Goals"] += value
    elif event == "shot":
        match_stats[team]["Shots on Target"] += value
    elif event == "possession":
        match_stats[team]["Possession"] = value

    # Append timestamp for time-series visualization
    timestamps.append(datetime.utcnow().strftime("%H:%M:%S"))
    arsenal_goals.append(match_stats["Arsenal"]["Goals"])
    liverpool_goals.append(match_stats["Liverpool"]["Goals"])
    arsenal_shots.append(match_stats["Arsenal"]["Shots on Target"])
    liverpool_shots.append(match_stats["Liverpool"]["Shots on Target"])

    logger.info(f"Updated Stats: {match_stats}")

def animate(i, consumer):
    """Updates both the bar and line charts dynamically."""
    for message in consumer:
        process_message(message.value)
        break  # Process only one message per cycle

    ax[0].clear()  # Clear bar chart before updating
    ax[1].clear()  # Clear line chart before updating

    # 🔹 **Bar Chart** (Latest Match Stats)
    categories = ["Goals", "Shots on Target", "Possession"]
    arsenal_values = [match_stats["Arsenal"].get(cat, 0) for cat in categories]
    liverpool_values = [match_stats["Liverpool"].get(cat, 0) for cat in categories]

    x = range(len(categories))
    ax[0].bar(x, arsenal_values, width=0.4, label="Arsenal", align='center', alpha=0.7, color="red")
    ax[0].bar(x, liverpool_values, width=0.4, label="Liverpool", align='edge', alpha=0.7, color="blue")
    ax[0].set_xticks(ticks=x, labels=categories)
    ax[0].set_ylabel("Stats")
    ax[0].set_title("Arsenal vs. Liverpool - Live Stats")
    ax[0].legend()
    ax[0].set_ylim(0, 100)

    # 🔹 **Line Chart** (Match Event Timeline)
    ax[1].plot(timestamps, arsenal_goals, marker="o", linestyle="-", label="Arsenal Goals", color="red")
    ax[1].plot(timestamps, liverpool_goals, marker="o", linestyle="-", label="Liverpool Goals", color="blue")
    ax[1].plot(timestamps, arsenal_shots, marker="x", linestyle="--", label="Arsenal Shots", color="orange")
    ax[1].plot(timestamps, liverpool_shots, marker="x", linestyle="--", label="Liverpool Shots", color="purple")

    ax[1].set_title("Match Event Timeline")
    ax[1].set_ylabel("Count")
    ax[1].set_xlabel("Time")
    ax[1].legend()
    ax[1].tick_params(axis='x', rotation=45)

    plt.tight_layout()

def main():
    """Starts the Kafka consumer and runs the Matplotlib animation."""
    logger.info("🎙️ Arsenal vs. Liverpool Consumer started.")
    consumer = create_consumer()

    ani = animation.FuncAnimation(fig, animate, fargs=(consumer,), interval=2000)
    plt.show()

    logger.info("🔚 Consumer closed.")

if __name__ == "__main__":
    main()
