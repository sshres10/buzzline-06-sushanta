import os
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from kafka import KafkaConsumer
from datetime import datetime
from collections import deque
from utils.utils_logger import logger

#####################################
# Kafka Configuration
#####################################

TOPIC = os.getenv("MATCH_STATS_TOPIC", "arsenal_liverpool_match")
GROUP_ID = os.getenv("MATCH_STATS_CONSUMER_GROUP_ID", "football_fans")
BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

#####################################
# Kafka Consumer Setup
#####################################

def create_consumer():
    """Creates and returns a Kafka consumer."""
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

#####################################
# Match Statistics
#####################################

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

#####################################
# Matplotlib Visualization
#####################################

fig, ax = plt.subplots(1, 2, figsize=(12, 5))

def process_message(message):
    """Processes each Kafka message and updates match statistics."""
    team = message.get("team", "Unknown")
    event = message.get("event", "Unknown")
    value = message.get("value", 0)

    if event == "goal":
        match_stats[team]["Goals"] += value
    elif event == "shot":
        match_stats[team]["Shots on Target"] += value
    elif event == "possession":
        match_stats[team]["Possession"] = max(0, min(100, value))  # Ensure possession is between 0-100

    # Append timestamp for time-series visualization
    timestamps.append(datetime.utcnow().strftime("%H:%M:%S"))
    arsenal_goals.append(match_stats["Arsenal"]["Goals"])
    liverpool_goals.append(match_stats["Liverpool"]["Goals"])
    arsenal_shots.append(match_stats["Arsenal"]["Shots on Target"])
    liverpool_shots.append(match_stats["Liverpool"]["Shots on Target"])

    logger.info(f"🔄 Updated Stats: {match_stats}")

def animate(i, consumer):
    """Fetches new messages and updates charts dynamically."""
    new_messages = 0
    try:
        batch = consumer.poll(timeout_ms=1000)  # Fetch multiple messages
        for _, messages in batch.items():
            for message in messages:
                process_message(message.value)
                new_messages += 1

        if new_messages:
            logger.info(f"📥 Processed {new_messages} new messages")

    except Exception as e:
        logger.error(f"❌ Error consuming messages: {e}")

    # 🔹 **Bar Chart** (Latest Match Stats)
    ax[0].clear()
    categories = ["Goals", "Shots on Target", "Possession"]
    arsenal_values = [match_stats["Arsenal"].get(cat, 0) for cat in categories]
    liverpool_values = [match_stats["Liverpool"].get(cat, 0) for cat in categories]

    x = range(len(categories))
    ax[0].bar(x, arsenal_values, width=0.4, label="Arsenal", color="red", alpha=0.7)
    ax[0].bar(x, liverpool_values, width=0.4, label="Liverpool", color="blue", alpha=0.7)
    ax[0].set_xticks(x, categories)
    ax[0].set_ylabel("Stats")
    ax[0].set_title("Arsenal vs. Liverpool - Live Stats")
    ax[0].legend()
    ax[0].set_ylim(0, max(10, max(arsenal_values + liverpool_values)))  # Auto-scale Y-axis

    # 🔹 **Line Chart** (Match Timeline)
    ax[1].clear()
    ax[1].plot(timestamps, arsenal_goals, "ro-", label="Arsenal Goals")
    ax[1].plot(timestamps, liverpool_goals, "bo-", label="Liverpool Goals")
    ax[1].plot(timestamps, arsenal_shots, "go--", label="Arsenal Shots")
    ax[1].plot(timestamps, liverpool_shots, "mo--", label="Liverpool Shots")

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
