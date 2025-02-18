"""
csv_consumer_arsenal.py

Consume Arsenal player stats from a Kafka topic.

Example message:
{"timestamp": "2025-01-11T18:15:00Z", "player": "Bukayo Saka", "stat": "goals", "value": 1}

"""

#####################################
# Import Modules
#####################################

import os
import json
from collections import deque
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("ARSENAL_STATS_TOPIC", "arsenal_player_stats")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id = os.getenv("ARSENAL_STATS_CONSUMER_GROUP_ID", "arsenal_fans")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Process Messages
#####################################


def process_message(message: str) -> None:
    """
    Process a single player stat update.

    Args:
        message (str): JSON message received from Kafka.
    """
    try:
        logger.debug(f"Raw message: {message}")
        message_dict = json.loads(message)

        if isinstance(message_dict, dict):
            player = message_dict.get("player", "Unknown")
            stat = message_dict.get("stat", "Unknown")
            value = message_dict.get("value", 0)

            if stat == "goals":
                print(f"⚽ GOAL ALERT: {player} has scored! Total goals: {value}")
            elif stat == "passes":
                print(f"🎯 PASS MASTER: {player} has completed {value} passes!")
            elif stat == "tackles":
                print(f"💪 DEFENSIVE WALL: {player} has made {value} tackles!")
            else:
                print(f"📢 Player Update: {player} | {stat}: {value}")
        else:
            logger.error(f"Unexpected message format: {message_dict}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Main Function
#####################################


def main() -> None:
    logger.info("🎙️ Arsenal Stats Consumer started.")
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    consumer = create_kafka_consumer(topic, group_id)

    for message in consumer:
        process_message(message.value)

    logger.info("🔴⚪ Arsenal Stats Consumer closed.")


if __name__ == "__main__":
    main()
