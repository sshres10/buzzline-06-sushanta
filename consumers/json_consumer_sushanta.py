"""
json_consumer_arsenal.py

Consume Arsenal match updates from a Kafka topic.

Example message:
{"message": "⚽ GOAL! Bukayo Saka scores a stunning curler! Arsenal 1-0 Spurs", "author": "MatchBot"}

"""

#####################################
# Import Modules
#####################################

import os
import json
from collections import defaultdict
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
    topic = os.getenv("ARSENAL_TOPIC", "arsenal_match_updates")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id = os.getenv("ARSENAL_CONSUMER_GROUP_ID", "arsenal_fans")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Process Messages
#####################################


def process_message(message: str) -> None:
    """
    Process a single match update message.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        logger.debug(f"Raw message: {message}")
        message_dict = json.loads(message)

        if isinstance(message_dict, dict):
            msg = message_dict.get("message", "Unknown event")

            if "GOAL" in msg:
                print(f"🚨 GOAL ALERT: {msg}")
            elif "Yellow Card" in msg:
                print(f"⚠️ YELLOW CARD: {msg}")
            elif "Red Card" in msg:
                print(f"🟥 RED CARD ALERT: {msg}")
            else:
                print(f"📢 Match Update: {msg}")
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
    logger.info("🎙️ Arsenal Match Consumer started.")
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    consumer = create_kafka_consumer(topic, group_id)

    for message in consumer:
        process_message(message.value)

    logger.info("🔴⚪ Arsenal Match Consumer closed.")


if __name__ == "__main__":
    main()
