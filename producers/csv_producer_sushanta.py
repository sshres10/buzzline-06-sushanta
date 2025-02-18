"""
csv_producer_arsenal.py

Stream Arsenal player performance data to a Kafka topic.

Each CSV record represents a player's in-game performance stats.

Example JSON-transferred CSV message:
{"timestamp": "2025-01-11T18:15:00Z", "player": "Bukayo Saka", "stat": "goals", "value": 1}

"""

#####################################
# Import Modules
#####################################

import os
import sys
import time
import pathlib  # Work with file paths
import csv  # Handle CSV data
import json  # Work with JSON data
from datetime import datetime  # Work with timestamps

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
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


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("ARSENAL_STATS_INTERVAL_SECONDS", 2))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

DATA_FOLDER = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

DATA_FILE = DATA_FOLDER.joinpath("arsenal_player_stats.csv")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Message Generator
#####################################


def generate_messages(file_path: pathlib.Path):
    """
    Read from a CSV file and yield records one by one, continuously.

    Args:
        file_path (pathlib.Path): Path to the CSV file.

    Yields:
        dict: A dictionary containing the player performance data.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {DATA_FILE}")
            with open(DATA_FILE, "r") as csv_file:
                logger.info(f"Reading Arsenal player data from file: {DATA_FILE}")

                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    # Ensure required fields are present
                    if "player" not in row or "stat" not in row or "value" not in row:
                        logger.error(f"Missing columns in row: {row}")
                        continue

                    # Generate a timestamp and prepare the message
                    current_timestamp = datetime.utcnow().isoformat()
                    message = {
                        "timestamp": current_timestamp,
                        "player": row["player"],
                        "stat": row["stat"],
                        "value": int(row["value"]),
                    }
                    logger.debug(f"Generated message: {message}")
                    yield message
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)


#####################################
# Define main function for this module.
#####################################


def main():
    """
    Main entry point for the producer.

    - Reads the Kafka topic name from an environment variable.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams messages to the Kafka topic.
    """

    logger.info("🏟️ START Arsenal player stats producer.")
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Verify the data file exists
    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"📢 Starting Arsenal stats streaming to topic '{topic}'...")
    try:
        for csv_message in generate_messages(DATA_FILE):
            producer.send(topic, value=csv_message)
            logger.info(f"📤 Sent player stat update: {csv_message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("🔚 Kafka producer closed.")

    logger.info("🔴⚪ END Arsenal stats producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
