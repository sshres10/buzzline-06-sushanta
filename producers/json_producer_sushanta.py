"""
json_producer_arsenal.py

Stream live Arsenal match updates to a Kafka topic.

Example JSON message:
{"message": "⚽ GOAL! Bukayo Saka scores a stunning curler! Arsenal 1-0 Spurs", "author": "MatchBot"}

Example serialized to Kafka message:
"{\"message\": \"⚽ GOAL! Bukayo Saka scores a stunning curler! Arsenal 1-0 Spurs\", \"author\": \"MatchBot\"}"

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time
import pathlib  # work with file paths
import json  # work with JSON data

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
    topic = os.getenv("ARSENAL_TOPIC", "arsenal_match_updates")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("ARSENAL_INTERVAL_SECONDS", 2))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where match data is stored
DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

# Set the name of the Arsenal match event file
DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("arsenal_match_events.json")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Message Generator
#####################################


def generate_messages(file_path: pathlib.Path):
    """
    Read from a JSON file and yield Arsenal match events one by one, continuously.

    Args:
        file_path (pathlib.Path): Path to the JSON file.

    Yields:
        dict: A dictionary containing Arsenal match event data.
    """
    while True:
        try:
            logger.info(f"Opening match events file: {DATA_FILE}")
            with open(DATA_FILE, "r") as json_file:
                logger.info(f"Reading Arsenal match events from file: {DATA_FILE}")

                # Load JSON as a list of dictionaries
                json_data: list = json.load(json_file)

                if not isinstance(json_data, list):
                    raise ValueError(
                        f"Expected a list of JSON objects, got {type(json_data)}."
                    )

                # Iterate over match events
                for event in json_data:
                    logger.debug(f"Generated match event: {event}")
                    yield event
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in file: {file_path}. Error: {e}")
            sys.exit(2)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)


#####################################
# Main Function
#####################################


def main():
    """
    Main function for the Arsenal match producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated Arsenal match updates to the Kafka topic.
    """

    logger.info("🏟️ START Arsenal match producer.")
    verify_services()

    # Fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Verify data file exists
    if not DATA_FILE.exists():
        logger.error(f"Match data file not found: {DATA_FILE}. Exiting.")
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
        logger.info(f"Kafka topic '{topic}' is ready for Arsenal updates.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send match updates
    logger.info(f"📢 Streaming Arsenal match updates to topic '{topic}'...")
    try:
        for match_event in generate_messages(DATA_FILE):
            # Send message directly as a dictionary (producer handles serialization)
            producer.send(topic, value=match_event)
            logger.info(f"📤 Sent match update to topic '{topic}': {match_event}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("🔚 Kafka producer closed.")

    logger.info("🔴⚪ END Arsenal match producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
