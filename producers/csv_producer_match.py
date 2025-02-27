import os
import time
import json
import random
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from utils.utils_logger import logger

# Load environment variables
load_dotenv()

#####################################
# Configuration
#####################################

TOPIC = os.getenv("MATCH_STATS_TOPIC", "arsenal_liverpool_match")
INTERVAL = int(os.getenv("MATCH_STATS_INTERVAL_SECONDS", 2))
BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE", 5))  # Send after `BATCH_SIZE` messages

#####################################
# Kafka Producer Setup
#####################################

def create_producer():
    """Initialize Kafka producer with retry logic."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=BROKER,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            linger_ms=500,  # Reduce Kafka overhead by waiting 500ms for potential batch
            retries=5  # Auto-retry sending messages
        )
        logger.info("✅ Kafka Producer connected successfully.")
        return producer
    except KafkaError as e:
        logger.error(f"❌ Kafka Producer connection failed: {e}")
        time.sleep(5)
        return create_producer()  # Retry connection

#####################################
# Match Event Generator
#####################################

def generate_match_events():
    """
    Generates real-time match events for Arsenal vs. Liverpool.
    
    Events: goal, shot, possession, yellow_card, red_card, corner_kick, foul.
    
    Returns:
        Generator: Yields event dictionaries.
    """
    players = ["Saka", "Odegaard", "Salah", "Nunez"]
    teams = {"Saka": "Arsenal", "Odegaard": "Arsenal", "Salah": "Liverpool", "Nunez": "Liverpool"}
    events = ["goal", "shot", "possession", "yellow_card", "red_card", "corner_kick", "foul"]

    while True:
        player = random.choice(players)
        event = random.choice(events)
        team = teams[player]
        timestamp = datetime.utcnow().isoformat()

        value = {
            "goal": random.randint(1, 3),
            "shot": random.randint(1, 3),
            "corner_kick": random.randint(1, 3),
            "possession": random.randint(40, 60),
            "yellow_card": 1,
            "red_card": 1,
            "foul": 1
        }.get(event, 0)  # Default to 0 if unexpected event

        message = {"timestamp": timestamp, "team": team, "player": player, "event": event, "value": value}
        logger.info(f"Generated event: {message}")
        yield message

#####################################
# Main Producer Loop
#####################################

def main():
    """Starts Kafka producer and continuously streams match events."""
    logger.info("⚽ Starting Arsenal vs. Liverpool match producer")
    producer = create_producer()

    message_batch = []  # Store messages for batch sending

    try:
        for match_event in generate_match_events():
            try:
                producer.send(TOPIC, value=match_event)
                message_batch.append(match_event)
                logger.info(f"📤 Sent match event: {match_event}")

                # Send messages in batch after `BATCH_SIZE`
                if len(message_batch) >= BATCH_SIZE:
                    producer.flush()  # Flush the batch to Kafka
                    logger.info(f"📦 Flushed {len(message_batch)} messages to Kafka")
                    message_batch.clear()  # Reset batch

                time.sleep(INTERVAL)

            except KafkaError as e:
                logger.error(f"❌ Kafka send error: {e}")

    except KeyboardInterrupt:
        logger.warning("⏹️ Producer interrupted by user.")
    finally:
        producer.flush()  # Ensure remaining messages are sent
        producer.close()
        logger.info("🔚 Kafka producer closed.")

if __name__ == "__main__":
    main()
