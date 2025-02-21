#####################################
# Producer: csv_producer_match.py
#####################################

import os
import sys
import time
import pathlib
import csv
import json
import random
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
from utils.utils_logger import logger

# Load environment variables
load_dotenv()

def get_kafka_topic() -> str:
    return os.getenv("MATCH_STATS_TOPIC", "arsenal_liverpool_match")

def get_message_interval() -> int:
    return int(os.getenv("MATCH_STATS_INTERVAL_SECONDS", 2))

TOPIC = get_kafka_topic()
INTERVAL = get_message_interval()

def create_producer():
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )

def generate_match_events():
    players = ["Saka", "Odegaard", "Salah", "Nunez"]
    teams = {"Saka": "Arsenal", "Odegaard": "Arsenal", "Salah": "Liverpool", "Nunez": "Liverpool"}
    events = ["goal", "shot", "possession", "yellow_card", "red_card"]
    
    while True:
        player = random.choice(players)
        event = random.choice(events)
        team = teams[player]
        timestamp = datetime.utcnow().isoformat()
        value = random.randint(1, 3) if event in ["goal", "shot"] else random.randint(40, 60) if event == "possession" else 1
        
        message = {"timestamp": timestamp, "team": team, "player": player, "event": event, "value": value}
        logger.info(f"Generated event: {message}")
        yield message

def main():
    logger.info("⚽ Starting Arsenal vs. Liverpool match producer")
    producer = create_producer()
    
    try:
        for match_event in generate_match_events():
            producer.send(TOPIC, value=match_event)
            logger.info(f"📤 Sent match event: {match_event}")
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    finally:
        producer.close()
        logger.info("🔚 Kafka producer closed.")

if __name__ == "__main__":
    main()
