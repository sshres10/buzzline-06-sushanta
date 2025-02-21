import os
import time
import json
import random
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
from utils.utils_logger import logger

# Load environment variables
load_dotenv()

def get_kafka_topic():
    return os.getenv("MATCH_STATS_TOPIC", "arsenal_liverpool_match")

def get_message_interval():
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
    events = ["goal", "shot", "possession", "yellow_card", "red_card", "corner_kick", "foul"]

    while True:
        player = random.choice(players)
        event = random.choice(events)
        team = teams[player]
        timestamp = datetime.utcnow().isoformat()
        
        if event in ["goal", "shot", "corner_kick"]:
            value = random.randint(1, 3)
        elif event == "possession":
            value = random.randint(40, 60)
        elif event in ["yellow_card", "red_card", "foul"]:
            value = 1
        else:
            value = 0
        
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
