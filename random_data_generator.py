import random
import json
import time
from datetime import datetime
from kafka import KafkaProducer

# Function to generate random user interaction data
def generate_data():
    user_id = random.randint(1, 1000)  # Simulating 1000 users
    item_id = random.randint(1, 100)   # Simulating 100 items
    interaction_types = ['click', 'view', 'purchase']
    interaction_type = random.choice(interaction_types)
    timestamp = datetime.utcnow().isoformat()

    data = {
        'user_id': user_id,
        'item_id': item_id,
        'interaction_type': interaction_type,
        'timestamp': timestamp
    }
    return data

# Function to send data to Kafka
def send_to_kafka(producer, topic, rate_of_data=1):
    while True:
        data = generate_data()
        producer.send(topic, value=json.dumps(data).encode('utf-8'))
        print(f"Sent: {data}")
        time.sleep(1 / rate_of_data)

if __name__ == "__main__":
    # Kafka configuration
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'user_interactions'

    rate_of_data = 10  # 10 messages per second
    send_to_kafka(producer, topic, rate_of_data)
