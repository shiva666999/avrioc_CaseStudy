from kafka import KafkaConsumer
import json
import time
from collections import defaultdict

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'user_interactions',  # Kafka topic
    bootstrap_servers='localhost:9092',
    group_id='consumer_group'
)

# Aggregation variables
interaction_count = defaultdict(int)
item_interactions = defaultdict(list)

# Function to process the Kafka message and perform aggregation
def process_message(message):
    data = json.loads(message.value.decode('utf-8'))
    user_id = data['user_id']
    item_id = data['item_id']
    interaction_type = data['interaction_type']
    
    # Aggregate interactions per user
    interaction_count[user_id] += 1
    
    # Aggregate interactions per item (e.g., interaction type count)
    item_interactions[item_id].append(interaction_type)

# Real-time processing loop
def consume_data():
    for message in consumer:
        process_message(message)
        print(f"Aggregated data: {interaction_count}")
        print(f"Item interactions: {item_interactions}")

if __name__ == "__main__":
    consume_data()
