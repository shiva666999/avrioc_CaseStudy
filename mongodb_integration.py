from pymongo import MongoClient
import json
from kafka import KafkaConsumer

# Initialize MongoDB client and database
client = MongoClient('mongodb://localhost:27017/')
db = client['user_data']
collection = db['aggregated_data']

# Function to store aggregated data into MongoDB
def store_aggregated_data(data):
    collection.update_one(
        {'user_id': data['user_id']},
        {'$set': data},
        upsert=True
    )

# Kafka consumer to fetch data and store aggregation results
def consume_and_store():
    consumer = KafkaConsumer(
        'user_interactions',
        bootstrap_servers='localhost:9092',
        group_id='consumer_group'
    )

    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        
        # Perform aggregation (simplified for illustration)
        aggregated_data = {
            'user_id': data['user_id'],
            'interaction_count': 1  # Simplified; real aggregation logic can be more complex
        }
        
        store_aggregated_data(aggregated_data)

if __name__ == "__main__":
    consume_and_store()
