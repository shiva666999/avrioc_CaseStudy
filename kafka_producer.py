from kafka import KafkaProducer
import json

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

def send_message_to_kafka(topic, data):
    producer.send(topic, value=json.dumps(data).encode('utf-8'))
    print(f"Data sent to Kafka: {data}")

if __name__ == "__main__":
    # Topic and data to be sent
    topic = 'user_interactions'
    data = {
        'user_id': 123,
        'item_id': 456,
        'interaction_type': 'click',
        'timestamp': '2025-02-28T12:00:00Z'
    }
    send_message_to_kafka(topic, data)
