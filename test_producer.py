from kafka import KafkaProducer
import json
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
    )

producer = KafkaProducer(bootstrap_servers='localhost:9092')
try:
    for i in range(5):
        message = {
            'i' : i,
            'message' : f'message {i}'
        }
        message_json = json.dumps(message).encode('utf-8')
        producer.send('test-topic', message_json)
        logging.info(f'Produced {message_json}')
    producer.flush()
except Exception as e:
    logging.error(f"An error occurred while sending messages: {e}")
finally:
    if producer:
        producer.close()

