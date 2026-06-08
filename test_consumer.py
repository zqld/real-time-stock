from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
    )

consumer = KafkaConsumer(
    'stock_topic', 
    bootstrap_servers='127.0.0.1:9092', 
    auto_offset_reset='earliest',
    group_id='test-group'
    )
try:
    for message in consumer:
        message_json = json.loads(message.value.decode('utf-8'))
        logging.info(f'Consumed {message_json}')
except KeyboardInterrupt:
    logging.info("Consumer stopped by user")
except Exception as e:
    logging.error(f"An error occurred while consuming messages: {e}")
finally:
    if consumer:
        consumer.close()