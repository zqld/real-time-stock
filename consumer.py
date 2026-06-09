import json
import logging
import csv
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO, 
    format='%(name)s - %(asctime)s - %(levelname)s - %(message)s'
    )

consumer = KafkaConsumer(
    'stock_topic', 
    bootstrap_servers='127.0.0.1:9092', 
    auto_offset_reset='earliest',
    group_id='stock_csv_consumer'
    )

try:
    with open('stock_data.csv', 'a', encoding='utf-8', newline='') as file:
        writer_new = csv.DictWriter(file, fieldnames=['ticker', 'price', 'currency', 'timestamp'])
        if file.tell() == 0:
            writer_new.writeheader()

        for message in consumer:
            try:
                message_json = json.loads(message.value.decode('utf-8'))
                writer_new.writerow(message_json)
                file.flush()
                logging.info(f'Consumed {message_json}')
            except json.JSONDecodeError:
                logging.error(f'Invalid JSON format in message: {message}')
                continue

except PermissionError:
    logging.error("PermissionError")
except csv.Error as e:
    logging.critical(f"CSV write error: {e}. Stopping consumer.")
    raise
except KeyboardInterrupt:
    logging.info("Consumer stopped by user")
except Exception as e:
    logging.error(f"An error occurred while consuming messages: {e}")
finally:
    if consumer: 
        consumer.close()