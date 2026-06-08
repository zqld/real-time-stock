import time

from producer import producer
#from consumer import consumer
from kafka import KafkaProducer

import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
    )

ticker_list = ['AAPL', 'TSLA', 'MSFT', 'NVDA']
producer_instance = None
try:
    producer_instance = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
    while True:
        for ticker in ticker_list:
            producer(producer_instance, ticker)
        producer_instance.flush()
        time.sleep(10)
    # while not message_queue.empty():
    #     consumer(message_queue)
except Exception as e:
    logging.error(f"An error occurred: {e}")
except KeyboardInterrupt:
    logging.info("Producer stopped by user")
finally:
    if producer_instance:
        producer_instance.close()