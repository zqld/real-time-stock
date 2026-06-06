import json
import logging
import csv
import time

logging.basicConfig(
    level=logging.INFO, 
    format='%(name)s - %(asctime)s - %(levelname)s - %(message)s'
    )

def consumer(queue):
    item_json = queue.get()
    item_json_load = json.loads(item_json)

    # ticker = item_json_load['ticker']
    # price = item_json_load['price']
    # currency = item_json_load['currency']
    # timestamp = item_json_load['timestamp']
    
    with open('stock_data.csv', 'a', encoding='utf-8', newline='') as file:
        writer_new = csv.DictWriter(file, fieldnames=['ticker', 'price', 'currency', 'timestamp'])
        if file.tell() == 0:
            writer_new.writeheader()
        writer_new.writerow(item_json_load)

        # file.write(f"{ticker},{price},{currency},{timestamp}\n")

    logging.info(f'Consumed {item_json}')
    queue.task_done()
    time.sleep(1)
