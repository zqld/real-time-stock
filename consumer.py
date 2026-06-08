import json
import logging
import csv
import queue

logging.basicConfig(
    level=logging.INFO, 
    format='%(name)s - %(asctime)s - %(levelname)s - %(message)s'
    )

def consumer(queue: queue.Queue) -> None:
    try:
        item_json = queue.get()
        item_json_load = json.loads(item_json)
        
        with open('stock_data.csv', 'a', encoding='utf-8', newline='') as file:
            writer_new = csv.DictWriter(file, fieldnames=['ticker', 'price', 'currency', 'timestamp'])
            if file.tell() == 0:
                writer_new.writeheader()
            writer_new.writerow(item_json_load)
        
        queue.task_done()
        logging.info(f'Consumed {item_json}')
    except json.JSONDecodeError:
        logging.error(f'Invalid JSON format in message: {item_json}')
        queue.task_done()
    except csv.Error:
        logging.error(f'Error occurred while writing to CSV file: {item_json}')
        queue.put(item_json)
        return
    except Exception as e:
        logging.error(f'Error occurred while consuming message: {e}')
        queue.task_done()

    