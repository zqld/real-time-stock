import yfinance as yf
import json
from datetime import datetime
from datetime import timezone
import time
import logging
import queue

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
    )


def producer(queue: queue.Queue, ticker: str) -> None:
    try:
        dat = yf.Ticker(ticker)
        price = dat.fast_info.get('lastPrice', 'N/A')
        currency = dat.fast_info.get('currency', 'USD')
        timestamp = datetime.now(timezone.utc).isoformat()

        item = {
            'ticker': ticker,
            'price': price,
            'currency': currency,
            'timestamp': timestamp
            }
        
        item_json = json.dumps(item)
        
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        item = {
            'ticker': ticker,
            'price': None,
            'currency': 'Error',
            'timestamp': 'Error'
        }
        item_json = json.dumps(item)

    queue.put(item_json)

    logging.info(f'Produced {item_json}')
    time.sleep(1)

