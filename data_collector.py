#*
# 1 Запрашивает данные: Скрипт должен делать HTTP-запрос к бесплатному API и получать текущие котировки акций. Рекомендация: Чтобы не мучиться с ключами API на первом получасе, используйте библиотеку yfinance (это обертка над Yahoo Finance) или сделайте запрос к любому открытому публичному API (например, котировки криптовалют от CoinGecko или Binance, они не требуют регистрации). Если хотите строго акции США, возьмите yfinance
# 2 Форматирует данные: Вытащите из ответа API: 
# название тикера (['AAPL', 'TSLA', 'MSFT', 'NVDA'])
# текущую цену, 
# валюту и 
# точное время (timestamp).
# 3 Сохраняет локально: Скрипт должен записать эти данные на ваш жесткий диск в обычный CSV-файл. Если файл уже существует, скрипт должен дописать (append) новую строку в конец файла, а не перезаписывать его.*#

import yfinance as yf
from datetime import datetime
from datetime import timezone


ticker_list = ['AAPL', 'TSLA', 'MSFT', 'NVDA']

with open('stock_data.csv', 'a') as file:
    for i in ticker_list:
        dat = yf.Ticker(i)
        info = dat.info
        timeStampPC = datetime.now(timezone.utc).isoformat()
        file.write(f"{i},{info['currentPrice']},{info['currency']},{timeStampPC}\n")