#*
# Задача №2 Требование Переделай проект так, чтобы данные НЕ писались в CSV напрямую. Вместо этого сделай две программы. producer.py Получает котировки из Yahoo Finance. Формирует сообщение: { "ticker": "AAPL", "price": 212.5, "currency": "USD", "timestamp": "2025-08-13T15:30:00Z" } Пока НЕ используй Kafka. Просто складывай сообщения в список. Например: messages = [] messages.append(...) consumer.py Берет сообщения из этого списка и пишет их в CSV. Зачем это нужно Ты сейчас вручную разделишь систему на: Producer ↓ Queue ↓ Consumer Это фундамент Kafka.
#  Ограничение Представь, что producer работает отдельно: python producer.py а consumer отдельно: python consumer.py И тут ты обнаружишь проблему. 
# Список в памяти одного процесса недоступен другому процессу.
# То есть: messages = [] не может быть общей очередью. Вот тогда появится Kafka И ты поймешь: Мне нужен внешний брокер сообщений. Тогда мы поставим: Docker Kafka Zookeeper и заменим: messages.append() на producer.send() 
# Что нужно изучить для Задачи №2 Гуглить: 
# producer consumer 
# pattern queue data structure 
# python separation of concerns 
# python json module 
#  У тебя должно появиться: project/ │ ├── producer.py ├── consumer.py ├── stock_data.csv producer.py выдает сообщения такого вида: { "ticker": "AAPL", "price": 212.5, "currency": "USD", "timestamp": "..." } consumer.py умеет принимать такие сообщения и писать их в CSV. Пока можно запускать их в одном файле для демонстрации: messages = producer() consumer(messages) Но producer и consumer должны быть разными сущностями.
# *#
from producer import producer
from consumer import consumer
import queue

ticker_list = ['AAPL', 'TSLA', 'MSFT', 'NVDA']

message_queue = queue.Queue()


for ticker in ticker_list:
    producer(message_queue, ticker)
while not message_queue.empty():
    consumer(message_queue)
message_queue.join()