import json
import websocket
import os, time
import pandas as pd
from kafka import KafkaProducer 
import kafka.errors
from functools import partial


KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = 'btc-usdt'
key = 0

KAFKA_CONFIGURATION = {
    "bootstrap_servers": os.environ.get("KAFKA_BROKER", "kafka1:19092").split(","),
    "key_serializer": lambda x: str.encode("" if not x else x, encoding='utf-8'),
    "value_serializer": lambda x: json.dumps(dict() if not x else x).encode(encoding='utf-8'),
    "reconnect_backoff_ms": int(100)
}

def setup_socket():
    assets = ['BTCUSDT']

    assets = [coin.lower() + '@kline_1m' for coin in assets]

    assets = '/'.join(assets)

    socket = "wss://stream.binance.com:9443/stream?streams="+assets

    return socket

def on_message(ws, message, producer):
    print("Something...")
    print(message)

    producer.send(TOPIC, key=message['E'], value=message['k'])

while True:
    try:
        print(KAFKA_CONFIGURATION)
        producer = KafkaProducer(**KAFKA_CONFIGURATION)
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

socket = setup_socket()

print(socket)

ws = websocket.WebSocketApp(socket, on_message=partial(on_message, producer=producer))

ws.run_forever()