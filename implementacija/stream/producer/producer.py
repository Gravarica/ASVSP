import json
import websocket
import os, time
import pandas as pd
from kafka import KafkaProducer 
import kafka.errors

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = 'btc-usdt'
key = 0

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

def setup_socket():
    assets = ['BTCUSDT']

    assets = [coin.lower() + '@kline_1m' for coin in assets]

    assets = '/'.join(assets)

    socket = "wss://stream.binance.com:9443/stream?streams="+assets

    return socket

def on_message(ws, message):
    message = json.loads(message)
    key, data = transform(message)
    producer.send(TOPIC, key=bytes(key, 'utf-8'), value=bytes(data, 'utf-8'))

def transform(source):
    kdata = source['data']['k']
    key = key + 1
    extracted_data = { 
        'pair' : source['data']['s'],
        'open_time' : pd.to_datetime(kdata['t'], unit='ms'),
        'open' : kdata['o'],
        'close' : kdata['c'],
        'high' : kdata['h'],
        'low' : kdata['l'],
        'volume' : kdata['v'],
        'number_of_trades' : kdata['n'],
        'quote_asset_volume' : kdata['q'],
        'taker_buy_base_asset_volume' : kdata['V'],
        'taker_buy_quote_asset_volume' : kdata['Q']
    }
    
    return key, extracted_data

ws = websocket.WebSocketApp(setup_socket(), on_message=on_message)

ws.run_forever()