import json
import websocket
import os, time
from kafka import KafkaProducer
import kafka.errors
from functools import partial
from datetime import datetime

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = 'btc-usdt'

KAFKA_CONFIGURATION = {
    "bootstrap_servers": os.environ.get("KAFKA_BROKER", "kafka1:19092").split(","),
    "key_serializer": lambda x: str.encode("" if not x else x, encoding='utf-8'),
    "value_serializer": lambda x: x.encode(encoding='utf-8') if isinstance(x, str) else x,
    "reconnect_backoff_ms": int(100)
}

def setup_socket():
    assets = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
    assets = [coin.lower() + '@kline_1m' for coin in assets]
    socket = "wss://stream.binance.com:9443/stream?streams=" + '/'.join(assets)
    print(socket)
    return socket

def on_send_success(record_metadata):
    print("Message sent successfully", record_metadata.topic)

def on_send_error(excp):
    print("Error sending message", excp)

def determine_topic(symbol):
    topic_mappings = {
        "BTCUSDT": "btc-usdt",
        "ETHUSDT": "eth-usdt",
        "BNBUSDT": "bnb-usdt"
    }
    return topic_mappings.get(symbol, "btc-usdt")

def on_message(ws, message, producer):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #print(f"Message received at {current_time}")
    
    try:
        parsed_message = json.loads(message)
        data = parsed_message['data']['k']
        symbol = parsed_message['data']['s']
        #print(symbol)
        topic = determine_topic(symbol)

        formatted_message = json.dumps({
            "open_time": data['t'],
            "open": float(data['o']),
            "high": float(data['h']),
            "close": float(data['c']),
            "low": float(data['l']),
            "volume": float(data['v']),
            "num_trades": data['n'],
            "quote_asset_volume": float(data['q']),
            "taker_base_asset_volume": float(data['V']),
            "taker_quote_asset_volume": float(data['Q'])
        })

        kafka_key = str(parsed_message['data']['E'])
        

        producer.send(topic, key=kafka_key, value=formatted_message).add_callback(on_send_success).add_errback(on_send_error)
    except json.JSONDecodeError as json_err:
        print("JSON parsing error:", json_err)
    except kafka.errors.KafkaError as kafka_err:
        print("Kafka sending error: ", kafka_err)
    except Exception as e:
        print("Some other error: ", e)

while True:
    try:
        producer = KafkaProducer(**KAFKA_CONFIGURATION)
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

socket = setup_socket()
ws = websocket.WebSocketApp(socket, on_message=partial(on_message, producer=producer),
                            on_error=lambda ws, msg: print("WebSocket Error:", msg),
                            on_close=lambda ws: print("WebSocket Closed"))
ws.run_forever()
