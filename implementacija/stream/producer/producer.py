import json
import websocket
import pandas as pd

assets = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']

assets = [coin.lower() + '@kline_1m' for coin in assets]

assets = '/'.join(assets)

def on_message(ws, message):
    message = json.loads(message)
    transform(message)

def transform(source):
    kdata = source['data']['k']
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
    
    df = pd.DataFrame([extracted_data])
    print(df.head().to_string())



socket = "wss://stream.binance.com:9443/stream?streams="+assets

ws = websocket.WebSocketApp(socket, on_message=on_message)
ws.run_forever()