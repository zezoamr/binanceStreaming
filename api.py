from typing import List
import json
from sqlalchemy import null
import websocket
from confluent_kafka import Producer

class binance_source():
    
    currency = ['btcusdt', 'ethbtc'] #gonna limit it to only two coins, easier to debug
    interval = '3m'
    socket = f'wss://stream.binance.com:9443/stream?streams={currency[0]}@kline_{interval}/{currency[1]}@kline_{interval}'
    topic = 'binance'
    prod = Producer({"bootstrap.servers":"localhost:9092"})
    ws : websocket.WebSocketApp = None
    
    def on_message(self, ws, message):
        
        data = message
        
        data = json.loads(message)
        #print(data['data'], data['stream'], data)
        
        self.prod.produce('binance', message, data['stream'])
        self.prod.flush()
        

    def on_error(self, ws, error):
        print("error",error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")

    def on_open(self, ws):
        print("Opened connection")
    
    def __init__(self, topic : str = topic, currency : List[str] = currency):  
        self.currency = currency 
        self.topic = topic
        
    def run(self):
        self.ws = websocket.WebSocketApp(self.socket, on_open=self.on_open, on_message=self.on_message,
                                    on_error=self.on_error, on_close=self.on_close)
        self.ws.run_forever()
        
    def terminate(self):
        if self.ws is not None:
            self.ws.close()

#if __name__ == '__main__':
    #b = binance_source()
