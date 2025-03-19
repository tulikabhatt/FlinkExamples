import websocket, json, time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: v.encode('utf-8'))

def on_message(ws, message):
    producer.send('crypto-prices', message)
    print("Sent:", message)

ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws/btcusdt@trade", on_message=on_message)
ws.run_forever()