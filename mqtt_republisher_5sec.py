import paho.mqtt.client as mqtt
import re
import json
import time
from datetime import datetime

# Configuration MQTT
BROKER = "test.mosquitto.org"
PORT = 1883
TOPICS = [
    "IUT/Colmar2025/SAE2.04/Maison1",
    "IUT/Colmar2025/SAE2.04/Maison2"
]
TOPIC_PUB = "IUT/Colmar2025/SAE2.04/JSON"

# Stockage des dernières données reçues
last_data = {}

def on_connect(client, userdata, flags, rc):
    print("Connecté au broker MQTT")
    for topic in TOPICS:
        client.subscribe(topic)

def on_message(client, userdata, msg):
    # Exemple de message reçu : "Id=12A6B8AF6CD3,piece=sejour,date=15/06/2022,heure=12:13:14,temp=26,35"
    payload = msg.payload.decode('utf-8')
    print(f"Message reçu sur {msg.topic}: {payload}")

    # Parsing du message CSV
    data = {}
    for item in payload.split(','):
        key, value = item.split('=', 1)
        data[key.strip()] = value.strip()

    # Conversion de la température (remplace la virgule par un point)
    if 'temp' in data:
        data['temp'] = float(data['temp'].replace(',', '.'))
    data['house'] = "Maison1" if "Maison1" in msg.topic else "Maison2"

    # Stockage des données par maison
    last_data[data['house']] = data

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_start()

try:
    while True:
        if last_data:
            # Publication toutes les 5 secondes
            for house, data in last_data.items():
                # Format JSON plat pour IoT MQTT Panel
                json_data = {
                    "temp": data.get('temp', 0),
                    "room": data.get('piece', 'unknown'),
                    "house": house,
                    "id": data.get('Id', 'unknown'),
                    "timestamp": datetime.now().isoformat()
                }
                client.publish(TOPIC_PUB, json.dumps(json_data), qos=1)
                print(f"Publié sur {TOPIC_PUB}: {json_data}")
        time.sleep(5)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
