import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime

BROKER = "test.mosquitto.org"
PORT = 1883
TOPICS = [
    "IUT/Colmar2025/SAE2.04/Maison1",
    "IUT/Colmar2025/SAE2.04/Maison2"
]

# Stockage des données par maison puis par pièce
last_data = {}

def on_connect(client, userdata, flags, rc):
    print("Connecté au broker MQTT")
    for topic in TOPICS:
        client.subscribe(topic)

def on_message(client, userdata, msg):
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

    # Stockage des données par maison et pièce
    if data['house'] not in last_data:
        last_data[data['house']] = {}
    last_data[data['house']][data['piece']] = data

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_start()

try:
    while True:
        if last_data:
            # Publication toutes les 5 secondes sur le topic adapté à chaque pièce/maison
            for house, pieces in last_data.items():
                for piece, data in pieces.items():
                    topic_pub = f"IUT/Colmar2025/SAE2.04/{house}/{piece}"
                    # Format JSON plat pour IoT MQTT Panel
                    json_data = {
                        "temp": data.get('temp', 0),
                        "room": piece,
                        "house": house,
                        "id": data.get('Id', 'unknown'),
                        "timestamp": datetime.now().isoformat()
                    }
                    client.publish(topic_pub, json.dumps(json_data), qos=1)
                    print(f"Publié sur {topic_pub}: {json_data}")
        time.sleep(5)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
