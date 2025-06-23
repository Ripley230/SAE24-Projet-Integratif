import paho.mqtt.client as mqtt
import time

BROKER = "test.mosquitto.org"
PORT = 1883
TOPICS = [
    "IUT/Colmar2025/SAE2.04/Maison1",
    "IUT/Colmar2025/SAE2.04/Maison2"
]

def on_connect(client, userdata, flags, rc):
    print("Connecté au broker MQTT")
    for topic in TOPICS:
        client.subscribe(topic)
        print(f"Abonné à {topic}")

def on_message(client, userdata, msg):
    print(f"Message reçu sur {msg.topic}: {msg.payload.decode('utf-8')}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
