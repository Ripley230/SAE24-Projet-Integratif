import paho.mqtt.client as mqtt
import time
from datetime import datetime

BROKER = "test.mosquitto.org"
PORT = 1883
TOPIC = "IUT/Colmar2025/SAE2.04/Maison1"  # ou Maison2 selon le besoin

client = mqtt.Client()
client.connect(BROKER, PORT)
client.loop_start()

try:
    while True:
        # Exemple de message (format attendu)
        message = f"Id=12A6B8AF6CD3,piece=sejour,date={datetime.now().strftime('%d/%m/%Y')},heure={datetime.now().strftime('%H:%M:%S')},temp={round(20 + 5 * (time.time() % 10) / 10, 2):.2f}".replace('.', ',')
        client.publish(TOPIC, message)
        print(f"Message envoyé sur {TOPIC}: {message}")
        time.sleep(5)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
