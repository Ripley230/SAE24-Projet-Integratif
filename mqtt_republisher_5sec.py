import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import mysql.connector
import threading

DB_CONFIG = {
    "host": "10.252.16.11",
    "user": "quentin",
    "password": "quentin",
    "database": "sae204"
}
BROKER = "test.mosquitto.org"
PORT = 1883
TOPICS = ["IUT/Colmar2025/SAE2.04/Maison1", "IUT/Colmar2025/SAE2.04/Maison2"]
last_data = {}

def connect_db():
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as err:
        print(f"Erreur DB: {err}")
        return None

def insert_data(data):
    conn = connect_db()
    if not conn:
        print("Connexion DB échouée, abandon de l'insertion.")
        return
    cursor = conn.cursor()
    try:
        id_capteur = data.get("idCapteur") or data.get("Id") or data.get("idcapteur")
        nom_capteur = data.get("nom_capteur", "Inconnu")
        piece = data.get("piece", "Unknown")
        emp_cap = data.get("emp_cap", "Inconnu")
        cursor.execute("SELECT id_capteur FROM capteurs WHERE id_capteur = %s", (id_capteur,))
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO capteurs (id_capteur, nom_capteur, piece, emp_cap) VALUES (%s, %s, %s, %s)",
                (id_capteur, nom_capteur, piece, emp_cap)
            )
        conn.commit()
        temperature = float(data["temp"].replace(',', '.')) if "temp" in data else 0.0
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        Capteur_idCapteur = data.get("capteur_idCapteur") or id_capteur
        cursor.execute(
            "INSERT INTO donnees (idDonnees, timestamp, temperature, Capteur_idCapteur) VALUES (%s, %s, %s, %s)",
            (data.get("idDonnees"), timestamp, temperature, Capteur_idCapteur)
        )
        conn.commit()
    except Exception as e:
        print("Erreur insertion:", e)
    finally:
        cursor.close()
        conn.close()

def on_connect(client, userdata, flags, rc, properties=None):
    print("Connecté au broker MQTT")
    for topic in TOPICS:
        client.subscribe(topic)

def on_disconnect(client, userdata, rc, properties=None):
    print("Déconnecté, tentative de reconnexion...")
    client.reconnect()

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        print(f"Message reçu sur {msg.topic}: {payload}")
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            data = {}
            for item in payload.split(','):
                if '=' in item:
                    key, value = item.split('=', 1)
                    data[key.strip()] = value.strip()
        if 'temp' in data:
            data['temp'] = data['temp'].replace(',', '.')
        data['house'] = "Maison1" if "Maison1" in msg.topic else "Maison2"
        if 'piece' not in data:
            data['piece'] = "Unknown"
        insert_data(data)
        if data['house'] not in last_data:
            last_data[data['house']] = {}
        last_data[data['house']][data['piece']] = data
    except Exception as e:
        print("Erreur on_message:", e)

def publish_task(client):
    while True:
        try:
            if last_data:
                for house, pieces in last_data.items():
                    for piece, data in pieces.items():
                        topic_pub = f"IUT/Colmar2025/SAE2.04/{house}/{piece}"
                        json_data = {
                            "temp": float(data.get('temp', 0)),
                            "room": piece,
                            "house": house,
                            "id": data.get('Id', 'unknown'),
                            "timestamp": datetime.now().isoformat()
                        }
                        client.publish(topic_pub, json.dumps(json_data), qos=1)
                        print(f"Publié sur {topic_pub}: {json_data}")
            else:
                print("Aucune donnée à publier.")
            time.sleep(5)
        except Exception as e:
            print("Erreur dans le thread publication:", e)
            time.sleep(5)

client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

try:
    client.connect(BROKER, PORT)
    threading.Thread(target=publish_task, args=(client,), daemon=True).start()
    client.loop_forever()
except KeyboardInterrupt:
    client.disconnect()
    print("Déconnexion propre du client MQTT")
