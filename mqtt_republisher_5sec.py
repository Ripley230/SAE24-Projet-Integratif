import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import mysql.connector

def connect_db():
    return mysql.connector.connect(
        host="10.252.16.11",
        user="quentin",
        password="quentin",
        database="sae204"
    )

last_data = {}

def insert_data(data):
    conn = connect_db()
    cursor = conn.cursor()
    try:
        # Récupération cohérente de l'ID capteur
        id_capteur = data.get("idCapteur") or data.get("Id") or data.get("idcapteur")
        # Récupération du nom du capteur, de la pièce, etc.
        nom_capteur = data.get("nom_capteur")
        piece = data.get("piece")
        emp_cap = data.get("emp_cap")

        # Vérification de l'existence du capteur dans la table capteurs
        cursor.execute("SELECT id_capteur FROM capteurs WHERE id_capteur = %s", (id_capteur,))
        if not cursor.fetchone():
            # Insertion du capteur s'il n'existe pas
            cursor.execute(
                "INSERT INTO capteurs (id_capteur, nom_capteur, piece, emp_cap) VALUES (%s, %s, %s, %s)",
                (id_capteur, nom_capteur, piece, emp_cap)
            )
            conn.commit()

        idDonnees = data.get("idDonnees")
        # Conversion et nettoyage de la température
        temperature = float(data["temp"].replace(',', '.'))
        # Génération du timestamp actuel
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        Capteur_idCapteur = data.get("capteur_idCapteur")

        # Insertion de la donnée dans la table donnees
        cursor.execute(
            "INSERT INTO donnees (idDonnees, timestamp, temperature, Capteur_idCapteur) VALUES (%s, %s, %s, %s)",
            (idDonnees, timestamp, temperature, Capteur_idCapteur)
        )
        conn.commit()
    except Exception as e:
        print("Erreur insertion:", e)
    finally:
        cursor.close()
        conn.close()

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

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    print(f"Message reçu sur {msg.topic}: {payload}")

    data = {}
    for item in payload.split(','):
        key, value = item.split('=', 1)
        data[key.strip()] = value.strip()

    # Nettoyage de la température
    if 'temp' in data:
        data['temp'] = data['temp'].replace(',', '.')

    # Identification de la maison
    data['house'] = "Maison1" if "Maison1" in msg.topic else "Maison2"
    # Ajout de la pièce si nécessaire
    if 'piece' not in data:
        data['piece'] = "Unknown"

    # INSERTION DANS LA BASE DE DONNÉES
    insert_data(data)

    # Mise à jour du cache
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
        time.sleep(5)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
