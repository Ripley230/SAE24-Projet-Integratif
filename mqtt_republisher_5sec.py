import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime
import mysql.connector
import threading
import os

DB_CONFIG = {
    "host": "10.252.16.11",
    "user": "quentin",
    "password": "quentin",
    "database": "sae204",
    "auth_plugin": "mysql_native_password"
}
BROKER = "test.mosquitto.org"
PORT = 1883
TOPICS = [
    "IUT/Colmar2025/SAE2.04/Maison1",
    "IUT/Colmar2025/SAE2.04/Maison2"
]
BUFFER_FILE = "buffer_donnees.json"
last_data = {}

db_lock = threading.Lock()
db_offline = False  # Mode offline si la base est déconnectée

def connect_db():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error:
        return None

def buffer_data(data):
    """Ajoute une donnée au buffer fichier."""
    with db_lock:
        buffer = []
        if os.path.exists(BUFFER_FILE):
            with open(BUFFER_FILE, "r", encoding="utf-8") as f:
                try:
                    buffer = json.load(f)
                except Exception:
                    buffer = []
        buffer.append(data)
        with open(BUFFER_FILE, "w", encoding="utf-8") as f:
            json.dump(buffer, f, ensure_ascii=False, indent=2)
    print("Donnée stockée dans le buffer temporaire.")

def flush_buffer():
    """Essaye d'envoyer toutes les données du buffer à la base."""
    global db_offline
    with db_lock:
        if not os.path.exists(BUFFER_FILE):
            return
        with open(BUFFER_FILE, "r", encoding="utf-8") as f:
            try:
                buffer = json.load(f)
            except Exception:
                buffer = []
        if not buffer:
            return
        print(f"Tentative de réinjection de {len(buffer)} données du buffer...")
        success = []
        conn = connect_db()
        if not conn:
            print("Base toujours déconnectée, attente...")
            db_offline = True
            return
        cursor = conn.cursor()
        try:
            for data in buffer:
                try:
                    id_capteur = data.get("idCapteur") or data.get("Id") or data.get("idcapteur")
                    nom_capteur = data.get("nom_capteur") or id_capteur
                    piece = data.get("piece", "Unknown")
                    emp_cap = data.get("emp_cap", "Inconnu")
                    cursor.execute("SELECT idCapteur FROM capteur WHERE idCapteur = %s", (id_capteur,))
                    if not cursor.fetchone():
                        cursor.execute(
                            "INSERT INTO capteur (idCapteur, nom_capteur, piece, emp_cap) VALUES (%s, %s, %s, %s)",
                            (id_capteur, nom_capteur, piece, emp_cap)
                        )
                        print(f"Capteur ajouté : {id_capteur}")
                    temp_raw = data.get("temp", "0.0")
                    try:
                        temperature = float(str(temp_raw).replace(',', '.'))
                    except Exception:
                        temperature = 0.0
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    Capteur_idCapteur = data.get("capteur_idCapteur") or id_capteur
                    cursor.execute(
                        "INSERT INTO donnees (timestamp, temperature, Capteur_idCapteur) VALUES (%s, %s, %s)",
                        (timestamp, temperature, Capteur_idCapteur)
                    )
                    conn.commit()
                    print(f"Donnée insérée pour capteur {id_capteur} à {timestamp} : {temperature}°C")
                    success.append(data)
                except Exception as e:
                    print("Erreur insertion (buffer):", e)
            # Supprime du buffer les données bien insérées
            if len(success) == len(buffer):
                os.remove(BUFFER_FILE)
                print("Buffer vidé avec succès.")
                db_offline = False  # On repasse en mode online
            else:
                buffer_rest = [d for d in buffer if d not in success]
                with open(BUFFER_FILE, "w", encoding="utf-8") as f:
                    json.dump(buffer_rest, f, ensure_ascii=False, indent=2)
                print(f"{len(buffer_rest)} données restent dans le buffer.")
                db_offline = True  # On reste en offline
        finally:
            cursor.close()
            conn.close()

def insert_data(data):
    """Si offline, bufferise systématiquement. Sinon, tente l'insertion."""
    global db_offline
    if db_offline:
        buffer_data(data)
        return
    conn = connect_db()
    if not conn:
        print("Base de données déconnectée ! Passage en mode offline. Les données seront bufferisées.")
        db_offline = True
        buffer_data(data)
        return
    cursor = conn.cursor()
    try:
        id_capteur = data.get("idCapteur") or data.get("Id") or data.get("idcapteur")
        nom_capteur = data.get("nom_capteur") or id_capteur
        piece = data.get("piece", "Unknown")
        emp_cap = data.get("emp_cap", "Inconnu")
        cursor.execute("SELECT idCapteur FROM capteur WHERE idCapteur = %s", (id_capteur,))
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO capteur (idCapteur, nom_capteur, piece, emp_cap) VALUES (%s, %s, %s, %s)",
                (id_capteur, nom_capteur, piece, emp_cap)
            )
            print(f"Capteur ajouté : {id_capteur}")
        temp_raw = data.get("temp", "0.0")
        try:
            temperature = float(str(temp_raw).replace(',', '.'))
        except Exception:
            temperature = 0.0
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        Capteur_idCapteur = data.get("capteur_idCapteur") or id_capteur
        cursor.execute(
            "INSERT INTO donnees (timestamp, temperature, Capteur_idCapteur) VALUES (%s, %s, %s)",
            (timestamp, temperature, Capteur_idCapteur)
        )
        conn.commit()
        print(f"Donnée insérée pour capteur {id_capteur} à {timestamp} : {temperature}°C")
    except Exception as e:
        print("Erreur insertion:", e)
        db_offline = True
        buffer_data(data)
    finally:
        cursor.close()
        conn.close()

def on_connect(client, userdata, flags, rc, properties=None):
    print("Connecté au broker MQTT")
    for topic in TOPICS:
        client.subscribe(topic)
    # À chaque reconnexion, on tente de vider le buffer
    flush_buffer()

def on_disconnect(client, userdata, rc, *args, **kwargs):
    print("Déconnecté, tentative de reconnexion...")
    try:
        client.reconnect()
    except Exception as e:
        print("Erreur de reconnexion:", e)

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
            data['temp'] = str(data['temp']).replace(',', '.')
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

def buffer_flush_task():
    """Thread qui tente régulièrement de vider le buffer si la base est de nouveau accessible."""
    while True:
        flush_buffer()
        time.sleep(5)

client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

try:
    client.connect(BROKER, PORT)
    threading.Thread(target=publish_task, args=(client,), daemon=True).start()
    threading.Thread(target=buffer_flush_task, daemon=True).start()
    client.loop_forever()
except KeyboardInterrupt:
    client.disconnect()
    print("Déconnexion propre du client MQTT")
