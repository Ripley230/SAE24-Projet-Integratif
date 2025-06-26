import paho.mqtt.client as mqtt
import mysql.connector
import json
import time
import os
from datetime import datetime

BDD_HOST = "10.252.16.11"
BDD_USER = "quentin"
BDD_PASSWORD = "quentin"
BDD_NOM = "sae204"
MQTT_SERVEUR = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_SUJETS = [
    "IUT/Colmar2025/SAE2.04/Maison1",
    "IUT/Colmar2025/SAE2.04/Maison2"
]

FICHIER_BUFFER = "buffer.json"
donnees_recues = {}

def connecter_bdd():
    return mysql.connector.connect(
        host=BDD_HOST,
        user=BDD_USER,
        password=BDD_PASSWORD,
        database=BDD_NOM
    )

def traiter_message(client, userdata, msg):
    print("Nouveau message !")
    donnees = json.loads(msg.payload.decode())
    id_capteur = donnees.get("idCapteur", "1")
    temperature = float(str(donnees.get("temp", "0")).replace(",", "."))
    piece = donnees.get("piece", "Unknown")

    conn = connecter_bdd()
    curseur = conn.cursor()
    curseur.execute("""
        INSERT IGNORE INTO capteur 
        (idCapteur, nom_capteur, piece, emp_cap) 
        VALUES (%s, %s, %s, %s)
    """, (id_capteur, id_capteur, piece, "Inconnu"))
    maintenant = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    curseur.execute("""
        INSERT INTO donnees 
        (timestamp, temperature, Capteur_idCapteur) 
        VALUES (%s, %s, %s)
    """, (maintenant, temperature, id_capteur))
    conn.commit()
    curseur.close()
    conn.close()
    donnees_recues[piece] = donnees

def quand_connecte(client, userdata, flags, rc):
    print("Connecté au serveur MQTT !")
    for sujet in MQTT_SUJETS:
        client.subscribe(sujet)

client_mqtt = mqtt.Client()
client_mqtt.on_connect = quand_connecte
client_mqtt.on_message = traiter_message
client_mqtt.connect(MQTT_SERVEUR, MQTT_PORT)
client_mqtt.loop_start()

while True:
    if os.path.exists(FICHIER_BUFFER):
        print("Traitement du buffer...")
        with open(FICHIER_BUFFER, "r") as f:
            lignes = f.readlines()
        os.remove(FICHIER_BUFFER)
        for ligne in lignes:
            donnees = json.loads(ligne)
            id_capteur = donnees.get("idCapteur", "1")
            temperature = float(str(donnees.get("temp", "0")).replace(",", "."))
            piece = donnees.get("piece", "Unknown")
            conn = connecter_bdd()
            curseur = conn.cursor()
            curseur.execute("""
                INSERT IGNORE INTO capteur 
                (idCapteur, nom_capteur, piece, emp_cap) 
                VALUES (%s, %s, %s, %s)
            """, (id_capteur, id_capteur, piece, "Inconnu"))
            maintenant = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            curseur.execute("""
                INSERT INTO donnees 
                (timestamp, temperature, Capteur_idCapteur) 
                VALUES (%s, %s, %s)
            """, (maintenant, temperature, id_capteur))
            conn.commit()
            curseur.close()
            conn.close()
    time.sleep(5)
