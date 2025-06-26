import paho.mqtt.client as mqtt
import mysql.connector
import json
import time
import os
from datetime import datetime

def connecter_bdd():
    return mysql.connector.connect(
        host="10.252.16.11",
        user="quentin",
        password="quentin",
        database="sae204"
    )

def traiter_message(client, userdata, msg):
    print("Nouveau message !")
    donnees = json.loads(msg.payload.decode())
    id_capteur = donnees.get("idCapteur")
    temperature = float(str(donnees.get("temp")).replace(",", "."))
    piece = donnees.get("piece")

    conn = connecter_bdd()
    curseur = conn.cursor()
    curseur.execute("""INSERT IGNORE INTO capteur (idCapteur, nom_capteur, piece, emp_cap) VALUES (%s, %s, %s, %s)""",
                   (id_capteur, id_capteur, piece, None))
    date_pc = datetime.now()
    curseur.execute("""INSERT INTO donnees (timestamp, temperature, Capteur_idCapteur) VALUES (%s, %s, %s)""",
                   (date_pc, temperature, id_capteur))
    conn.commit()
    curseur.close()
    conn.close()
    donnees_recues[piece] = donnees

def quand_connecte(client, userdata, flags, rc):
    print("Connecté au serveur MQTT !")
    for sujet in ["IUT/Colmar2025/SAE2.04/Maison1", "IUT/Colmar2025/SAE2.04/Maison2"]:
        client.subscribe(sujet)

# Correction: Callback API VERSION2
client_mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client_mqtt.on_connect = quand_connecte
client_mqtt.on_message = traiter_message
client_mqtt.connect("test.mosquitto.org", 1883)
client_mqtt.loop_start()

donnees_recues = {}

while True:
    if os.path.exists("buffer.json"):
        print("Traitement du buffer...")
        with open("buffer.json", "r") as f:
            lignes = f.readlines()
        os.remove("buffer.json")
        for ligne in lignes:
            donnees = json.loads(ligne)
            id_capteur = donnees.get("idCapteur")
            temperature = float(str(donnees.get("temp")).replace(",", "."))
            piece = donnees.get("piece")
            conn = connecter_bdd()
            curseur = conn.cursor()
            curseur.execute("""INSERT IGNORE INTO capteur (idCapteur, nom_capteur, piece, emp_cap) VALUES (%s, %s, %s, %s)""",
                           (id_capteur, id_capteur, piece, None))
            date_pc = datetime.now()
            curseur.execute("""INSERT INTO donnees (timestamp, temperature, Capteur_idCapteur) VALUES (%s, %s, %s)""",
                           (date_pc, temperature, id_capteur))
            conn.commit()
            curseur.close()
            conn.close()
    time.sleep(5)
