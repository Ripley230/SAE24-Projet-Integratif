import json
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import mariadb

# ─── 1) CONFIGURATION BASE DE DONNÉES ────────────────────────────────────────────
DB_CONFIG = {
    'host': '10.252.15.45',
    'port': 3306,
    'user': 'toto',
    'password': 'toto',
    'database': 'sae204',  # Adapté au nom de votre base
}

BUFFER_FILE = "buffer.json"


# ─── 2) FONCTIONS DE GESTION DU BUFFER ──────────────────────────────────────────

def save_to_buffer(data):
    """
    Sauvegarde les données dans un fichier buffer JSON en cas d'indisponibilité de la base
    """
    try:
        with open(BUFFER_FILE, "r") as f:
            buffer = json.load(f)
    except FileNotFoundError:
        buffer = []

    buffer.append(data)

    with open(BUFFER_FILE, "w") as f:
        json.dump(buffer, f, indent=2)

    print(f"📥 Donnée mise en tampon: {data}")


def flush_buffer():
    """
    Tente d'envoyer toutes les données en tampon vers la base de données
    """
    try:
        with open(BUFFER_FILE, "r") as f:
            buffer = json.load(f)
    except FileNotFoundError:
        return  # Rien à flush

    if not buffer:
        return  # Buffer vide

    print(f"🔄 Tentative d'envoi de {len(buffer)} données tamponnées vers la base...")

    remaining = []
    for data in buffer:
        try:
            insert_data_raw(
                data["id"],
                datetime.fromisoformat(data["dt"]),
                data["temp"],
                data.get("piece", ""),
                data.get("emp_cap", "")  # Adapté au nom de colonne réel
            )
        except Exception as e:
            print(f"❌ Échec d'envoi d'une donnée tamponnée: {e}")
            remaining.append(data)

    if remaining:
        with open(BUFFER_FILE, "w") as f:
            json.dump(remaining, f, indent=2)
        print(f"⚠️ {len(remaining)} données restent en tampon.")
    else:
        # Vide le fichier buffer si tout est envoyé
        open(BUFFER_FILE, "w").close()
        print("✔️ Toutes les données tamponnées ont été envoyées avec succès.")


# ─── 3) FONCTION D'INSERTION ADAPTÉE À VOTRE BASE ──────────────────────────────
def insert_data_raw(id_capteur, dt: datetime, temp: float, piece: str = "", emp_cap: str = ""):
    """
    Insère les données dans les tables 'capteur' et 'donnees' selon votre structure DB

    Structure adaptée :
    - Table capteur: idCapteur (PK), nom_capteur (UNIQUE), piece, emp_cap
    - Table donnees: idDonnees (PK AUTO_INCREMENT), timestamp, temperature, Capteur_idCapteur (FK)
    """
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 1) Insertion/mise à jour du capteur
        # Note: ON DUPLICATE KEY UPDATE fonctionne sur la clé primaire (idCapteur)
        cursor.execute("""
            INSERT INTO capteur (idCapteur, nom_capteur, piece, emp_cap)
            VALUES (?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
              nom_capteur=VALUES(nom_capteur),
              piece=VALUES(piece),
              emp_cap=VALUES(emp_cap)
        """, (id_capteur, f"capteur_{id_capteur}", piece, emp_cap))

        # 2) Insertion des données de température
        # idDonnees est AUTO_INCREMENT donc pas besoin de le spécifier
        cursor.execute("""
            INSERT INTO donnees (timestamp, temperature, Capteur_idCapteur)
            VALUES (?, ?, ?)
        """, (dt.strftime("%Y-%m-%d %H:%M:%S"), temp, id_capteur))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Données insérées: Capteur {id_capteur} -> Temp: {temp}°C à {dt}")

    except mariadb.Error as e:
        print("\n❌ ⚠️ Base de données INACCESSIBLE. La donnée va être mise en tampon.\n")
        print(f"🔧 Erreur MariaDB : {e}\n")
        save_to_buffer({
            "id": id_capteur,
            "dt": dt.isoformat(),
            "temp": temp,
            "piece": piece,
            "emp_cap": emp_cap  # Adapté au nom de colonne réel
        })
        print("⚠️ Donnée mise en tampon suite à une erreur de base de données.")


# ─── 4) CALLBACKS MQTT ──────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    """
    Callback appelé lors de la connexion au broker MQTT
    """
    if rc == 0:
        print("✅ Connecté au broker MQTT (rc=0)")
        client.subscribe("IUT/Colmar2025/SAE2.04/#")
        # Tenter d'envoyer les données tamponnées à chaque reconnexion MQTT
        try:
            flush_buffer()
        except Exception as e:
            print(f"⚠️ Problème lors de l'envoi du buffer après reconnexion MQTT : {e}")
    else:
        print(f"❌ Erreur de connexion MQTT, code {rc}")


def on_disconnect(client, userdata, rc):
    """
    Callback appelé lors de la déconnexion du broker MQTT
    """
    if rc != 0:
        print(f"⚠️ Déconnecté du broker MQTT (code de retour {rc}). Tentative de reconnexion automatique...")
    else:
        print("✅ Déconnecté proprement du broker MQTT.")


def on_message(client, userdata, msg):
    """
    Callback appelé à chaque réception de message MQTT
    Traite les données JSON ou format clé=valeur
    """
    raw = msg.payload.decode()
    print(f"📨 Message reçu: {raw}")

    data = None

    # 1) Essai de parsing JSON
    try:
        data = json.loads(raw)
        print("✅ Format JSON détecté")
    except json.JSONDecodeError:
        # 2) Fallback format clé=valeur (ex: id=123,temp=25.5,date=2025-06-24)
        try:
            items = dict(x.split("=", 1) for x in raw.split(","))
            data = {k.lower(): v for k, v in items.items()}
            if "time" in data:
                data["heure"] = data.pop("time")
            print("✅ Format clé=valeur détecté")
        except Exception as e:
            print(f"❌ Payload mal formé, abandon du traitement: {e}")
            return

    if data is None:
        print("❌ Pas de données à traiter.")
        return

    # ─── NORMALISATION DES DONNÉES ───
    if isinstance(data, dict):
        # Gestion timestamp ISO
        ts = data.pop("timestamp", None)
        if ts:
            try:
                data["dt_obj"] = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
            except Exception as e:
                print(f"❌ Timestamp invalide: {e}")
                return

        # Normalisation des noms de champs
        if "room" in data:
            data["piece"] = data.pop("room")
        if "house" in data:
            data["emp_cap"] = data.pop("house")  # Adapté au nom de colonne
        if "emplacement" in data:
            data["emp_cap"] = data.pop("emplacement")

    # Récupération des métadonnées depuis les champs ou le topic
    piece = data.get("piece", "") if isinstance(data, dict) else ""
    emp_cap = data.get("emp_cap", "") if isinstance(data, dict) else ""

    # Déduction de l'emplacement à partir du topic MQTT
    topic = msg.topic.lower()
    if "maison1" in topic and not emp_cap:
        emp_cap = "maison1"
    elif "maison2" in topic and not emp_cap:
        emp_cap = "maison2"

    # ─── CONSTRUCTION DE LA DATETIME ───
    if "dt_obj" in data:
        dt = data["dt_obj"]
    else:
        date_str = data.get("date", "") if isinstance(data, dict) else ""
        heure_str = data.get("heure", "") if isinstance(data, dict) else ""

        if not date_str or not heure_str:
            print("❌ Il manque 'date' ou 'heure' dans les données.")
            return

        # Support de différents formats de date
        fmt = "%d/%m/%Y %H:%M:%S" if "/" in date_str else "%Y-%m-%d %H:%M:%S"
        try:
            dt = datetime.strptime(f"{date_str} {heure_str}", fmt).replace(tzinfo=timezone.utc)
        except ValueError as e:
            print(f"❌ Erreur de parsing date/heure: {e}")
            return

    # ─── TRAITEMENT DE LA TEMPÉRATURE ───
    raw_temp = str(data.get("temp", "0")).replace(",", ".") if isinstance(data, dict) else "0"
    try:
        temp_val = float(raw_temp)
        # Validation basique de la température (optionnel)
        if temp_val < -50 or temp_val > 100:
            print(f"⚠️ Température suspecte: {temp_val}°C")
    except ValueError:
        print(f"❌ Impossible de convertir temp='{raw_temp}' en nombre")
        return

    # ─── RÉCUPÉRATION ID CAPTEUR ───
    cap_id = data.get("id") if isinstance(data, dict) else None
    if not cap_id:
        print("❌ Pas d'ID de capteur dans les données.")
        return

    try:
        cap_id = int(cap_id)  # S'assurer que l'ID est un entier
    except ValueError:
        print(f"❌ ID capteur invalide: {cap_id}")
        return

    # ─── ENREGISTREMENT EN BASE ───
    try:
        insert_data_raw(cap_id, dt, temp_val, piece, emp_cap)
        print(f"✔️ Donnée stockée: capteur={cap_id}, dt={dt}, temp={temp_val}°C, piece={piece}, emp_cap={emp_cap}")
    except Exception as e:
        print(f"⚠️ Erreur lors de l'insertion: {e}")


# ─── 5) BOUCLE PRINCIPALE ────────────────────────────────────────────────────────
def main():
    """
    Point d'entrée principal - Configure et lance le client MQTT
    """
    print("🚀 Démarrage du listener MQTT pour capteurs IoT")

    # Configuration du client MQTT
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # Paramètres de connexion MQTT
    host, port, keepalive = "test.mosquitto.org", 1883, 60

    # Tentatives de connexion avec retry
    print(f"🔌 Tentative de connexion à {host}:{port}")
    for i in range(5):
        try:
            client.connect(host, port, keepalive)
            print("✅ Connexion MQTT établie")
            break
        except Exception as e:
            print(f"❌ Connexion MQTT échouée ({i + 1}/5): {e}")
            time.sleep(2)
    else:
        print("❌ Impossible de joindre le broker MQTT après 5 tentatives, sortie.")
        return

    # Boucle d'écoute des messages
    try:
        print("👂 En écoute des messages MQTT...")
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n🛑 Arrêt du listener demandé par l'utilisateur.")
        client.disconnect()


if __name__ == "__main__":
    main()
