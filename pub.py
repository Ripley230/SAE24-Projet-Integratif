#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lanceur Automatique SAE2.04 - Setup Complet
Lance automatiquement le simulateur et le republisher

Auteur: Assistant IA
Version: 1.0
Date: 2025-06-23
"""

import subprocess
import sys
import time
import os
import signal
from pathlib import Path

class SAE204Launcher:
    def __init__(self):
        self.processes = []

    def check_dependencies(self):
        """Vérifie les dépendances Python"""
        print("🔍 Vérification des dépendances...")

        try:
            import paho.mqtt.client
            print("✅ paho-mqtt installé")
        except ImportError:
            print("❌ paho-mqtt manquant")
            print("📦 Installation automatique...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "paho-mqtt"])
                print("✅ paho-mqtt installé avec succès")
            except subprocess.CalledProcessError:
                print("❌ Échec d'installation de paho-mqtt")
                print("💡 Exécutez manuellement: pip install paho-mqtt")
                return False

        return True

    def check_files(self):
        """Vérifie la présence des fichiers nécessaires"""
        print("📁 Vérification des fichiers...")

        required_files = [
            "mqtt_republisher_5sec.py",
            "simulateur_capteurs.py"
        ]

        missing_files = []
        for file in required_files:
            if not Path(file).exists():
                missing_files.append(file)
            else:
                print(f"✅ {file}")

        if missing_files:
            print(f"❌ Fichiers manquants: {', '.join(missing_files)}")
            return False

        return True

    def start_simulator(self):
        """Démarre le simulateur en arrière-plan"""
        print("🔬 Démarrage du simulateur...")

        try:
            # Lancer le simulateur avec l'option automatique
            process = subprocess.Popen(
                [sys.executable, "simulateur_capteurs.py"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Envoyer automatiquement l'option 1 (simulation continue)
            process.stdin.write("1\n")
            process.stdin.flush()

            self.processes.append(("Simulateur", process))
            print("✅ Simulateur démarré")
            return True

        except Exception as e:
            print(f"❌ Erreur démarrage simulateur: {e}")
            return False

    def start_republisher(self):
        """Démarre le republisher"""
        print("🔄 Démarrage du republisher...")

        try:
            process = subprocess.Popen(
                [sys.executable, "mqtt_republisher_5sec.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            self.processes.append(("Republisher", process))
            print("✅ Republisher démarré")
            return True

        except Exception as e:
            print(f"❌ Erreur démarrage republisher: {e}")
            return False

    def monitor_processes(self):
        """Surveille les processus et affiche les logs"""
        print("\n📊 Surveillance des processus - Ctrl+C pour arrêter")
        print("="*60)

        try:
            while True:
                for name, process in self.processes:
                    if process.poll() is not None:
                        print(f"⚠️ {name} s'est arrêté")
                        return

                time.sleep(1)

        except KeyboardInterrupt:
            print("\n🛑 Arrêt demandé par l'utilisateur")

    def stop_all(self):
        """Arrête tous les processus"""
        print("🔄 Arrêt des processus...")

        for name, process in self.processes:
            if process.poll() is None:  # Processus encore actif
                print(f"🛑 Arrêt de {name}")
                process.terminate()

                # Attendre l'arrêt gracieux
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    print(f"⚡ Arrêt forcé de {name}")
                    process.kill()

        print("✅ Tous les processus arrêtés")

    def show_info(self):
        """Affiche les informations de connexion"""
        print("\n📱 Configuration IoT MQTT Panel:")
        print("-" * 40)
        print("🔗 Broker: test.mosquitto.org")
        print("🔌 Port: 1883")
        print("📡 Topic: IUT/Colmar2025/SAE2.04/JSON")
        print("🎯 JSONPath température: $.temp")
        print("🎯 JSONPath pièce: $.room")
        print("🎯 JSONPath maison: $.house")
        print()
        print("🔄 Fréquence de mise à jour: 5 secondes")
        print("✨ Format JSON optimisé pour gauges et widgets")

    def run(self):
        """Lance le système complet"""
        print("🚀 Lanceur Automatique SAE2.04")
        print("="*50)

        # Vérifications préliminaires
        if not self.check_dependencies():
            return False

        if not self.check_files():
            return False

        print("\n✅ Tous les prérequis sont satisfaits")

        try:
            # Démarrage des services
            print("\n🎬 Démarrage des services...")

            if not self.start_simulator():
                return False

            time.sleep(3)  # Attendre que le simulateur démarre

            if not self.start_republisher():
                return False

            time.sleep(2)  # Attendre que le republisher démarre

            # Afficher les informations de connexion
            self.show_info()

            # Surveiller les processus
            self.monitor_processes()

        except Exception as e:
            print(f"❌ Erreur: {e}")

        finally:
            self.stop_all()

def main():
    """Menu principal"""
    print("🎯 SAE2.04 - Système MQTT Auto")
    print("1. Lancement automatique complet")
    print("2. Vérification seule")
    print("3. Informations de configuration")
    print("4. Quitter")

    while True:
        try:
            choice = input("\nChoisissez une option (1-4): ").strip()

            if choice == "1":
                launcher = SAE204Launcher()
                launcher.run()
                break

            elif choice == "2":
                launcher = SAE204Launcher()
                print("\n🔍 Vérification des prérequis...")
                deps_ok = launcher.check_dependencies()
                files_ok = launcher.check_files()

                if deps_ok and files_ok:
                    print("\n✅ Système prêt à fonctionner!")
                else:
                    print("\n❌ Problèmes détectés - voir ci-dessus")
                break

            elif choice == "3":
                launcher = SAE204Launcher()
                launcher.show_info()
                print("\n📖 Consultez Guide_IoT_MQTT_Panel.md pour plus de détails")
                break

            elif choice == "4":
                print("👋 Au revoir !")
                break

            else:
                print("❌ Option invalide, essayez encore")

        except KeyboardInterrupt:
            print("\n👋 Au revoir !")
            break

if __name__ == "__main__":
    main()
