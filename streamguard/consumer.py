"""
StreamGuard — Kafka Consumer
=============================
Lit les messages bruts depuis le topic Kafka source.
Première étape du pipeline StreamGuard.

Auteur  : Roui Anas — PFE EMI MIS 2026 — DSIG CDM

RÔLE DANS LE PIPELINE :

  Kafka topic brut
  cdm.sisdm.kondor.dbo.deals
          ↓
  [consumer.py]  ← ICI
          ↓
  pseudonymizer.py
          ↓
  Kafka topic sécurisé
  cdm.sisdm.kondor.dbo.deals.secure
"""

import json
import logging
import os
from typing import Optional, Callable
from confluent_kafka import Consumer, KafkaError, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STREAMGUARD:CONSUMER] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── CONFIGURATION ─────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
TOPIC_SOURCE = os.getenv(
    "TOPIC_SOURCE", "cdm.sisdm.kondor.dbo.deals"
)
CONSUMER_GROUP = os.getenv(
    "CONSUMER_GROUP", "streamguard-pseudonymizer"
)


# ── CONSUMER PRINCIPAL ────────────────────────────────────────

class StreamGuardConsumer:
    """
    Consomme les messages bruts depuis Kafka.

    Chaque message contient un deal financier CDM
    au format Debezium CDC :
    {
      "op"    : "c",
      "after" : { deal_id, montant, trader, ... },
      "source": { db, table, lsn, ts_ms }
    }

    Le consumer lit ces messages et les passe
    au pseudonymizer pour traitement.
    """

    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers"       : KAFKA_BOOTSTRAP_SERVERS,
            "group.id"                : CONSUMER_GROUP,
            "client.id"               : "streamguard-consumer",
            # Démarre depuis le début si nouveau groupe
            "auto.offset.reset"       : "earliest",
            # NE PAS commiter automatiquement
            # On commitera manuellement après
            # pseudonymisation réussie
            "enable.auto.commit"      : False,
            # Timeout de session
            "session.timeout.ms"      : 30000,
            "heartbeat.interval.ms"   : 10000,
        })

        self.consumer.subscribe([TOPIC_SOURCE])
        self._running = False

        logger.info(
            f"Consumer initialisé — "
            f"topic={TOPIC_SOURCE} "
            f"group={CONSUMER_GROUP}"
        )

    def poll_message(self, timeout: float = 1.0) -> Optional[dict]:
        """
        Lit UN message depuis Kafka.

        Args:
            timeout : secondes d'attente max

        Returns:
            dict avec le payload du message
            None si pas de message disponible
        """
        msg = self.consumer.poll(timeout=timeout)

        # Pas de message disponible
        if msg is None:
            return None

        # Erreur Kafka
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Fin de partition — pas une vraie erreur
                logger.debug(
                    f"Fin de partition — "
                    f"topic={msg.topic()} "
                    f"partition={msg.partition()}"
                )
                return None
            else:
                raise KafkaException(msg.error())

        # Message valide — déserialise
        try:
            raw_value = msg.value().decode("utf-8")
            payload   = json.loads(raw_value)

            logger.info(
                f"Message reçu — "
                f"topic={msg.topic()} "
                f"partition={msg.partition()} "
                f"offset={msg.offset()} "
                f"key={msg.key().decode('utf-8') if msg.key() else 'None'}"
            )

            # Retourne le message enrichi avec les métadonnées
            return {
                "payload"   : payload,
                "topic"     : msg.topic(),
                "partition" : msg.partition(),
                "offset"    : msg.offset(),
                "key"       : msg.key().decode("utf-8") if msg.key() else None,
                "_raw_msg"  : msg,
            }

        except json.JSONDecodeError as e:
            logger.error(f"JSON invalide — offset={msg.offset()} — {e}")
            self.commit(msg)
            return None

    def commit(self, raw_msg=None):
        """
        Confirme à Kafka que le message a été traité.

        Important :
        On ne commit QU'APRÈS pseudonymisation réussie.
        Si StreamGuard crash avant le commit →
        Kafka re-livrera le message au redémarrage.
        Zéro perte garantie.
        """
        if raw_msg:
            self.consumer.commit(message=raw_msg, asynchronous=False)
        else:
            self.consumer.commit(asynchronous=False)

    def run(self, on_message: Callable[[dict], bool]):
        """
        Boucle principale de consommation.

        Args:
            on_message : fonction appelée pour chaque message
                         doit retourner True si traité avec succès
                         False si échec (→ pas de commit)
        """
        self._running = True
        logger.info("Démarrage boucle de consommation...")

        try:
            while self._running:
                message = self.poll_message(timeout=1.0)

                if message is None:
                    continue

                # Traiter le message
                success = on_message(message)

                if success:
                    # Commit seulement si traitement réussi
                    self.commit(message["_raw_msg"])
                    logger.debug(
                        f"Commit offset={message['offset']} ✅"
                    )
                else:
                    logger.warning(
                        f"Traitement échoué — "
                        f"offset={message['offset']} "
                        f"pas de commit → sera relu"
                    )

        except KeyboardInterrupt:
            logger.info("Arrêt demandé")
        finally:
            self.close()

    def close(self):
        """Ferme proprement le consumer."""
        self._running = False
        self.consumer.close()
        logger.info("Consumer fermé proprement")

    def extract_deal(self, message: dict) -> Optional[dict]:
        """
        Extrait le deal depuis le message Debezium.

        Le message Debezium a cette structure :
        {
          "payload": {
            "op"    : "c",
            "after" : { deal_id, montant, ... },
            "source": { ... }
          }
        }

        On veut juste le contenu de "after".

        Args:
            message : message enrichi de poll_message()

        Returns:
            dict du deal ou None si DELETE
        """
        payload = message.get("payload", {})

        # Récupère le payload Debezium
        debezium = payload.get("payload", payload)
        op       = debezium.get("op", "c")

        # Ignore les DELETE
        if op == "d":
            logger.debug("Message DELETE ignoré")
            return None

        # Retourne le contenu "after" (état après modification)
        deal = debezium.get("after")
        if not deal:
            logger.warning("Message sans 'after' — ignoré")
            return None

        return deal


# ── TEST LOCAL ───────────────────────────────────────────────

if __name__ == "__main__":
    """
    Test : consomme 5 messages et affiche les deals.
    Lance d'abord kafka_producer.py dans un autre terminal.
    """
    consumer = StreamGuardConsumer()
    count = 0

    print("\n── En attente de messages Kafka... ──\n")

    try:
        while count < 5:
            message = consumer.poll_message(timeout=2.0)

            if message is None:
                print("Pas de message... en attente")
                continue

            deal = consumer.extract_deal(message)
            if deal:
                count += 1
                print(
                    f"Deal {count} reçu : "
                    f"{deal.get('deal_id')} | "
                    f"{deal.get('devise')} | "
                    f"taux={deal.get('taux')} | "
                    f"montant={deal.get('montant'):,}"
                )
                consumer.commit(message["_raw_msg"])

    finally:
        consumer.close()
        print(f"\nTotal reçus : {count}")
