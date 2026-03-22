"""
CDM Kafka Producer
==================
Publie les deals synthétiques dans Kafka
au format exact Debezium CDC SQL Server.

Auteur  : Roui Anas — PFE EMI MIS 2026 — DSIG CDM

RÔLE DANS LE PIPELINE :
  Ce producer simule ce que Debezium ferait
  en production CDM sur KONDOR+ MSSQL.

  generator.py  →  kafka_producer.py  →  Kafka Topic
  (crée deals)     (publie dans Kafka)    (stocke)

  En production CDM :
  KONDOR+ MSSQL → CDC → Debezium → Kafka Topic
  (même topic, même format, même pipeline après)
"""

import json
import time
import logging
import os
from typing import Optional
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)
from generator import CDMDealGenerator, to_debezium_message, DealCounter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── CONFIGURATION ─────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL", "http://localhost:8081"
)
TOPIC_DEALS = os.getenv(
    "TOPIC_DEALS", "cdm.sisdm.kondor.dbo.deals"
)
TOPIC_POSITIONS = os.getenv(
    "TOPIC_POSITIONS", "cdm.sisdm.kondor.dbo.positions"
)
DEALS_PER_MINUTE = int(os.getenv("DEALS_PER_MINUTE", "10"))
ANOMALY_RATE     = float(os.getenv("ANOMALY_RATE", "0.05"))


# ── CALLBACKS ─────────────────────────────────────────────────

def delivery_callback(err, msg):
    """
    Appelé par Kafka après chaque envoi.
    Confirme la livraison ou logue l'erreur.
    """
    if err:
        logger.error(
            f"❌ Échec livraison — "
            f"topic={msg.topic()} "
            f"erreur={err}"
        )
    else:
        logger.info(
            f"✅ Deal publié — "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"key={msg.key().decode('utf-8')}"
        )


# ── PRODUCER PRINCIPAL ────────────────────────────────────────

class CDMKafkaProducer:
    """
    Publie les deals synthétiques CDM dans Kafka
    au format exact Debezium CDC.

    Format du message :
    - Key   : deal_id (ex: "CDM-00001")
    - Value : message Debezium JSON complet
              {op, before, after, source, ts_ms}

    Ce format est identique à ce que Debezium
    produirait sur KONDOR+ MSSQL en production.
    NiFi ConsumeKafka reçoit exactement ce format.
    """

    def __init__(self):
        # Configuration du producer Kafka
        self.producer = Producer({
            "bootstrap.servers"       : KAFKA_BOOTSTRAP_SERVERS,
            "client.id"               : "cdm-synthetic-producer",
            # Attendre confirmation des 3 brokers (prod)
            # ou juste du leader (local)
            "acks"                    : "all",
            # Retry automatique si erreur réseau
            "retries"                 : 3,
            "retry.backoff.ms"        : 1000,
            # Compression pour réduire la bande passante
            "compression.type"        : "snappy",
        })

        self.generator = CDMDealGenerator(
            anomaly_rate=ANOMALY_RATE
        )

        logger.info(
            f"Producer initialisé — "
            f"broker={KAFKA_BOOTSTRAP_SERVERS} "
            f"topic={TOPIC_DEALS} "
            f"rate={DEALS_PER_MINUTE} deals/min "
            f"anomaly_rate={ANOMALY_RATE*100:.0f}%"
        )

    def publish_deal(self, deal=None) -> bool:
        """
        Génère et publie un deal dans Kafka.

        Args:
            deal : deal pré-généré (optionnel)
                   si None → génère automatiquement

        Returns:
            True si publié avec succès
        """
        try:
            # Générer le deal si pas fourni
            if deal is None:
                deal = self.generator.generate()

            # Encapsuler dans le format Debezium
            message = to_debezium_message(deal, operation="c")

            # Sérialiser en JSON
            key   = deal.deal_id.encode("utf-8")
            value = json.dumps(
                message,
                ensure_ascii=False,
                default=str,
            ).encode("utf-8")

            # Publier dans Kafka
            self.producer.produce(
                topic    = TOPIC_DEALS,
                key      = key,
                value    = value,
                callback = delivery_callback,
            )

            # Flush immédiat pour les tests
            # En production → flush par batch
            self.producer.poll(0)

            return True

        except Exception as e:
            logger.error(f"Erreur publication deal : {e}")
            return False

    def run_continuous(self):
        """
        Publie des deals en continu
        selon le rythme DEALS_PER_MINUTE.

        C'est le mode production :
        simule le flux continu de KONDOR+
        pendant les heures de marché.
        """
        interval = 60.0 / DEALS_PER_MINUTE
        logger.info(
            f"Démarrage flux continu — "
            f"intervalle={interval:.1f}s entre chaque deal"
        )

        deals_published  = 0
        deals_failed     = 0
        anomalies_sent   = 0

        try:
            while True:
                deal = self.generator.generate()
                success = self.publish_deal(deal)

                if success:
                    deals_published += 1
                    if deal.is_anomaly:
                        anomalies_sent += 1
                        logger.warning(
                            f"⚠️  Anomalie publiée — "
                            f"type={deal.anomaly_type} "
                            f"deal_id={deal.deal_id}"
                        )
                else:
                    deals_failed += 1

                # Stats toutes les 10 deals
                if deals_published % 10 == 0:
                    logger.info(
                        f"📊 Stats — "
                        f"publiés={deals_published} "
                        f"anomalies={anomalies_sent} "
                        f"échecs={deals_failed}"
                    )

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("Arrêt demandé par l'utilisateur")
        finally:
            # S'assurer que tous les messages
            # sont bien envoyés avant de quitter
            self.producer.flush(timeout=10)
            logger.info(
                f"Producer arrêté — "
                f"total publiés={deals_published} "
                f"anomalies={anomalies_sent}"
            )

    def run_batch(self, n: int = 100):
        """
        Publie un batch de N deals d'un coup.
        Utile pour initialiser des données de test.

        Args:
            n : nombre de deals à publier
        """
        logger.info(f"Envoi batch de {n} deals...")
        success = 0
        anomalies = 0

        for i in range(n):
            deal = self.generator.generate()
            if self.publish_deal(deal):
                success += 1
                if deal.is_anomaly:
                    anomalies += 1

        # Flush final — attend que tous les messages
        # soient confirmés par Kafka
        self.producer.flush(timeout=30)

        logger.info(
            f"Batch terminé — "
            f"succès={success}/{n} "
            f"anomalies={anomalies}"
        )
        return success


# ── POINT D'ENTRÉE ────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    producer = CDMKafkaProducer()

    if len(sys.argv) > 1 and sys.argv[1] == "batch":
        # Mode batch : publie 50 deals et quitte
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 50
        print(f"\nMode batch — publication de {n} deals\n")
        producer.run_batch(n)
    else:
        # Mode continu : publie indéfiniment
        print(f"\nMode continu — {DEALS_PER_MINUTE} deals/minute")
        print("Ctrl+C pour arrêter\n")
        producer.run_continuous()
