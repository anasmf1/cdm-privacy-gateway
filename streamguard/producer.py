"""
StreamGuard — Secured Kafka Producer
=====================================
Publie les messages pseudonymisés dans le topic sécurisé.
Troisième étape du pipeline StreamGuard.

Auteur  : Roui Anas — PFE EMI MIS 2026 — DSIG CDM

RÔLE DANS LE PIPELINE :

  consumer.py → pseudonymizer.py → [producer.py]

  Reçoit un deal pseudonymisé :
  { deal_id: "DEAL_a3f9", trader: "TRADER_b2e1", ... }

  Publie dans le topic sécurisé :
  cdm.sisdm.kondor.dbo.deals.secure

  NiFi consomme ce topic sécurisé.
  Iceberg ne stocke jamais les vraies valeurs.
"""

import json
import logging
import os
from typing import Optional
from confluent_kafka import Producer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STREAMGUARD:PRODUCER] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── CONFIGURATION ─────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
TOPIC_SECURE = os.getenv(
    "TOPIC_SECURE", "cdm.sisdm.kondor.dbo.deals.secure"
)
TOPIC_AUDIT = os.getenv(
    "TOPIC_AUDIT", "cdm.streamguard.audit"
)


# ── CALLBACKS ─────────────────────────────────────────────────

def delivery_callback(err, msg):
    if err:
        logger.error(
            f"❌ Échec livraison sécurisée — "
            f"topic={msg.topic()} erreur={err}"
        )
    else:
        logger.info(
            f"🔒 Deal sécurisé publié — "
            f"topic={msg.topic()} "
            f"offset={msg.offset()} "
            f"key={msg.key().decode('utf-8') if msg.key() else 'None'}"
        )


# ── PRODUCER SÉCURISÉ ─────────────────────────────────────────

class StreamGuardProducer:
    """
    Publie les deals pseudonymisés dans le topic sécurisé.

    Deux topics gérés :

    1. cdm.sisdm.kondor.dbo.deals.secure
       → deals pseudonymisés pour NiFi
       → NiFi → Iceberg Bronze sécurisé

    2. cdm.streamguard.audit
       → log de chaque pseudonymisation
       → traçabilité BAM
       → qui a été pseudonymisé, quand, comment
    """

    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers"  : KAFKA_BOOTSTRAP_SERVERS,
            "client.id"          : "streamguard-producer",
            "acks"               : "all",
            "retries"            : 3,
            "retry.backoff.ms"   : 1000,
            "compression.type"   : "snappy",
        })

        logger.info(
            f"Producer sécurisé initialisé — "
            f"topic_secure={TOPIC_SECURE} "
            f"topic_audit={TOPIC_AUDIT}"
        )

    def publish_secure(
        self,
        deal_key: str,
        pseudo_deal: dict,
        original_message: dict,
    ) -> bool:
        """
        Publie le deal pseudonymisé dans le topic sécurisé.

        Le message conserve le format Debezium
        mais avec les valeurs pseudonymisées dans "after".

        Args:
            deal_key       : clé du deal (token pseudonymisé)
            pseudo_deal    : deal avec tokens
            original_message : message Debezium original
                               (pour conserver les métadonnées source)

        Returns:
            True si publié avec succès
        """
        try:
            # Reconstruit le message Debezium avec deal pseudonymisé
            secure_message = self._build_secure_message(
                pseudo_deal, original_message
            )

            key   = deal_key.encode("utf-8")
            value = json.dumps(
                secure_message,
                ensure_ascii=False,
                default=str,
            ).encode("utf-8")

            self.producer.produce(
                topic    = TOPIC_SECURE,
                key      = key,
                value    = value,
                callback = delivery_callback,
            )
            self.producer.poll(0)
            return True

        except Exception as e:
            logger.error(f"Erreur publication sécurisée : {e}")
            return False

    def publish_audit(
        self,
        deal_key: str,
        entities: list,
        latency_ms: float,
        session_id: str,
    ) -> bool:
        """
        Publie le log d'audit dans le topic audit.

        Chaque pseudonymisation est tracée :
        → qui a été pseudonymisé
        → quelle méthode (regex/llm)
        → score de confiance
        → latence de traitement
        → timestamp

        Répond à l'audit BAM :
        "Quelles données ont été pseudonymisées
         et comment ?"

        Args:
            deal_key   : token du deal (DEAL_a3f9)
            entities   : liste des entités détectées
            latency_ms : temps de pseudonymisation en ms
            session_id : identifiant de session
        """
        try:
            from datetime import datetime

            audit_record = {
                "timestamp"   : datetime.now().isoformat(),
                "session_id"  : session_id,
                "deal_token"  : deal_key,
                "latency_ms"  : round(latency_ms, 2),
                "nb_entities" : len(entities),
                "entities"    : [
                    {
                        "field"      : e.field,
                        "entity_type": e.entity_type,
                        "method"     : e.method,
                        "confidence" : e.confidence,
                        "token"      : e.token,
                    }
                    for e in entities
                ],
                "compliance"  : "BAM_N5_G_2022",
            }

            value = json.dumps(
                audit_record,
                ensure_ascii=False,
            ).encode("utf-8")

            self.producer.produce(
                topic = TOPIC_AUDIT,
                key   = deal_key.encode("utf-8"),
                value = value,
            )
            self.producer.poll(0)
            return True

        except Exception as e:
            logger.error(f"Erreur publication audit : {e}")
            return False

    def _build_secure_message(
        self,
        pseudo_deal: dict,
        original_message: dict,
    ) -> dict:
        """
        Reconstruit le message Debezium
        avec le deal pseudonymisé dans "after".

        Conserve les métadonnées source (lsn, ts_ms, table)
        pour la traçabilité Atlas.
        """
        payload = original_message.get("payload", {})
        debezium = payload.get("payload", payload)

        return {
            "schema" : original_message.get("schema", {}),
            "payload": {
                "op"    : debezium.get("op", "c"),
                "before": None,
                "after" : pseudo_deal,
                "source": {
                    **debezium.get("source", {}),
                    "streamguard": True,
                    "pseudonymized": True,
                },
                "ts_ms" : debezium.get("ts_ms"),
            }
        }

    def flush(self, timeout: int = 10):
        """Attend que tous les messages soient confirmés."""
        self.producer.flush(timeout=timeout)
        logger.info("Flush terminé — tous les messages confirmés")

    def close(self):
        """Ferme proprement le producer."""
        self.flush()
        logger.info("Producer sécurisé fermé")


# ── TEST LOCAL ───────────────────────────────────────────────

if __name__ == "__main__":
    """
    Test : publie un deal pseudonymisé dans le topic sécurisé.
    Vérifie dans Kafka UI que le message arrive.
    """
    print("\n" + "="*60)
    print("STREAMGUARD PRODUCER — Test local")
    print("="*60)

    producer = StreamGuardProducer()

    # Deal pseudonymisé simulé
    pseudo_deal = {
        "deal_id"     : "DEAL_a3f9",
        "montant"     : "AMT_b2e1",
        "taux"        : "RATE_c4d7",
        "devise"      : "CCY_d1e2",
        "trader"      : "TRADER_e5f3",
        "contrepartie": "ENTITY_f6g7",
        "desk"        : "SDM_CASA",
        "statut"      : "CONFIRMED",
    }

    # Message Debezium original simulé
    original_message = {
        "payload": {
            "op"    : "c",
            "after" : {"deal_id": "CDM-08547"},
            "source": {
                "db"    : "KONDOR",
                "table" : "deals",
                "lsn"   : "0x0000A3",
                "ts_ms" : 1744723918000,
            },
            "ts_ms" : 1744723918000,
        }
    }

    success = producer.publish_secure(
        deal_key         = "DEAL_a3f9",
        pseudo_deal      = pseudo_deal,
        original_message = original_message,
    )

    print(f"\nPublication sécurisée : {'✅' if success else '❌'}")
    print(f"Topic : {TOPIC_SECURE}")
    print(f"Deal  : {pseudo_deal['deal_id']}")

    producer.close()
    print("="*60 + "\n")
