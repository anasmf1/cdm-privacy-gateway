"""
StreamGuard — Pipeline Orchestrator
=====================================
Orchestre le flux complet :
Consumer → Pseudonymizer → Producer

Auteur  : Roui Anas — PFE EMI MIS 2026 — DSIG CDM

FLUX COMPLET :

  Kafka topic brut
  cdm.sisdm.kondor.dbo.deals
          ↓
  StreamGuardConsumer.poll_message()
          ↓
  StreamGuardPseudonymizer.pseudonymize_deal()
          ↓
  StreamGuardProducer.publish_secure()
  StreamGuardProducer.publish_audit()
          ↓
  Kafka topic sécurisé
  cdm.sisdm.kondor.dbo.deals.secure
          ↓
  NiFi → Iceberg Bronze sécurisé
"""

import time
import uuid
import logging
import signal
import sys
from datetime import datetime
from typing import Dict, Any

from consumer import StreamGuardConsumer
from pseudonymizer import StreamGuardPseudonymizer
from producer import StreamGuardProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STREAMGUARD:PIPELINE] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class StreamGuardPipeline:
    """
    Pipeline principal StreamGuard.

    Lit les deals bruts depuis Kafka,
    les pseudonymise en vol,
    et publie dans le topic sécurisé.

    Métriques collectées en temps réel :
    → deals traités / seconde
    → entités pseudonymisées / deal
    → latence de pseudonymisation
    → taux de succès
    → anomalies détectées
    """

    def __init__(self):
        # Session unique pour cette instance
        self.session_id = str(uuid.uuid4())[:8]

        # Composantes
        self.consumer      = StreamGuardConsumer()
        self.pseudonymizer = StreamGuardPseudonymizer(
            session_id=self.session_id
        )
        self.producer      = StreamGuardProducer()

        # Métriques
        self.metrics: Dict[str, Any] = {
            "deals_processed"   : 0,
            "deals_failed"      : 0,
            "entities_detected" : 0,
            "total_latency_ms"  : 0.0,
            "start_time"        : datetime.now(),
            "last_deal_id"      : None,
            "last_latency_ms"   : 0.0,
        }

        self._running = False

        logger.info(
            f"Pipeline initialisé — session_id={self.session_id}"
        )

    def process_message(self, message: dict) -> bool:
        """
        Traite UN message Kafka :
        1. Extrait le deal
        2. Pseudonymise
        3. Publie dans topic sécurisé
        4. Publie dans topic audit

        Args:
            message : message enrichi depuis consumer.poll_message()

        Returns:
            True si traitement complet réussi
        """
        t_start = time.time()

        try:
            # ── ÉTAPE 1 : Extraire le deal ────────────────────
            deal = self.consumer.extract_deal(message)
            if deal is None:
                return True  # DELETE ignoré → commit quand même

            deal_id = deal.get("deal_id", "UNKNOWN")
            logger.info(f"Traitement deal : {deal_id}")

            # ── ÉTAPE 2 : Pseudonymiser ───────────────────────
            pseudo_deal, entities = \
                self.pseudonymizer.pseudonymize_deal(deal)

            # Token du deal pour la clé Kafka
            deal_token = pseudo_deal.get("deal_id", deal_id)

            # ── ÉTAPE 3 : Publier dans topic sécurisé ─────────
            success = self.producer.publish_secure(
                deal_key         = deal_token,
                pseudo_deal      = pseudo_deal,
                original_message = message["payload"],
            )

            if not success:
                logger.error(
                    f"Échec publication sécurisée — deal={deal_id}"
                )
                return False

            # ── ÉTAPE 4 : Publier audit ───────────────────────
            latency_ms = (time.time() - t_start) * 1000

            self.producer.publish_audit(
                deal_key   = deal_token,
                entities   = entities,
                latency_ms = latency_ms,
                session_id = self.session_id,
            )

            # ── ÉTAPE 5 : Mettre à jour les métriques ─────────
            self._update_metrics(
                deal_id    = deal_id,
                entities   = entities,
                latency_ms = latency_ms,
                success    = True,
            )

            # Log résumé
            logger.info(
                f"✅ Deal traité — "
                f"{deal_id} → {deal_token} | "
                f"entités={len(entities)} | "
                f"latence={latency_ms:.1f}ms"
            )

            return True

        except Exception as e:
            latency_ms = (time.time() - t_start) * 1000
            self._update_metrics(
                deal_id    = message.get("key", "UNKNOWN"),
                entities   = [],
                latency_ms = latency_ms,
                success    = False,
            )
            logger.error(f"Erreur traitement : {e}")
            return False

    def run(self):
        """
        Lance le pipeline en mode continu.
        Tourne jusqu'à Ctrl+C ou signal SIGTERM.
        """
        self._running = True

        # Gestion arrêt propre
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        logger.info(
            f"StreamGuard Pipeline démarré — "
            f"session={self.session_id}"
        )
        logger.info(
            f"Écoute sur topic : "
            f"cdm.sisdm.kondor.dbo.deals"
        )
        logger.info(
            f"Publication sur  : "
            f"cdm.sisdm.kondor.dbo.deals.secure"
        )

        # Log des stats toutes les 30 secondes
        last_stats_log = time.time()

        try:
            self.consumer.run(
                on_message=self.process_message
            )
        finally:
            self._shutdown()

    def _update_metrics(
        self,
        deal_id: str,
        entities: list,
        latency_ms: float,
        success: bool,
    ):
        """Met à jour les métriques internes."""
        if success:
            self.metrics["deals_processed"]    += 1
            self.metrics["entities_detected"]  += len(entities)
            self.metrics["total_latency_ms"]   += latency_ms
            self.metrics["last_deal_id"]        = deal_id
            self.metrics["last_latency_ms"]     = latency_ms
        else:
            self.metrics["deals_failed"] += 1

    def get_metrics(self) -> dict:
        """
        Retourne les métriques actuelles du pipeline.
        Utilisé par le dashboard pour l'affichage temps réel.
        """
        processed = self.metrics["deals_processed"]
        elapsed   = (
            datetime.now() - self.metrics["start_time"]
        ).total_seconds()

        avg_latency = (
            self.metrics["total_latency_ms"] / processed
            if processed > 0 else 0
        )
        throughput = processed / elapsed if elapsed > 0 else 0

        return {
            "session_id"       : self.session_id,
            "deals_processed"  : processed,
            "deals_failed"     : self.metrics["deals_failed"],
            "entities_detected": self.metrics["entities_detected"],
            "avg_latency_ms"   : round(avg_latency, 2),
            "last_latency_ms"  : round(
                self.metrics["last_latency_ms"], 2
            ),
            "throughput_per_s" : round(throughput, 2),
            "last_deal_id"     : self.metrics["last_deal_id"],
            "uptime_seconds"   : round(elapsed),
            "pseudonymizer"    : self.pseudonymizer.get_stats(),
        }

    def _handle_signal(self, signum, frame):
        """Gère les signaux d'arrêt proprement."""
        logger.info(f"Signal {signum} reçu — arrêt en cours...")
        self.consumer._running = False

    def _shutdown(self):
        """Arrêt propre du pipeline."""
        logger.info("Arrêt du pipeline StreamGuard...")
        self.consumer.close()
        self.producer.close()

        # Log métriques finales
        metrics = self.get_metrics()
        logger.info(
            f"Métriques finales — "
            f"deals={metrics['deals_processed']} | "
            f"entités={metrics['entities_detected']} | "
            f"latence_moy={metrics['avg_latency_ms']}ms | "
            f"uptime={metrics['uptime_seconds']}s"
        )


# ── POINT D'ENTRÉE ────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "="*60)
    print("STREAMGUARD — Privacy Streaming Pipeline")
    print("Crédit du Maroc — DSIG — PFE 2026")
    print("="*60 + "\n")

    pipeline = StreamGuardPipeline()

    try:
        pipeline.run()
    except Exception as e:
        logger.error(f"Erreur fatale pipeline : {e}")
        sys.exit(1)
