"""
CDM Synthetic Data Generator
=============================
Simule les deals de la Salle des Marchés KONDOR+
Produit des messages au format exact Debezium CDC

Auteur  : Roui Anas — PFE EMI MIS 2026 — DSIG CDM

RÔLE DANS LE PIPELINE :
  Sans accès CDM (Option 4) :
    [Ce générateur] → Kafka → NiFi → Iceberg → Spark → Dremio

  En production CDM (Option 1) :
    [KONDOR+ MSSQL → CDC → Debezium] → Kafka → même pipeline

  Le topic Kafka et le format des messages sont identiques.
  NiFi ne voit aucune différence.
"""

import random
import time
import uuid
import json
import logging
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GENERATOR] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── RÉFÉRENTIELS CDM ─────────────────────────────────────────

# Paires de devises traitées à la Salle des Marchés CDM
CURRENCY_PAIRS = [
    "EUR/MAD", "USD/MAD", "GBP/MAD",
    "CHF/MAD", "JPY/MAD", "CAD/MAD",
]

# Fourchettes de taux normaux par paire (min, max)
NORMAL_RATES = {
    "EUR/MAD": (10.70, 11.10),
    "USD/MAD": (9.80,  10.20),
    "GBP/MAD": (12.50, 13.20),
    "CHF/MAD": (11.00, 11.50),
    "JPY/MAD": (0.065, 0.075),
    "CAD/MAD": (7.20,  7.60),
}

# Traders de la Salle des Marchés
TRADERS = [
    "TRADER_01", "TRADER_02", "TRADER_03",
    "TRADER_04", "TRADER_05", "TRADER_06",
]

# Contreparties bancaires réelles (anonymisées en prod via Privacy Gateway)
COUNTERPARTIES = [
    "Attijariwafa Bank",
    "BNP Paribas Maroc",
    "Société Générale Maroc",
    "CIH Bank",
    "BMCE Bank of Africa",
    "Banque Populaire Maroc",
    "Goldman Sachs International",
    "Deutsche Bank AG",
    "Crédit Agricole CIB",
    "HSBC Bank Maroc",
]

# Desks de la Salle des Marchés
DESKS = ["SDM_CASA", "Desk FX", "Desk Taux", "Desk Monétaire"]

# Types d'opérations
OPERATION_TYPES = ["SPOT", "FORWARD", "SWAP", "OPTION"]

# Statuts possibles
STATUTS = ["CONFIRMED", "PENDING", "CANCELLED"]

# Fourchettes de montants normaux (en MAD)
NORMAL_AMOUNT_RANGE = (1_000_000, 50_000_000)


# ── COMPTEUR GLOBAL DE DEALS ─────────────────────────────────

class DealCounter:
    """Compteur thread-safe pour les IDs de deals."""
    _count: int = 0

    @classmethod
    def next_id(cls) -> str:
        cls._count += 1
        return f"CDM-{cls._count:05d}"

    @classmethod
    def current(cls) -> int:
        return cls._count


# ── MODÈLE DE DEAL ───────────────────────────────────────────

@dataclass
class Deal:
    """
    Représente un deal de la Salle des Marchés CDM.
    Structure identique à dbo.deals dans KONDOR+.
    """
    deal_id:        str
    montant:        int
    taux:           float
    devise:         str
    trader:         str
    contrepartie:   str
    desk:           str
    statut:         str
    type_operation: str
    date_valeur:    str
    created_at:     str
    updated_at:     str
    is_anomaly:     bool = field(default=False, repr=False)
    anomaly_type:   Optional[str] = field(default=None, repr=False)

    def to_dict(self) -> dict:
        """Sérialise le deal sans les champs internes."""
        d = asdict(self)
        d.pop("is_anomaly", None)
        d.pop("anomaly_type", None)
        return d


# ── GÉNÉRATEUR PRINCIPAL ─────────────────────────────────────

class CDMDealGenerator:
    """
    Génère des deals synthétiques réalistes
    simulant les opérations de la Salle des Marchés CDM.

    Deux types de deals générés :
    1. Deals normaux  (95% du temps) — dans les fourchettes réelles
    2. Deals anomalies (5% du temps) — pour entraîner le modèle ML
    """

    def __init__(self, anomaly_rate: float = 0.05):
        """
        Args:
            anomaly_rate : taux d'anomalies injectées (0.05 = 5%)
        """
        self.anomaly_rate = anomaly_rate
        logger.info(
            f"Générateur initialisé — "
            f"taux anomalies : {anomaly_rate * 100:.0f}%"
        )

    def generate(self) -> Deal:
        """
        Génère un deal — normal ou anomalie selon anomaly_rate.
        Point d'entrée principal.
        """
        if random.random() < self.anomaly_rate:
            return self._generate_anomaly()
        return self._generate_normal()

    def _generate_normal(self) -> Deal:
        """Génère un deal dans les fourchettes normales CDM."""
        devise = random.choice(CURRENCY_PAIRS)
        rate_min, rate_max = NORMAL_RATES[devise]

        now = datetime.now()
        date_valeur = date.today() + timedelta(
            days=random.choice([0, 1, 2, 7, 30])
        )

        return Deal(
            deal_id        = DealCounter.next_id(),
            montant        = random.randint(*NORMAL_AMOUNT_RANGE),
            taux           = round(random.uniform(rate_min, rate_max), 4),
            devise         = devise,
            trader         = random.choice(TRADERS),
            contrepartie   = random.choice(COUNTERPARTIES),
            desk           = random.choice(DESKS),
            statut         = random.choices(
                               STATUTS,
                               weights=[0.85, 0.12, 0.03]
                             )[0],
            type_operation = random.choices(
                               OPERATION_TYPES,
                               weights=[0.60, 0.25, 0.10, 0.05]
                             )[0],
            date_valeur    = date_valeur.isoformat(),
            created_at     = now.isoformat(),
            updated_at     = now.isoformat(),
            is_anomaly     = False,
        )

    def _generate_anomaly(self) -> Deal:
        """
        Génère un deal anormal pour entraîner le modèle ML.

        4 types d'anomalies représentatives des risques CDM :
        1. RATE_OUTLIER    — taux hors fourchette marché
        2. AMOUNT_OUTLIER  — montant inhabituellement élevé
        3. OFF_HOURS       — deal saisi hors heures ouvrées
        4. DUPLICATE_RATE  — taux identique suspect (arrondi)
        """
        anomaly_type = random.choice([
            "RATE_OUTLIER",
            "AMOUNT_OUTLIER",
            "OFF_HOURS",
            "DUPLICATE_RATE",
        ])

        devise = random.choice(CURRENCY_PAIRS)
        rate_min, rate_max = NORMAL_RATES[devise]
        now = datetime.now()

        # Paramètres communs
        base_params = dict(
            deal_id        = DealCounter.next_id(),
            devise         = devise,
            trader         = random.choice(TRADERS),
            contrepartie   = random.choice(COUNTERPARTIES),
            desk           = random.choice(DESKS),
            statut         = "CONFIRMED",
            type_operation = "SPOT",
            date_valeur    = date.today().isoformat(),
            is_anomaly     = True,
            anomaly_type   = anomaly_type,
        )

        if anomaly_type == "RATE_OUTLIER":
            # Taux +/- 15% hors fourchette normale
            direction = random.choice([-1, 1])
            deviation = random.uniform(0.15, 0.30)
            center = (rate_min + rate_max) / 2
            taux = round(center * (1 + direction * deviation), 4)
            deal = Deal(
                montant    = random.randint(*NORMAL_AMOUNT_RANGE),
                taux       = taux,
                created_at = now.isoformat(),
                updated_at = now.isoformat(),
                **base_params,
            )

        elif anomaly_type == "AMOUNT_OUTLIER":
            # Montant > 10x la moyenne normale
            deal = Deal(
                montant    = random.randint(200_000_000, 500_000_000),
                taux       = round(random.uniform(rate_min, rate_max), 4),
                created_at = now.isoformat(),
                updated_at = now.isoformat(),
                **base_params,
            )

        elif anomaly_type == "OFF_HOURS":
            # Deal saisi entre 22h et 5h du matin
            off_hour = random.choice(
                list(range(22, 24)) + list(range(0, 6))
            )
            off_time = now.replace(
                hour=off_hour,
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )
            deal = Deal(
                montant    = random.randint(*NORMAL_AMOUNT_RANGE),
                taux       = round(random.uniform(rate_min, rate_max), 4),
                created_at = off_time.isoformat(),
                updated_at = off_time.isoformat(),
                **base_params,
            )

        else:  # DUPLICATE_RATE
            # Taux arrondi à 2 décimales (suspect — deal manuel ?)
            taux = round(
                random.uniform(rate_min, rate_max), 2
            )
            deal = Deal(
                montant    = random.randint(*NORMAL_AMOUNT_RANGE),
                taux       = taux,
                created_at = now.isoformat(),
                updated_at = now.isoformat(),
                **base_params,
            )

        logger.warning(
            f"⚠️  Anomalie générée — "
            f"type={anomaly_type} "
            f"deal_id={deal.deal_id} "
            f"devise={deal.devise} "
            f"taux={deal.taux} "
            f"montant={deal.montant:,}"
        )
        return deal


# ── FORMAT MESSAGE DEBEZIUM ──────────────────────────────────

def to_debezium_message(deal: Deal, operation: str = "c") -> dict:
    """
    Encapsule un deal dans le format exact
    qu'aurait produit Debezium CDC sur KONDOR+.

    Args:
        deal      : le deal généré
        operation : "c" = create (INSERT)
                    "u" = update (UPDATE)
                    "d" = delete (DELETE)

    Returns:
        dict au format Debezium SQL Server Connector
    """
    deal_dict = deal.to_dict()

    return {
        "schema": {
            "type": "struct",
            "name": "cdm.sisdm.kondor.dbo.deals.Envelope",
            "fields": [
                {"field": "op",     "type": "string"},
                {"field": "before", "type": "struct", "optional": True},
                {"field": "after",  "type": "struct", "optional": True},
                {"field": "source", "type": "struct"},
                {"field": "ts_ms",  "type": "int64"},
            ]
        },
        "payload": {
            "op"    : operation,
            "before": None if operation == "c" else deal_dict,
            "after" : deal_dict if operation != "d" else None,
            "source": {
                "version"  : "2.5.0.Final",
                "connector": "sqlserver",
                "name"     : "cdm.sisdm.kondor",
                "db"       : "KONDOR",
                "schema"   : "dbo",
                "table"    : "deals",
                "lsn"      : str(uuid.uuid4()),
                "ts_ms"    : int(datetime.now().timestamp() * 1000),
            },
            "ts_ms" : int(datetime.now().timestamp() * 1000),
        }
    }


# ── TEST LOCAL ───────────────────────────────────────────────

if __name__ == "__main__":
    """
    Test du générateur sans Kafka.
    Affiche 5 deals normaux et 2 anomalies.
    """
    print("\n" + "="*60)
    print("CDM SYNTHETIC DATA GENERATOR — Test local")
    print("="*60)

    gen = CDMDealGenerator(anomaly_rate=0.3)

    print("\n── 5 deals générés ──\n")
    for i in range(5):
        deal = gen.generate()
        msg  = to_debezium_message(deal)

        status = "⚠️  ANOMALIE" if deal.is_anomaly else "✅ Normal  "
        print(
            f"{status} | "
            f"{deal.deal_id} | "
            f"{deal.devise} | "
            f"taux={deal.taux} | "
            f"montant={deal.montant:>15,} MAD | "
            f"trader={deal.trader}"
        )
        if deal.is_anomaly:
            print(f"           └─ type={deal.anomaly_type}")

    print("\n── Format message Debezium (dernier deal) ──\n")
    print(json.dumps(msg, indent=2, ensure_ascii=False))
    print("\n" + "="*60)
    print(f"Total deals générés : {DealCounter.current()}")
    print("="*60 + "\n")
