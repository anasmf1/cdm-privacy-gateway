"""
StreamGuard — Pseudonymizer
============================
Détecte et pseudonymise les entités financières sensibles
dans les messages Kafka en temps réel.

Auteur  : Roui Anas — PFE EMI MIS 2026 — DSIG CDM

RÔLE DANS LE PIPELINE :

  consumer.py → [pseudonymizer.py] → producer.py

  Reçoit un deal brut :
  { deal_id: "CDM-08547", trader: "TRADER_01", ... }

  Retourne un deal pseudonymisé :
  { deal_id: "DEAL_a3f9", trader: "PERSON_b2e1", ... }

DEUX MÉTHODES DE DÉTECTION :
  1. Déterministe (regex)  → entités à pattern fixe
     DEAL_ID, CURRENCY_PAIR, SWIFT, IBAN, TRADE_RATE
  2. LLM contextuel (Mistral) → entités complexes
     COUNTERPARTY, PERSON
"""

import re
import json
import hashlib
import logging
import os
import requests
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STREAMGUARD:PSEUDO] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── CONFIGURATION ─────────────────────────────────────────────

OLLAMA_URL       = os.getenv("OLLAMA_URL", "http://localhost:11434")
PSEUDONYM_SECRET = os.getenv("CDM_PSEUDONYM_SECRET", "cdm_default_secret")
OLLAMA_MODEL     = os.getenv("OLLAMA_MODEL", "mistral")

# ── NIVEAUX DE SENSIBILITÉ ────────────────────────────────────
# Niveau 3 = critique  → pseudonymisé toujours
# Niveau 2 = sensible  → pseudonymisé
# Niveau 1 = interne   → pseudonymisé

SENSITIVITY = {
    "deal_id"      : 3,
    "bank_account" : 3,
    "swift_bic"    : 3,
    "trader"       : 2,
    "contrepartie" : 2,
    "montant"      : 2,
    "taux"         : 1,
    "devise"       : 1,
}

# ── PATTERNS REGEX CDM ────────────────────────────────────────

PATTERNS = {
    "deal_id": re.compile(
        r"\b(?:CDM|TRD|DEAL)-[A-Z0-9\-]{3,20}\b",
        re.IGNORECASE
    ),
    "currency_pair": re.compile(
        r"\b[A-Z]{3}/[A-Z]{3}\b"
    ),
    "swift_bic": re.compile(
        r"\b[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}(?:[A-Z0-9]{3})?\b"
    ),
    "iban": re.compile(
        r"\bMA\d{2}[\s]?\d{4}[\s]?\d{4}[\s]?\d{4}[\s]?\d{4}[\s]?\d{4}[\s]?\d{2}\b",
        re.IGNORECASE
    ),
    "trader_code": re.compile(
        r"\b(?:TRADER|TRAD|TR)_\d{1,4}\b",
        re.IGNORECASE
    ),
    "trade_amount": re.compile(
        r"\b\d{1,3}(?:[,\s]\d{3}){2,}(?:\.\d{1,4})?\b"
        r"|\b\d{6,}(?:\.\d{1,4})?\b"
    ),
    "trade_rate": re.compile(
        r"\b1[0-4]\.\d{2,6}\b"
    ),
}

# Préfixes des tokens par type d'entité
TOKEN_PREFIXES = {
    "deal_id"      : "DEAL",
    "currency_pair": "CCY",
    "swift_bic"    : "SWIFT",
    "iban"         : "IBAN",
    "trader_code"  : "TRADER",
    "trade_amount" : "AMT",
    "trade_rate"   : "RATE",
    "counterparty" : "ENTITY",
    "person"       : "PERSON",
}


# ── ENTITÉ DÉTECTÉE ───────────────────────────────────────────

@dataclass
class DetectedEntity:
    field:      str        # nom du champ deal (ex: "deal_id")
    value:      str        # valeur originale (ex: "CDM-08547")
    entity_type: str       # type (ex: "deal_id")
    method:     str        # "regex" ou "llm"
    confidence: float      # 1.0 pour regex, 0.7-0.95 pour LLM
    token:      str = ""   # token assigné (ex: "DEAL_a3f9")


# ── PSEUDONYMIZER PRINCIPAL ───────────────────────────────────

class StreamGuardPseudonymizer:
    """
    Pseudonymise les champs sensibles d'un deal CDM.

    Détection hybride :
    → Regex pour les patterns fixes (DEAL_ID, SWIFT, etc.)
    → Mistral local pour les entités contextuelles
       (noms de contreparties, personnes)

    Session-stable :
    → La même valeur dans la même session
      donne toujours le même token.
    → Permet la dépseudonymisation cohérente.
    """

    def __init__(self, session_id: str = "default"):
        self.session_id = session_id
        # Mappings session
        self.forward:  Dict[str, str] = {}  # valeur → token
        self.backward: Dict[str, str] = {}  # token → valeur
        self.entities: List[DetectedEntity] = []

    def pseudonymize_deal(self, deal: dict) -> Tuple[dict, List[DetectedEntity]]:
        """
        Pseudonymise tous les champs sensibles d'un deal.

        Args:
            deal : dict du deal brut depuis Kafka

        Returns:
            (deal_pseudonymisé, liste des entités détectées)
        """
        pseudo_deal = deal.copy()
        detected    = []

        # ── CHAMPS À TRAITEMENT DÉTERMINISTE ─────────────────
        deterministic_fields = {
            "deal_id"  : "deal_id",
            "devise"   : "currency_pair",
            "trader"   : "trader_code",
            "montant"  : "trade_amount",
            "taux"     : "trade_rate",
        }

        for field, entity_type in deterministic_fields.items():
            if field in deal and deal[field] is not None:
                value = str(deal[field])
                entity = self._detect_regex(
                    field, value, entity_type
                )
                if entity:
                    token = self._get_or_create_token(
                        entity_type, value
                    )
                    entity.token      = token
                    pseudo_deal[field] = token
                    detected.append(entity)
                    logger.debug(
                        f"Pseudonymisé [{entity_type}] "
                        f"{value} → {token}"
                    )

        # ── CHAMPS À TRAITEMENT LLM ───────────────────────────
        llm_fields = ["contrepartie", "desk"]

        for field in llm_fields:
            if field in deal and deal[field] is not None:
                value = str(deal[field])
                entity = self._detect_llm(field, value)
                if entity:
                    token = self._get_or_create_token(
                        entity.entity_type, value
                    )
                    entity.token       = token
                    pseudo_deal[field] = token
                    detected.append(entity)

        self.entities.extend(detected)
        return pseudo_deal, detected

    def depseudonymize_deal(self, pseudo_deal: dict) -> dict:
        """
        Restaure les vraies valeurs depuis les tokens.

        Args:
            pseudo_deal : deal avec tokens

        Returns:
            deal avec vraies valeurs
        """
        real_deal = pseudo_deal.copy()

        for field, value in pseudo_deal.items():
            if isinstance(value, str) and value in self.backward:
                real_deal[field] = self.backward[value]

        return real_deal

    def _detect_regex(
        self,
        field: str,
        value: str,
        entity_type: str,
    ) -> Optional[DetectedEntity]:
        """
        Détection par regex.
        Confidence = 1.0 car déterministe.
        """
        pattern = PATTERNS.get(entity_type)

        if pattern and pattern.search(value):
            return DetectedEntity(
                field       = field,
                value       = value,
                entity_type = entity_type,
                method      = "regex",
                confidence  = 1.0,
            )

        # Si pas de pattern → pseudonymise quand même
        # les champs sensibles connus
        if field in ["deal_id", "trader", "montant", "taux"]:
            return DetectedEntity(
                field       = field,
                value       = value,
                entity_type = entity_type,
                method      = "regex_fallback",
                confidence  = 0.95,
            )

        return None

    def _detect_llm(
        self,
        field: str,
        value: str,
    ) -> Optional[DetectedEntity]:
        """
        Détection contextuelle via Mistral local.
        Utilisée pour les contreparties et noms de personnes.
        """
        try:
            prompt = f"""
Est-ce que cette valeur est sensible financièrement ?
Valeur : "{value}"
Champ  : "{field}"

Réponds UNIQUEMENT avec ce JSON :
{{"is_sensitive": true/false, "entity_type": "COUNTERPARTY/PERSON/OTHER"}}
"""
            response = requests.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model" : OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False,
                },
                timeout=10,
            )

            result = json.loads(
                response.json()["response"].strip()
            )

            if result.get("is_sensitive"):
                etype = result.get(
                    "entity_type", "OTHER"
                ).lower()
                return DetectedEntity(
                    field       = field,
                    value       = value,
                    entity_type = etype,
                    method      = "llm",
                    confidence  = 0.85,
                )

        except Exception as e:
            # Si Mistral indisponible →
            # pseudonymise quand même par sécurité
            logger.warning(
                f"LLM indisponible — "
                f"pseudonymisation par défaut : {e}"
            )
            return DetectedEntity(
                field       = field,
                value       = value,
                entity_type = "counterparty",
                method      = "fallback",
                confidence  = 0.5,
            )

        return None

    def _get_or_create_token(
        self,
        entity_type: str,
        value: str,
    ) -> str:
        """
        Retourne le token existant ou en crée un nouveau.
        Session-stable : même valeur → même token.
        """
        if value in self.forward:
            return self.forward[value]

        # Génère un token déterministe par hash
        prefix = TOKEN_PREFIXES.get(entity_type, "TOKEN")
        token_id = self._hash(entity_type, value)
        token = f"{prefix}_{token_id}"

        # Sauvegarde dans les deux sens
        self.forward[value]  = token
        self.backward[token] = value

        return token

    def _hash(self, entity_type: str, value: str) -> str:
        """
        Hash déterministe SHA-256.
        Même entrée → même sortie toujours.
        """
        input_str = f"{PSEUDONYM_SECRET}|{self.session_id}|{entity_type}|{value}"
        digest = hashlib.sha256(
            input_str.encode("utf-8")
        ).hexdigest()
        return digest[:6]

    def get_stats(self) -> dict:
        """Retourne les statistiques de pseudonymisation."""
        by_method = {"regex": 0, "llm": 0, "fallback": 0}
        for e in self.entities:
            method = e.method.split("_")[0]
            by_method[method] = by_method.get(method, 0) + 1

        return {
            "total_entities"    : len(self.entities),
            "unique_values"     : len(self.forward),
            "by_method"         : by_method,
            "session_id"        : self.session_id,
        }


# ── TEST LOCAL ───────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "="*60)
    print("STREAMGUARD PSEUDONYMIZER — Test local")
    print("="*60)

    pseudo = StreamGuardPseudonymizer(session_id="test_session")

    # Deal de test simulant KONDOR+
    deal = {
        "deal_id"     : "CDM-08547",
        "montant"     : 10000000,
        "taux"        : 10.91,
        "devise"      : "EUR/MAD",
        "trader"      : "TRADER_01",
        "contrepartie": "Attijariwafa Bank",
        "desk"        : "SDM_CASA",
        "statut"      : "CONFIRMED",
    }

    print("\n── Deal original ──")
    for k, v in deal.items():
        print(f"  {k:15s} : {v}")

    # Pseudonymise
    pseudo_deal, entities = pseudo.pseudonymize_deal(deal)

    print("\n── Deal pseudonymisé ──")
    for k, v in pseudo_deal.items():
        original = deal.get(k, "")
        changed  = "🔒" if str(original) != str(v) else "  "
        print(f"  {changed} {k:15s} : {v}")

    print("\n── Entités détectées ──")
    for e in entities:
        print(
            f"  [{e.method:15s}] "
            f"conf={e.confidence:.2f} | "
            f"{e.field:15s} | "
            f"{e.value} → {e.token}"
        )

    # Dépseudonymise
    real_deal = pseudo.depseudonymize_deal(pseudo_deal)
    print("\n── Dépseudonymisation ──")
    match = real_deal == deal
    print(f"  Reconstruction correcte : {'✅' if match else '❌'}")

    print("\n── Stats ──")
    stats = pseudo.get_stats()
    for k, v in stats.items():
        print(f"  {k} : {v}")

    print("="*60 + "\n")
