"""
StreamGuard — Privacy Gateway
================================
Inspiré de ØllamØnym (Apache 2.0) — H-Ismael
Adapté au contexte Capital Markets CDM

Rôle :
→ Pseudonymise les questions avant envoi au LLM
→ Dépseudonymise les réponses avant retour à l'employé
→ Mapping stocké dans PostgreSQL (session-stable)

Pattern exact ØllamØnym :
  question → pseudonymize_text() → mapping
  → LLM → réponse avec tokens
  → deanonymize() → réponse avec vraies valeurs

Auteur : Roui Anas — PFE EMI MIS 2026 — DSIG CDM
"""

import re
import hmac
import hashlib
import base64
import logging
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# ── SECRET CDM ────────────────────────────────────────────────
# Chargé depuis .env en production
import os
CDM_SECRET = os.getenv("CDM_PSEUDONYM_SECRET", "cdm_secret_dev_2026")

# ── ENTITÉS FINANCIÈRES CDM ───────────────────────────────────
# Regex déterministes pour les entités Capital Markets

CDM_ENTITIES = [
    # Devise pairs
    {
        "id"     : "CCY",
        "pattern": r"\b(EUR|USD|GBP|CHF|JPY|MAD)/(EUR|USD|GBP|CHF|JPY|MAD)\b",
        "desc"   : "Currency pair",
    },
    # Montants financiers
    {
        "id"     : "AMT",
        "pattern": r"\b\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?\s*(?:MAD|EUR|USD|GBP|M|K|milliards?)?\b",
        "desc"   : "Financial amount",
    },
    # Taux de change
    {
        "id"     : "RATE",
        "pattern": r"\b1[0-9]\.[0-9]{2,4}\b",
        "desc"   : "Exchange rate",
    },
    # Deal ID CDM
    {
        "id"     : "DEAL",
        "pattern": r"\bCDM-\d{4,6}\b",
        "desc"   : "Deal identifier",
    },
    # Trader
    {
        "id"     : "TRADER",
        "pattern": r"\bTRADER_\d{2}\b",
        "desc"   : "Trader identifier",
    },
    # IBAN Maroc
    {
        "id"     : "IBAN",
        "pattern": r"\bMA\d{2}[A-Z0-9]{20,28}\b",
        "desc"   : "IBAN",
    },
    # SWIFT/BIC
    {
        "id"     : "SWIFT",
        "pattern": r"\b[A-Z]{4}MA[A-Z0-9]{2}([A-Z0-9]{3})?\b",
        "desc"   : "SWIFT/BIC code",
    },
    # Banques marocaines connues
    {
        "id"     : "ENTITY",
        "pattern": r"\b(Attijariwafa|BMCE|CIH|BCP|BMCI|Société Générale Maroc|BNP Paribas Maroc|CDM|Crédit du Maroc)\b",
        "desc"   : "Financial institution",
    },
]


# ── TOKEN GENERATION ──────────────────────────────────────────

def generate_token(
    entity_id  : str,
    canon_text : str,
    session_id : str,
    token_len  : int = 4,
) -> str:
    """
    Génère un token déterministe via HMAC-SHA256.
    Exactement le même pattern que ØllamØnym.

    Session-stable : même entité = même token
    dans la même session.
    """
    input_str = f"{session_id}|{entity_id}|{canon_text.lower().strip()}"

    digest = hmac.new(
        CDM_SECRET.encode("utf-8"),
        input_str.encode("utf-8"),
        hashlib.sha256,
    ).digest()

    b32 = base64.b32encode(digest).decode("utf-8").rstrip("=")
    token_id = b32[:token_len].lower()

    return f"{entity_id}_{token_id}"


# ── PSEUDONYMIZER ─────────────────────────────────────────────

class StreamGuardGateway:
    """
    Privacy Gateway — pattern ØllamØnym adapté CDM.

    Usage :
        gw = StreamGuardGateway(session_id="sess_001")

        # Pseudonymise
        pseudo, mapping = gw.pseudonymize("Position EUR/MAD ?")
        # pseudo  : "Position CCY_ab12 ?"
        # mapping : {"CCY_ab12": "EUR/MAD"}

        # ... LLM génère SQL avec tokens ...

        # Dépseudonymise
        real = gw.deanonymize("CCY_ab12 : 490M", mapping)
        # real : "EUR/MAD : 490M"
    """

    def __init__(self, session_id: str = "default"):
        self.session_id    = session_id
        # Cache session — même token pour même valeur
        self._cache: dict[str, str] = {}

    def pseudonymize(
        self,
        text: str,
    ) -> tuple[str, dict[str, str]]:
        """
        Pseudonymise un texte.

        Returns:
            (texte_pseudonymisé, mapping token→original)
        """
        mapping      = {}   # token → original
        result       = text
        replacements = []   # (start, end, original, token)

        # ── Détection par regex ───────────────────────────────
        for entity in CDM_ENTITIES:
            for match in re.finditer(entity["pattern"], result, re.IGNORECASE):
                original = match.group(0)
                canon    = original.upper().strip()

                # Vérifier le cache session
                if canon in self._cache:
                    token = self._cache[canon]
                else:
                    token = generate_token(
                        entity_id  = entity["id"],
                        canon_text = canon,
                        session_id = self.session_id,
                    )
                    self._cache[canon] = token

                mapping[token]  = original
                replacements.append((match.start(), match.end(), original, token))

        # Appliquer les remplacements de droite à gauche
        # pour ne pas décaler les positions
        replacements.sort(key=lambda x: x[0], reverse=True)

        for start, end, original, token in replacements:
            result = result[:start] + token + result[end:]

        logger.info(
            f"Pseudonymisé {len(mapping)} entités — "
            f"session={self.session_id}"
        )
        return result, mapping

    def deanonymize(
        self,
        text   : str,
        mapping: dict[str, str],
    ) -> str:
        """
        Dépseudonymise un texte.
        Remplace chaque token par sa valeur originale.

        Exactement Deanonymizer.deanonymize() de ØllamØnym.
        """
        result = text

        for token, original in mapping.items():
            result = result.replace(token, original)

        return result

    def deanonymize_data(
        self,
        data   : list[dict],
        mapping: dict[str, str],
    ) -> list[dict]:
        """
        Dépseudonymise une liste de résultats SQL.
        Utilisé pour les résultats Dremio.
        """
        real_data = []

        for row in data:
            real_row = {}
            for key, value in row.items():
                real_key   = self.deanonymize(str(key),   mapping)
                real_value = self.deanonymize(str(value), mapping)
                real_row[real_key] = real_value
            real_data.append(real_row)

        return real_data
