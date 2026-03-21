"""
CDM Capital Markets — Deterministic Rule Extractor
Extension de ØllamØnym RuleExtractor pour les entités
spécifiques à la Salle des Marchés du Crédit du Maroc.

Auteur : Roui Anas — PFE EMI MIS 2026 — DSIG CDM
"""

import re
from typing import List, Set
from app.schemas.models import ExtractedEntity


class CDMRuleExtractor:
    """
    Extraction déterministe par regex pour les entités
    financières CDM haute-confiance.

    Principe : les entités à pattern prévisible (DEAL_ID,
    CURRENCY_PAIR, BANK_ACCOUNT, SWIFT_BIC, TRADE_AMOUNT,
    TRADE_RATE) sont détectées ici sans appel LLM.

    Les entités contextuelles (COUNTERPARTY, TRADER, PERSON,
    DESK) sont laissées au modèle Mistral local.
    """

    # ── DEAL IDENTIFIERS ────────────────────────────────────
    # Formats : CDM-08547, TRD-20260415-001, DEAL-EUR-2026-042
    DEAL_ID_PATTERN = re.compile(
        r"\b(?:CDM|TRD|DEAL|POS|NET)-[A-Z0-9]{2,}-?[A-Z0-9-]{0,20}\b",
        flags=re.IGNORECASE,
    )

    # ── POSITION IDENTIFIERS ─────────────────────────────────
    POSITION_ID_PATTERN = re.compile(
        r"\b(?:POS|POSITION|NET)-[A-Z0-9/]{2,}-?[A-Z0-9-]{0,20}\b",
        flags=re.IGNORECASE,
    )

    # ── CURRENCY PAIRS ───────────────────────────────────────
    # Format ISO 4217 : EUR/MAD, USD/MAD, GBP/MAD, etc.
    CURRENCY_PAIR_PATTERN = re.compile(
        r"\b[A-Z]{3}/[A-Z]{3}\b"
    )

    # ── MOROCCAN IBAN ────────────────────────────────────────
    # Format : MA + 2 chiffres + 24 chiffres (total 28)
    IBAN_MA_PATTERN = re.compile(
        r"\bMA\d{2}[\s]?\d{4}[\s]?\d{4}[\s]?\d{4}[\s]?\d{4}[\s]?\d{4}[\s]?\d{2}\b",
        flags=re.IGNORECASE,
    )

    # ── MOROCCAN RIB ─────────────────────────────────────────
    # 24 chiffres consécutifs (RIB marocain)
    RIB_PATTERN = re.compile(
        r"\b\d{3}[\s-]?\d{3}[\s-]?\d{11}[\s-]?\d{2}\b"
    )

    # ── SWIFT / BIC ──────────────────────────────────────────
    # 8 ou 11 caractères : BCDMMAMC ou BCDMMAMC001
    SWIFT_PATTERN = re.compile(
        r"\b[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}(?:[A-Z0-9]{3})?\b"
    )

    # SWIFT connus CDM Maroc pour réduire les faux positifs
    KNOWN_SWIFT_PREFIXES = {
        "BCDM", "SGMB", "ATTM", "BMCM", "BMCE",
        "CIHM", "CFGM", "WAFAM", "BKAM", "BNPA",
    }

    # ── TRADE AMOUNTS ────────────────────────────────────────
    # Montants financiers : 10 000 000, 10,000,000, 490.35M, etc.
    # On cible les grands montants (>= 6 chiffres)
    TRADE_AMOUNT_PATTERN = re.compile(
        r"\b\d{1,3}(?:[,\s]\d{3}){2,}(?:\.\d{1,4})?\b"
        r"|\b\d{6,}(?:\.\d{1,4})?\b"
        r"|\b\d{1,4}(?:\.\d{1,4})?\s*(?:M|millions?|milliards?)\s*(?:MAD|EUR|USD|GBP|CHF)?\b",
        flags=re.IGNORECASE,
    )

    # ── EXCHANGE RATES ───────────────────────────────────────
    # Taux de change : 10.91, 10.8750, taux de 10.93
    # Fourchette MAD : entre 8.00 et 15.00 typiquement
    TRADE_RATE_PATTERN = re.compile(
        r"\b(?:taux|cours|rate|prix)\s+(?:de\s+)?(\d{1,2}\.\d{2,6})\b"
        r"|\b(1[0-4]\.\d{2,6})\b",
        flags=re.IGNORECASE,
    )

    # ── TRADER CODES ─────────────────────────────────────────
    # Codes systèmes : TRADER_01, TRAD_03, TR_007
    TRADER_CODE_PATTERN = re.compile(
        r"\b(?:TRADER|TRAD|TR)_\d{1,4}\b",
        flags=re.IGNORECASE,
    )

    @classmethod
    def extract(
        cls,
        text: str,
        enabled_entity_ids: Set[str],
    ) -> List[ExtractedEntity]:
        """
        Extrait les entités CDM par règles déterministes.

        Args:
            text               : texte brut à analyser
            enabled_entity_ids : entités activées dans le template

        Returns:
            Liste d'ExtractedEntity avec positions
        """
        entities: List[ExtractedEntity] = []

        if "DEAL_ID" in enabled_entity_ids:
            entities.extend(cls._extract_pattern(
                text, cls.DEAL_ID_PATTERN, "DEAL_ID"
            ))

        if "POSITION_ID" in enabled_entity_ids:
            entities.extend(cls._extract_pattern(
                text, cls.POSITION_ID_PATTERN, "POSITION_ID"
            ))

        if "CURRENCY_PAIR" in enabled_entity_ids:
            entities.extend(cls._extract_pattern(
                text, cls.CURRENCY_PAIR_PATTERN, "CURRENCY_PAIR"
            ))

        if "BANK_ACCOUNT" in enabled_entity_ids:
            entities.extend(cls._extract_pattern(
                text, cls.IBAN_MA_PATTERN, "BANK_ACCOUNT"
            ))
            entities.extend(cls._extract_rib(text))

        if "SWIFT_BIC" in enabled_entity_ids:
            entities.extend(cls._extract_swift(text))

        if "TRADE_AMOUNT" in enabled_entity_ids:
            entities.extend(cls._extract_pattern(
                text, cls.TRADE_AMOUNT_PATTERN, "TRADE_AMOUNT"
            ))

        if "TRADE_RATE" in enabled_entity_ids:
            entities.extend(cls._extract_rates(text))

        if "TRADER" in enabled_entity_ids:
            entities.extend(cls._extract_pattern(
                text, cls.TRADER_CODE_PATTERN, "TRADER"
            ))

        return entities

    @staticmethod
    def _extract_pattern(
        text: str,
        pattern: re.Pattern,
        entity_id: str,
    ) -> List[ExtractedEntity]:
        """Extraction générique par regex."""
        return [
            ExtractedEntity(entity_id=entity_id, text=m.group(0).strip())
            for m in pattern.finditer(text)
            if m.group(0).strip()
        ]

    @classmethod
    def _extract_rib(cls, text: str) -> List[ExtractedEntity]:
        """
        Extraction RIB marocain (24 chiffres).
        Filtrage des faux positifs par longueur exacte.
        """
        entities = []
        for m in cls.RIB_PATTERN.finditer(text):
            raw = re.sub(r"[\s-]", "", m.group(0))
            if len(raw) == 24 and raw.isdigit():
                entities.append(
                    ExtractedEntity(entity_id="BANK_ACCOUNT", text=m.group(0).strip())
                )
        return entities

    @classmethod
    def _extract_swift(cls, text: str) -> List[ExtractedEntity]:
        """
        Extraction SWIFT/BIC avec filtre préfixes connus CDM.
        Évite les faux positifs sur des mots en majuscules.
        """
        entities = []
        for m in cls.SWIFT_PATTERN.finditer(text):
            code = m.group(0).upper()
            prefix = code[:4]
            # Accepte seulement les préfixes bancaires connus
            # ou les codes de longueur exacte 8 ou 11
            if prefix in cls.KNOWN_SWIFT_PREFIXES or len(code) in (8, 11):
                entities.append(
                    ExtractedEntity(entity_id="SWIFT_BIC", text=m.group(0).strip())
                )
        return entities

    @classmethod
    def _extract_rates(cls, text: str) -> List[ExtractedEntity]:
        """
        Extraction taux de change.
        Capture les groupes non vides des deux alternatives.
        """
        entities = []
        for m in cls.TRADE_RATE_PATTERN.finditer(text):
            # Groupe 1 : "taux de 10.91"  → capture 10.91
            # Groupe 2 : standalone 10.XXXX
            rate_value = m.group(1) or m.group(2)
            if rate_value:
                entities.append(
                    ExtractedEntity(entity_id="TRADE_RATE", text=rate_value.strip())
                )
        return entities


# ── TEST RAPIDE ──────────────────────────────────────────────
if __name__ == "__main__":
    sample = """
    Le deal CDM-08547 a été exécuté par TRADER_01 avec
    la contrepartie Attijariwafa Bank (SWIFT: ATTMMAMC)
    sur la paire EUR/MAD au taux de 10.91.
    Montant notionnel : 10 000 000 MAD.
    Le compte de règlement est MA64011519000001234567890.
    Position nette référencée POS-EUR-001.
    """

    enabled = {
        "DEAL_ID", "POSITION_ID", "CURRENCY_PAIR",
        "BANK_ACCOUNT", "SWIFT_BIC", "TRADE_AMOUNT",
        "TRADE_RATE", "TRADER",
    }

    results = CDMRuleExtractor.extract(sample, enabled)
    print(f"\n{len(results)} entités détectées :\n")
    for e in results:
        print(f"  [{e.entity_id:15s}] → {e.text}")
