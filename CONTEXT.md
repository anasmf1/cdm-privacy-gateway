# CDM Privacy Gateway — Contexte Projet PFE

## Identité
- **Étudiant** : Roui Anas — EMI MIS 2026
- **Email** : m.rouianas@gmail.com
- **GitHub** : github.com/anasmf1
- **Entreprise** : Crédit du Maroc — DSIG
- **Encadrant** : M. Mehdi Seghrouchni (Manager Data & SI)
- **Période** : Février – Juin 2026
- **Soutenance** : Diplôme Ingénieur EMI

---

## Titre Officiel du PFE
"Optimisation Data-Driven des Pipelines DataLake et Développement d'un Assistant Analytique Intelligent pour la Salle des Marchés — Crédit du Maroc"

---

## Contexte CDM
- Banque universelle marocaine — Groupe Holmarcom
- Département : DSIG (Direction SI et Gouvernance)
- Système actuel : SISDM avec ARPSON (Oracle)
- Migration vers : KONDOR+ (FIS) — Novembre 2026
- Contrainte réglementaire : BAM N°5/G/2022
  → données financières ne peuvent pas sortir du datacenter marocain

---

## Architecture Cible (Production CDM)

```
KONDOR+ MSSQL
    ↓ CDC (cdc.dbo_deals_CT)
Debezium (Kafka Connect JAR)
    ↓ Avro
Schema Registry (port 8081)
    ↓ Topics
Kafka Broker KBS/NKP × 3 (port 9092)
    ↓ ConsumeKafka
Apache NiFi (port 8443)
    ↓ PutIceberg + spark-submit
Apache Spark PySpark
    ↓
Apache Iceberg sur HDFS
    Bronze → Silver → Gold
    + silver.ecarts (réconciliation ARPSON/KONDOR+)
    + silver.anomalies (ML)
    ↓
Apache Atlas (lineage — port 21000)
    ↓
Dremio SQL Engine (port 9047 / 31010)
    ↓
Power BI Service (DirectQuery ODBC)
Privacy Gateway ØllamØnym CDM → Mistral Ollama → Flask (port 5000)
ML Anomaly Detection (Isolation Forest vs Autoencoder)
```

---

## Deux Options du Projet

### Option 4 — Local sans accès CDM (EN COURS)
```
Générateur Synthétique Python
    → Kafka (même format que Debezium)
    → NiFi → Iceberg → PySpark → Dremio
    → Atlas → Mistral → Flask
```

### Option 1 — Production CDM (quand accès arrivent)
```
KONDOR+ MSSQL + CDC + Debezium
    → même pipeline
    + Privacy Gateway ØllamØnym CDM
```

**Clé** : même topic Kafka, même format message → pipeline identique.
Quand accès CDM arrivent → on arrête générateur, on démarre Debezium.

---

## Stack Technologique Complète

| Couche | Technologie | Version | Port |
|--------|-------------|---------|------|
| CDC | Debezium SQL Server | 2.5.0 | - |
| Messagerie | Apache Kafka | 7.5.3 | 9092 |
| Schémas | Schema Registry | 7.5.3 | 8081 |
| Connecteur | Kafka Connect | 7.5.3 | 8083 |
| Orchestration | Apache NiFi | 1.23.2 | 8443 |
| Traitement | Apache Spark PySpark | 3.x | - |
| Stockage | Apache Iceberg sur HDFS | - | - |
| Lineage | Apache Atlas | 2.3.0 | 21000 |
| SQL Engine | Dremio | 24.x | 9047/31010 |
| LLM local | Mistral 7B via Ollama | latest | 11434 |
| API | Flask Python | 3.x | 5000 |
| Privacy | ØllamØnym (Ismail Halbade) | main | - |
| Dashboard | Power BI Service | DirectQuery | - |
| Simulation | SQL Server local | 2019 | 1433 |
| ML | Isolation Forest + Autoencoder | - | - |

---

## Privacy Gateway — ØllamØnym CDM
- Projet open source d'Ismail Halbade (Apache 2.0)
- On l'utilise comme dépendance — pas de réécriture
- Notre contribution : template CDM + rule extractor

### Fichiers créés
1. `templates/cdm-capital-markets-v1.json`
   → Template ØllamØnym pour entités Salle des Marchés
   → 13 entités : DEAL_ID, TRADER, COUNTERPARTY,
     CURRENCY_PAIR, TRADE_AMOUNT, TRADE_RATE,
     BANK_ACCOUNT, SWIFT_BIC, POSITION_ID,
     DESK, PERSON, EMAIL, PHONE

2. `cdm_extensions/cdm_rule_extractor.py`
   → Détection déterministe par regex
   → Patterns CDM : CDM-XXXXX, EUR/MAD, IBAN MA, SWIFT

### Flux Privacy Gateway
```
Question employé
    ↓
cdm_rule_extractor.py → détecte entités (regex)
    ↓
ØllamØnym → pseudonymise (tokens session-stable)
    ↓
Mistral → génère SQL sur données anonymisées
    ↓
Dremio → exécute SQL → résultats
    ↓
Mistral → formate réponse en français
    ↓
ØllamØnym → dépseudonymise la réponse
    ↓
Réponse finale à l'employé
```

---

## Couches Iceberg

| Couche | Table | Contenu |
|--------|-------|---------|
| Bronze | bronze.deals_raw | Copie exacte Debezium |
| Silver | silver.deals_clean | Dédoublonné, nettoyé, enrichi |
| Silver | silver.deals_rejetes | Nulls, données invalides |
| Silver | silver.ecarts | Écarts ARPSON vs KONDOR+ |
| Silver | silver.anomalies | ML anomaly detection |
| Gold | gold.positions_nettes | Positions par devise |
| Gold | gold.pnl_journalier | P&L du jour |
| Gold | gold.mtm_intraday | Mark-to-Market temps réel |

---

## Transformations PySpark

### Bronze → Silver (bronze_to_silver.py)
```python
# Dédoublonnage
Window.partitionBy("deal_id").orderBy(timestamp.desc())
# Nettoyage nulls → silver.deals_rejetes
# Enrichissement référentiel devises
# Écriture silver.deals_clean
```

### Silver → Gold (silver_to_gold.py)
```python
# gold.positions_nettes : groupBy("devise").agg(sum, count)
# gold.pnl_journalier   : filter today + agg pnl
# gold.mtm_intraday     : join taux_marche → mtm_value
```

---

## État du Projet

### ✅ Fait
- Architecture complète définie
- Template ØllamØnym CDM (13 entités)
- CDM Rule Extractor (regex déterministes)
- docker-compose.yml (11 services)
- SQL Server init.sql (simule KONDOR+)
- Structure repo GitHub

### 🔄 En cours
- synthetic_data/generator.py
- synthetic_data/kafka_producer.py

### ⏳ À faire
- pipeline/bronze_to_silver.py
- pipeline/silver_to_gold.py
- atlas/lineage_client.py
- gateway/app.py (Flask + ØllamØnym)
- docker/generator/Dockerfile
- docker/gateway/Dockerfile
- Tests end-to-end
- Rapport PFE (80 pages)

---

## Repo GitHub
https://github.com/anasmf1/cdm-privacy-gateway

## Structure Repo
```
cdm-privacy-gateway/
├── templates/
│   └── cdm-capital-markets-v1.json    ✅
├── cdm_extensions/
│   ├── __init__.py
│   └── cdm_rule_extractor.py          ✅
├── synthetic_data/
│   ├── __init__.py
│   ├── generator.py                   🔄
│   └── kafka_producer.py              🔄
├── pipeline/
│   ├── __init__.py
│   ├── bronze_to_silver.py            ⏳
│   └── silver_to_gold.py             ⏳
├── atlas/
│   ├── __init__.py
│   └── lineage_client.py              ⏳
├── gateway/
│   ├── __init__.py
│   └── app.py                         ⏳
├── docker/
│   ├── sqlserver/init.sql             ✅
│   ├── kafka-plugins/
│   ├── nifi/templates/
│   ├── generator/Dockerfile           ⏳
│   └── gateway/Dockerfile             ⏳
├── docker-compose.yml                 ✅
└── README.md
```

---

## Questions Clés pour l'Architecte CDM
1. Oracle Enterprise ou Standard Edition (ARPSON) ?
2. CDC déjà activé sur MSSQL KONDOR+ ?
3. Kafka Connect installé sur cluster Kafka ?
4. VM disponible pour Dremio + Flask (16GB RAM) ?
5. VM GPU disponible pour Mistral Ollama ?

---

## Rapport PFE — Structure 80 pages
- Chapitre 1 : Contexte CDM (8p)
- Chapitre 2 : État de l'art (12p)
- Chapitre 3 : Architecture proposée (15p)
- Chapitre 4 : Implémentation technique (25p)
- Chapitre 5 : Résultats et benchmark ML (12p)
- Chapitre 6 : Conclusion et perspectives (8p)

---

## Instructions pour Claude
Tu travailles avec Anas sur son PFE.
À chaque fichier généré tu dois indiquer
exactement où le mettre dans le repo.
On code ensemble composante par composante.
Chaque composante est testée avant de passer à la suivante.
Le projet doit être déployable en production
chez Crédit du Maroc en novembre 2026.
