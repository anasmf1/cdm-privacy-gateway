{{
  config(
    materialized = 'incremental',
    unique_key   = 'deal_id',
    on_schema_change = 'sync_all_columns'
  )
}}

/*
  CDM StreamGuard — Silver Layer : deals_clean
  =============================================
  Source : Bronze deals_raw
  Transformations :
  → Filtrage des deals invalides
  → Enrichissement avec features ML
  → Score d'anomalie Isolation Forest
  → Classification des types d'anomalies

  Auteur : Roui Anas — PFE EMI MIS 2026 — DSIG CDM
*/

WITH bronze AS (

    SELECT *
    FROM {{ ref('bronze_deals_raw') }}

    {% if is_incremental() %}
    WHERE ts_bronze > (SELECT MAX(ts_bronze) FROM {{ this }})
    {% endif %}

),

-- Filtrage : deals valides uniquement
valid_deals AS (

    SELECT *
    FROM bronze
    WHERE statut IN ('CONFIRMED', 'SETTLED')
      AND montant > 0
      AND taux > 0
      AND devise IS NOT NULL

),

-- Statistiques par devise pour le scoring ML
devise_stats AS (

    SELECT
        devise,
        AVG(taux)    AS avg_taux,
        STDDEV(taux) AS std_taux,
        AVG(montant) AS avg_montant,
        STDDEV(montant) AS std_montant
    FROM bronze
    GROUP BY devise

),

-- Enrichissement avec features ML
enriched AS (

    SELECT
        v.deal_id,
        v.montant,
        v.taux,
        v.devise,
        v.trader,
        v.contrepartie,
        v.desk,
        v.statut,
        v.type_deal,
        v.date_valeur,
        v.ts_bronze,
        v.partition_date,

        -- Features pour l'anomaly detection
        HOUR(v.ts_bronze)                             AS heure_saisie,
        CASE
            WHEN HOUR(v.ts_bronze) < 8
              OR HOUR(v.ts_bronze) >= 18 THEN 1
            ELSE 0
        END                                           AS hors_heures,

        -- Z-score du taux
        CASE
            WHEN s.std_taux > 0
            THEN (v.taux - s.avg_taux) / s.std_taux
            ELSE 0
        END                                           AS taux_zscore,

        -- Volume relatif par rapport à la moyenne
        CASE
            WHEN s.avg_montant > 0
            THEN v.montant / s.avg_montant
            ELSE 1
        END                                           AS volume_relatif,

        -- Classification anomalie règle métier
        CASE
            WHEN v.taux < s.avg_taux - 3 * s.std_taux
              OR v.taux > s.avg_taux + 3 * s.std_taux
            THEN 'RATE_OUTLIER'
            WHEN v.montant > s.avg_montant + 5 * s.std_montant
            THEN 'AMOUNT_OUTLIER'
            WHEN HOUR(v.ts_bronze) < 8
              OR HOUR(v.ts_bronze) >= 18
            THEN 'OFF_HOURS'
            ELSE NULL
        END                                           AS anomaly_type_rule,

        CURRENT_TIMESTAMP                             AS ts_silver

    FROM valid_deals v
    LEFT JOIN devise_stats s ON v.devise = s.devise

)

SELECT * FROM enriched
