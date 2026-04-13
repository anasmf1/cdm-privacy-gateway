{{
  config(
    materialized = 'incremental',
    unique_key   = 'deal_id',
    on_schema_change = 'sync_all_columns'
  )
}}

/*
  CDM StreamGuard — Silver Layer : anomalies
  ===========================================
  Source : silver_deals_clean + ML scores
  Contient : tous les deals détectés comme anomalies
  Par règle métier ET par Isolation Forest

  Auteur : Roui Anas — PFE EMI MIS 2026 — DSIG CDM
*/

WITH deals AS (

    SELECT *
    FROM {{ ref('silver_deals_clean') }}

    {% if is_incremental() %}
    WHERE ts_silver > (SELECT MAX(detected_at) FROM {{ this }})
    {% endif %}

),

-- Anomalies par règle métier (déterministes)
rule_anomalies AS (

    SELECT
        deal_id,
        anomaly_type_rule       AS anomaly_type,
        CASE
            WHEN anomaly_type_rule = 'RATE_OUTLIER'   THEN -0.85
            WHEN anomaly_type_rule = 'AMOUNT_OUTLIER' THEN -0.90
            WHEN anomaly_type_rule = 'OFF_HOURS'      THEN -0.75
            ELSE -0.80
        END                     AS score,
        taux_zscore,
        volume_relatif,
        hors_heures,
        'RULE_BASED'            AS detection_method,
        ts_silver               AS detected_at

    FROM deals
    WHERE anomaly_type_rule IS NOT NULL

),

-- Anomalies par Isolation Forest (ML)
-- Score calculé par anomaly_detector.py et stocké dans une table intermédiaire
ml_anomalies AS (

    SELECT
        deal_id,
        'ML_ISOLATION_FOREST'   AS anomaly_type,
        isolation_forest_score  AS score,
        taux_zscore,
        volume_relatif,
        hors_heures,
        'ISOLATION_FOREST'      AS detection_method,
        ts_silver               AS detected_at

    FROM deals
    WHERE isolation_forest_score < -0.75

),

-- Union des deux sources d'anomalies
all_anomalies AS (

    SELECT * FROM rule_anomalies
    UNION ALL
    SELECT * FROM ml_anomalies

)

SELECT
    deal_id,
    anomaly_type,
    ROUND(score, 4)     AS score,
    taux_zscore,
    volume_relatif,
    hors_heures,
    detection_method,
    detected_at,
    CURRENT_TIMESTAMP   AS ts_dbt_processed

FROM all_anomalies
