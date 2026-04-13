{{
  config(
    materialized = 'incremental',
    unique_key   = 'deal_id',
    on_schema_change = 'sync_all_columns'
  )
}}

/*
  CDM StreamGuard — Bronze Layer
  ================================
  Table source : Iceberg Bronze (écrite par Apache Flink)
  Données pseudonymisées par StreamGuard

  Ce model :
  → Lit les données brutes depuis Iceberg Bronze
  → Ajoute les métadonnées de traitement
  → Filtre les doublons (unique_key = deal_id)

  Auteur : Roui Anas — PFE EMI MIS 2026 — DSIG CDM
*/

WITH source AS (

    SELECT
        deal_id,
        montant,
        taux,
        devise,
        trader,
        contrepartie,
        desk,
        statut,
        type_deal,
        date_valeur,
        ts_kafka,
        ts_bronze,
        partition_date

    FROM {{ source('bronze', 'deals_raw') }}

    {% if is_incremental() %}
    WHERE ts_bronze > (SELECT MAX(ts_bronze) FROM {{ this }})
    {% endif %}

),

cleaned AS (

    SELECT
        deal_id,
        CAST(montant AS DECIMAL(18,2))       AS montant,
        CAST(taux AS DECIMAL(10,4))          AS taux,
        UPPER(TRIM(devise))                  AS devise,
        UPPER(TRIM(trader))                  AS trader,
        UPPER(TRIM(contrepartie))            AS contrepartie,
        UPPER(TRIM(desk))                    AS desk,
        UPPER(TRIM(statut))                  AS statut,
        UPPER(TRIM(type_deal))               AS type_deal,
        CAST(date_valeur AS DATE)            AS date_valeur,
        CAST(ts_kafka AS TIMESTAMP)          AS ts_kafka,
        CAST(ts_bronze AS TIMESTAMP)         AS ts_bronze,
        CAST(partition_date AS DATE)         AS partition_date,
        CURRENT_TIMESTAMP                    AS ts_dbt_processed

    FROM source

    WHERE deal_id IS NOT NULL
      AND montant IS NOT NULL
      AND taux IS NOT NULL

)

SELECT * FROM cleaned
