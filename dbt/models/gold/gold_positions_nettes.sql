{{
  config(
    materialized = 'table'
  )
}}

/*
  CDM StreamGuard — Gold Layer : positions_nettes
  =================================================
  Source : silver_deals_clean
  Agrégation : positions nettes par devise
  Utilisé par : Dashboard · Assistant IA · Power BI

  Auteur : Roui Anas — PFE EMI MIS 2026 — DSIG CDM
*/

WITH deals AS (

    SELECT *
    FROM {{ ref('silver_deals_clean') }}
    WHERE statut = 'CONFIRMED'

),

positions AS (

    SELECT
        devise,
        COUNT(deal_id)          AS nb_deals,
        SUM(montant)            AS position_mad,
        AVG(taux)               AS taux_moyen,
        MIN(taux)               AS taux_min,
        MAX(taux)               AS taux_max,
        SUM(CASE WHEN type_deal = 'ACHAT'  THEN montant ELSE 0 END) AS position_achat,
        SUM(CASE WHEN type_deal = 'VENTE'  THEN montant ELSE 0 END) AS position_vente,
        MIN(date_valeur)        AS premiere_date_valeur,
        MAX(date_valeur)        AS derniere_date_valeur,
        MAX(ts_silver)          AS derniere_maj

    FROM deals
    WHERE DATE(ts_silver) = CURRENT_DATE

    GROUP BY devise

)

SELECT
    devise,
    nb_deals,
    ROUND(position_mad, 2)      AS position_mad,
    ROUND(taux_moyen, 4)        AS taux_moyen,
    ROUND(taux_min, 4)          AS taux_min,
    ROUND(taux_max, 4)          AS taux_max,
    ROUND(position_achat, 2)    AS position_achat,
    ROUND(position_vente, 2)    AS position_vente,
    premiere_date_valeur,
    derniere_date_valeur,
    derniere_maj,
    CURRENT_TIMESTAMP           AS ts_gold

FROM positions
ORDER BY position_mad DESC
