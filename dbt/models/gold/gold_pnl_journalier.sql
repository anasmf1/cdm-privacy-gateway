{{
  config(
    materialized = 'table'
  )
}}

/*
  CDM StreamGuard — Gold Layer : pnl_journalier
  ===============================================
  Source : silver_deals_clean
  Agrégation : P&L quotidien par devise
  Utilisé par : Back Office · Dashboard · Power BI

  P&L = (taux_marche - taux_deal) × montant
  MTM = Mark-to-Market en temps réel

  Auteur : Roui Anas — PFE EMI MIS 2026 — DSIG CDM
*/

WITH deals AS (

    SELECT *
    FROM {{ ref('silver_deals_clean') }}
    WHERE statut IN ('CONFIRMED', 'SETTLED')

),

-- Taux de marché actuels (depuis topic.secure temps réel)
taux_marche AS (

    SELECT
        devise,
        taux                    AS taux_marche,
        ts_silver               AS ts_taux

    FROM {{ ref('silver_deals_clean') }}
    WHERE ts_silver = (
        SELECT MAX(ts_silver)
        FROM {{ ref('silver_deals_clean') }}
    )

),

-- Calcul P&L et MTM
pnl AS (

    SELECT
        d.date_valeur           AS date_trade,
        d.devise,
        COUNT(d.deal_id)        AS nb_deals,
        SUM(d.montant)          AS volume_total,
        AVG(d.taux)             AS taux_deal_moyen,
        MAX(t.taux_marche)      AS taux_marche_actuel,

        -- P&L brut = différence de valeur
        SUM(
            (COALESCE(t.taux_marche, d.taux) - d.taux) * d.montant
        )                       AS pnl_brut,

        -- MTM = valeur au prix de marché
        SUM(d.montant * COALESCE(t.taux_marche, d.taux)) AS mtm_value,

        -- Valeur au prix deal
        SUM(d.montant * d.taux) AS valeur_deal

    FROM deals d
    LEFT JOIN taux_marche t ON d.devise = t.devise

    GROUP BY d.date_valeur, d.devise

)

SELECT
    date_trade,
    devise,
    nb_deals,
    ROUND(volume_total, 2)          AS volume_total,
    ROUND(taux_deal_moyen, 4)       AS taux_deal_moyen,
    ROUND(taux_marche_actuel, 4)    AS taux_marche_actuel,
    ROUND(pnl_brut, 2)              AS pnl_brut,
    ROUND(mtm_value, 2)             AS mtm_value,
    ROUND(valeur_deal, 2)           AS valeur_deal,
    ROUND(mtm_value - valeur_deal, 2) AS mtm_pnl,
    CURRENT_TIMESTAMP               AS ts_gold

FROM pnl
ORDER BY date_trade DESC, ABS(pnl_brut) DESC
