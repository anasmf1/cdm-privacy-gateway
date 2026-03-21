-- ============================================================
-- CDM Local — Initialisation SQL Server
-- Simule la base KONDOR+ de la Salle des Marchés CDM
--
-- Auteur : Roui Anas — PFE EMI MIS 2026 — DSIG CDM
-- ============================================================

-- Créer la base KONDOR
CREATE DATABASE KONDOR;
GO

USE KONDOR;
GO

-- ── TABLE DEALS ──────────────────────────────────────────────
-- Simule dbo.deals de KONDOR+ (FIS)
CREATE TABLE dbo.deals (
    deal_id        NVARCHAR(20)   NOT NULL PRIMARY KEY,
    montant        BIGINT         NOT NULL,
    taux           FLOAT          NOT NULL,
    devise         NVARCHAR(10)   NOT NULL,
    trader         NVARCHAR(50)   NULL,
    contrepartie   NVARCHAR(100)  NULL,
    desk           NVARCHAR(50)   NULL,
    statut         NVARCHAR(20)   NOT NULL DEFAULT 'PENDING',
    type_operation NVARCHAR(20)   NOT NULL DEFAULT 'SPOT',
    date_valeur    DATE           NULL,
    created_at     DATETIME       NOT NULL DEFAULT GETDATE(),
    updated_at     DATETIME       NOT NULL DEFAULT GETDATE()
);
GO

-- ── TABLE POSITIONS ──────────────────────────────────────────
CREATE TABLE dbo.positions (
    position_id    NVARCHAR(20)   NOT NULL PRIMARY KEY,
    devise         NVARCHAR(10)   NOT NULL,
    montant_net    BIGINT         NOT NULL DEFAULT 0,
    nb_deals       INT            NOT NULL DEFAULT 0,
    updated_at     DATETIME       NOT NULL DEFAULT GETDATE()
);
GO

-- ── TABLE MTM (Mark-to-Market) ───────────────────────────────
CREATE TABLE dbo.mtm (
    mtm_id         NVARCHAR(20)   NOT NULL PRIMARY KEY,
    deal_id        NVARCHAR(20)   NOT NULL,
    taux_marche    FLOAT          NOT NULL,
    mtm_value      FLOAT          NOT NULL,
    calculated_at  DATETIME       NOT NULL DEFAULT GETDATE()
);
GO

-- ── ACTIVER CDC SUR LA BASE ──────────────────────────────────
EXEC sys.sp_cdc_enable_db;
GO

-- ── ACTIVER CDC SUR CHAQUE TABLE ─────────────────────────────
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'deals',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'positions',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name   = 'mtm',
    @role_name     = NULL,
    @supports_net_changes = 1;
GO

-- ── CRÉER LE COMPTE DEBEZIUM ──────────────────────────────────
CREATE LOGIN debezium_user
    WITH PASSWORD = 'Deb3z!um@Local2026';
GO

CREATE USER debezium_user FOR LOGIN debezium_user;
GO

EXEC sp_addrolemember 'db_datareader', 'debezium_user';
GO

GRANT SELECT ON SCHEMA::cdc TO debezium_user;
GO

-- ── DONNÉES DE TEST ───────────────────────────────────────────
INSERT INTO dbo.deals VALUES
('CDM-00001', 10000000, 10.91, 'EUR/MAD',
 'TRADER_01', 'Attijariwafa Bank', 'SDM_CASA',
 'CONFIRMED', 'SPOT', '2026-04-15', GETDATE(), GETDATE()),

('CDM-00002', 5000000,  10.85, 'USD/MAD',
 'TRADER_02', 'BNP Paribas Maroc', 'SDM_CASA',
 'CONFIRMED', 'SPOT', '2026-04-15', GETDATE(), GETDATE()),

('CDM-00003', 8000000,  13.42, 'GBP/MAD',
 'TRADER_01', 'Société Générale Maroc', 'SDM_CASA',
 'PENDING',   'FORWARD', '2026-04-20', GETDATE(), GETDATE());
GO

PRINT 'Base KONDOR initialisée avec succès ✅';
GO
