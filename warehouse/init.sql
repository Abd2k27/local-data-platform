-- Runs once, on first start of the Postgres container.
-- Local-only credentials: this stack is not meant to be exposed.

CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE USER warehouse WITH PASSWORD 'warehouse';
CREATE DATABASE warehouse OWNER warehouse;

CREATE USER metabase WITH PASSWORD 'metabase';
CREATE DATABASE metabase OWNER metabase;

\connect warehouse

-- raw     : data as delivered by the source (loaded by ingestion/)
-- staging : cleaned & typed views (dbt)
-- marts   : business-ready tables (dbt)
CREATE SCHEMA IF NOT EXISTS raw     AUTHORIZATION warehouse;
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION warehouse;
CREATE SCHEMA IF NOT EXISTS marts   AUTHORIZATION warehouse;
