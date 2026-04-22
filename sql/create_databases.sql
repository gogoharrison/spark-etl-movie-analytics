-- sql/create_databases.sql
--
-- Runs automatically on first Postgres container boot (placed in
-- /docker-entrypoint-initdb.d by the docker-compose volume mount).
--
-- Creates the two additional databases required by Airflow and Metabase.
-- moviedb is already created by POSTGRES_DB in docker-compose.yml.
-- All three databases use the same movieuser role for simplicity on local dev.

SELECT 'CREATE DATABASE airflowdb OWNER movieuser'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflowdb')\gexec

SELECT 'CREATE DATABASE metabasedb OWNER movieuser'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabasedb')\gexec