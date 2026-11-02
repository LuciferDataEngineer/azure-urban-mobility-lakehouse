-- Azure-only, unexecuted. In a privileged SQL editor substitute the reviewed
-- storage account and job/analyst principals for ${...}; no secrets belong here.
-- Create mobility_storage credential in Catalog Explorer using the Bicep
-- accessConnectorId output before this script. Disable automatic file events:
-- this bounded batch project does not grant storage-queue permissions.
-- Create mobility_raw and mobility_processed external locations in Catalog
-- Explorer with automatic file events DISABLED before running this script.
-- mobility_raw: abfss://raw@${storage_account}.dfs.core.windows.net/ ; READ ONLY.
-- mobility_processed: abfss://processed@${storage_account}.dfs.core.windows.net/ .
-- Both use mobility_storage. Access connector has only Reader on raw.
CREATE CATALOG IF NOT EXISTS mobility
MANAGED LOCATION 'abfss://processed@${storage_account}.dfs.core.windows.net/managed';
CREATE SCHEMA IF NOT EXISTS mobility.yellow;
CREATE EXTERNAL VOLUME IF NOT EXISTS mobility.yellow.raw
LOCATION 'abfss://raw@${storage_account}.dfs.core.windows.net/';

GRANT USE CATALOG ON CATALOG mobility TO `${job_principal}`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA mobility.yellow TO `${job_principal}`;
GRANT READ VOLUME ON VOLUME mobility.yellow.raw TO `${job_principal}`;
-- CREATE TABLE is scoped to this one schema because the job uses disposable
-- managed staging tables. Only this job identity owns/mutates the Delta tables.
-- Do not give analysts direct storage or Bronze access.
GRANT USE CATALOG ON CATALOG mobility TO `${analyst_group}`;
GRANT USE SCHEMA ON SCHEMA mobility.yellow TO `${analyst_group}`;
-- Run views.sql after the first successful load, then grant SELECT on named
-- views individually. Do not grant SELECT ON SCHEMA (would expose raw_payload).
