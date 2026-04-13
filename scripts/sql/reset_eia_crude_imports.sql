-- Reset only EIA crude-oil-imports data (test/dev). Run against DATABASE_URL, e.g.:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/sql/reset_eia_crude_imports.sql
--
-- Order respects FKs: revisions → trade_flows → ingestion runs; then DQ + fingerprints.

BEGIN;

DELETE FROM trade_flow_revisions
WHERE trade_flow_id IN (
  SELECT id FROM trade_flows
  WHERE source = 'eia' AND dataset = 'crude-oil-imports'
);

DELETE FROM trade_flows
WHERE source = 'eia' AND dataset = 'crude-oil-imports';

DELETE FROM ingestion_runs
WHERE source_hint = 'eia';

DELETE FROM data_quality_issues
WHERE source = 'eia' AND dataset = 'crude-oil-imports';

DELETE FROM schema_fingerprints
WHERE source = 'eia' AND dataset = 'crude-oil-imports';

COMMIT;
