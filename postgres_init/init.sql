
CREATE SCHEMA ydan;

CREATE SCHEMA iceberg_catalog;

CREATE TYPE ydan.scrape_state AS ENUM ('pending', 'running', 'completed', 'failed');
CREATE TYPE ydan.scrape_kind AS ENUM ('video', 'channel');

CREATE TABLE ydan.entries (
  id CHAR(24) PRIMARY KEY,
  kind ydan.scrape_kind NOT NULL,
  state ydan.scrape_state NOT NULL DEFAULT 'pending',
  tries INT NOT NULL DEFAULT 0
);

CREATE OR REPLACE FUNCTION ydan.random_n_entries(n INT, entry_kind ydan.scrape_kind)
RETURNS TABLE(id CHAR(24), tries INT)
LANGUAGE plpgsql
AS $$
BEGIN
  SET search_path TO ydan;

  RETURN QUERY
  WITH
  pending_entries AS (
    SELECT entries.id
    FROM entries
    WHERE state = 'pending' AND kind = entry_kind
  ),
  random_ranks AS (
    SELECT
      trunc(random() * (SELECT COUNT(*) FROM pending_entries))
    FROM generate_series(1, n)
  ),
  ranked_tuples AS (
    SELECT
      pending_entries.id,
      RANK() OVER (ORDER BY pending_entries.id) as "rank"
    FROM pending_entries
  ),
  random_tuples AS (
    SELECT ranked_tuples.id
    FROM ranked_tuples
    WHERE ranked_tuples."rank" - 1 IN (SELECT * FROM random_ranks)
  )
  UPDATE entries updated_es
  SET state = 'running'
  WHERE updated_es.id IN (SELECT * FROM random_tuples)
  RETURNING updated_es.id, updated_es.tries;
END;
$$;
