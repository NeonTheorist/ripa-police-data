{{ config(materialized = 'table') }}

WITH base AS (
  SELECT
    stop_id,
    COALESCE(perceived_race, 'UNKNOWN') AS perceived_race,
    NULLIF(TRIM(search_basis), '') AS search_basis,
    LOWER(COALESCE(stop_result, '')) AS stop_result
  FROM {{ ref('int_search_outcomes') }}
),

searched AS (
  SELECT DISTINCT
    CAST(stop_id AS STRING) AS stop_id,
    perceived_race,
    COALESCE(search_basis, 'UNKNOWN') AS search_basis
  FROM base
  WHERE search_basis IS NOT NULL
),

found AS (
  SELECT DISTINCT
    CAST(stop_id AS STRING) AS stop_id,
    perceived_race,
    COALESCE(search_basis, 'UNKNOWN') AS search_basis
  FROM base
  WHERE stop_result LIKE '%contraband%' OR stop_result LIKE '%evidence%'
),

search_counts AS (
  SELECT
    perceived_race,
    search_basis,
    COUNT(DISTINCT stop_id) AS searches
  FROM searched
  GROUP BY perceived_race, search_basis
),

find_counts AS (
  SELECT
    perceived_race,
    search_basis,
    COUNT(DISTINCT stop_id) AS finds
  FROM found
  GROUP BY perceived_race, search_basis
)

SELECT
  sc.perceived_race,
  sc.search_basis,
  sc.searches,
  IFNULL(fc.finds, 0) AS finds,
  SAFE_DIVIDE(CAST(IFNULL(fc.finds, 0) AS FLOAT64), CAST(sc.searches AS FLOAT64)) AS yield_rate
FROM search_counts sc
LEFT JOIN find_counts fc
  ON sc.perceived_race = fc.perceived_race
 AND sc.search_basis   = fc.search_basis
ORDER BY sc.perceived_race, sc.search_basis;
