{{ config(materialized = 'table') }}

-- Base rows from the intermediate model
WITH base AS (
  SELECT
    stop_id,
    COALESCE(perceived_race, 'UNKNOWN') AS perceived_race,
    NULLIF(TRIM(stop_reason), '') AS stop_reason,
    NULLIF(TRIM(search_basis), '') AS search_basis,
    LOWER(COALESCE(stop_result, '')) AS stop_result
  FROM {{ ref('int_search_outcomes') }}
),

-- Distinct searches at the grain race x reason x stop
searched AS (
  SELECT DISTINCT
    CAST(stop_id AS STRING) AS stop_id,
    perceived_race,
    COALESCE(stop_reason, 'UNKNOWN') AS stop_reason
  FROM base
  WHERE search_basis IS NOT NULL
),

-- Distinct "find" events (contraband/evidence)
found AS (
  SELECT DISTINCT
    CAST(stop_id AS STRING) AS stop_id,
    perceived_race,
    COALESCE(stop_reason, 'UNKNOWN') AS stop_reason
  FROM base
  WHERE stop_result LIKE '%contraband%' OR stop_result LIKE '%evidence%'
),

-- Counts by group
search_counts AS (
  SELECT
    perceived_race,
    stop_reason,
    COUNT(DISTINCT stop_id) AS searches
  FROM searched
  GROUP BY perceived_race, stop_reason
),

find_counts AS (
  SELECT
    perceived_race,
    stop_reason,
    COUNT(DISTINCT stop_id) AS finds
  FROM found
  GROUP BY perceived_race, stop_reason
)

SELECT
  sc.perceived_race,
  sc.stop_reason,
  sc.searches,
  IFNULL(fc.finds, 0) AS finds,
  SAFE_DIVIDE(CAST(IFNULL(fc.finds, 0) AS FLOAT64), CAST(sc.searches AS FLOAT64)) AS yield_rate
FROM search_counts sc
LEFT JOIN find_counts fc
  ON sc.perceived_race = fc.perceived_race
 AND sc.stop_reason   = fc.stop_reason
ORDER BY sc.perceived_race, sc.stop_reason;
