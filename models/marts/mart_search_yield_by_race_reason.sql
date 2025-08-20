{{ config(materialized = 'table') }}

SELECT
  sc.perceived_race,
  sc.stop_reason,
  sc.searches,
  IFNULL(fc.finds, 0) AS finds,
  SAFE_DIVIDE(CAST(IFNULL(fc.finds, 0) AS FLOAT64), CAST(sc.searches AS FLOAT64)) AS yield_rate
FROM (
  SELECT
    COALESCE(perceived_race, 'UNKNOWN') AS perceived_race,
    COALESCE(NULLIF(TRIM(stop_reason), ''), 'UNKNOWN') AS stop_reason,
    COUNT(DISTINCT CAST(stop_id AS STRING)) AS searches
  FROM {{ ref('int_search_outcomes') }}
  WHERE NULLIF(TRIM(search_basis), '') IS NOT NULL
  GROUP BY perceived_race, stop_reason
) AS sc
LEFT JOIN (
  SELECT
    COALESCE(perceived_race, 'UNKNOWN') AS perceived_race,
    COALESCE(NULLIF(TRIM(stop_reason), ''), 'UNKNOWN') AS stop_reason,
    COUNT(DISTINCT CAST(stop_id AS STRING)) AS finds
  FROM {{ ref('int_search_outcomes') }}
  WHERE LOWER(COALESCE(stop_result, '')) LIKE '%contraband%'
     OR LOWER(COALESCE(stop_result, '')) LIKE '%evidence%'
  GROUP BY perceived_race, stop_reason
) AS fc
USING (perceived_race, stop_reason)
ORDER BY perceived_race, stop_reason
