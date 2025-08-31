{{ config(materialized='view') }}

WITH base_raw AS (
  SELECT
    CAST(stop_id AS INT64) AS stop_id,
    perceived_race,
    DATE_TRUNC(stop_date, MONTH) AS stop_month,
    CASE WHEN search_basis IS NOT NULL THEN 1 ELSE 0 END AS row_has_search
  FROM {{ ref('int_search_outcomes') }}
),

base AS (
  SELECT
    stop_id,
    perceived_race,
    stop_month,
    MAX(row_has_search) AS had_search
  FROM base_raw
  GROUP BY stop_id, perceived_race, stop_month
),

finds AS (
  SELECT DISTINCT
    CAST(stop_id AS INT64) AS stop_id
  FROM {{ ref('stg_ripa_contraband_evid') }}
),

agg AS (
  SELECT
    b.stop_month,
    b.perceived_race,
    COUNT(DISTINCT b.stop_id) AS total_stops,
    COUNT(DISTINCT IF(b.had_search = 1, b.stop_id, NULL)) AS searched_stops,
    COUNT(DISTINCT IF(f.stop_id IS NOT NULL, b.stop_id, NULL)) AS finds
  FROM base b
  LEFT JOIN finds f USING (stop_id)
  GROUP BY 1, 2
)

SELECT
  stop_month,
  perceived_race,
  total_stops,
  searched_stops,
  SAFE_DIVIDE(searched_stops, total_stops) AS search_rate,
  finds,
  SAFE_DIVIDE(finds, searched_stops) AS yield_rate
FROM agg
ORDER BY stop_month, perceived_race;
