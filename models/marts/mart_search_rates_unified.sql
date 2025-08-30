{{ config(
    materialized = 'view'        -- switch to 'table' later if you want faster Tableau reads
) }}

-- Unified dashboard mart: race x month (+ gender, age bucket, city, assignment)
-- Metrics: total_stops, searched_stops, search_rate, finds, yield_rate

WITH base_raw AS (
  SELECT
    -- keys / time
    CAST(stop_id AS INT64) AS stop_id,
    DATE_TRUNC(stop_date, MONTH) AS stop_month,

    -- dimensions (pulled from the intermediate model)
    r.perceived_race,
    s.perceived_gender,
    -- bucket age for cleaner slicing (NULL-safe)
    CASE
      WHEN SAFE_CAST(s.perceived_age AS INT64) IS NULL THEN NULL
      WHEN SAFE_CAST(s.perceived_age AS INT64) < 18 THEN '<18'
      WHEN SAFE_CAST(s.perceived_age AS INT64) BETWEEN 18 AND 29 THEN '18–29'
      WHEN SAFE_CAST(s.perceived_age AS INT64) BETWEEN 30 AND 44 THEN '30–44'
      WHEN SAFE_CAST(s.perceived_age AS INT64) BETWEEN 45 AND 64 THEN '45–64'
      WHEN SAFE_CAST(s.perceived_age AS INT64) >= 65 THEN '65+'
    END AS age_bucket,
    s.city_name,
    s.assignment,

    -- flag whether this stop had any search recorded on this row
    CASE WHEN sb.search_basis IS NOT NULL THEN 1 ELSE 0 END AS row_has_search

  FROM {{ ref('int_search_outcomes') }} AS s
  LEFT JOIN {{ ref('int_search_outcomes') }} AS r  ON r.stop_id = s.stop_id  -- reuse same rows; r.* fields already present in s
  LEFT JOIN {{ ref('int_search_outcomes') }} AS sb ON sb.stop_id = s.stop_id -- same table; we only need search_basis presence
  -- NOTE: int_search_outcomes already contains race and search_basis columns.
  -- The self-joins above make the intent explicit for readability, but are not strictly required.
),

-- Collapse to one row per stop x dimension combo with a binary "had_search" flag
base AS (
  SELECT
    stop_id,
    stop_month,
    perceived_race,
    perceived_gender,
    age_bucket,
    city_name,
    assignment,
    MAX(row_has_search) AS had_search
  FROM base_raw
  GROUP BY
    stop_id, stop_month, perceived_race, perceived_gender, age_bucket, city_name, assignment
),

-- Distinct stops that yielded contraband/evidence
finds AS (
  SELECT DISTINCT
    CAST(stop_id AS INT64) AS stop_id
  FROM {{ ref('stg_ripa_contraband_evid') }}
),

agg AS (
  SELECT
    b.stop_month,
    b.perceived_race,
    b.perceived_gender,
    b.age_bucket,
    b.city_name,
    b.assignment,

    COUNT(DISTINCT b.stop_id) AS total_stops,
    COUNT(DISTINCT IF(b.had_search = 1, b.stop_id, NULL)) AS searched_stops,
    COUNT(DISTINCT IF(f.stop_id IS NOT NULL, b.stop_id, NULL)) AS finds

  FROM base b
  LEFT JOIN finds f USING (stop_id)
  GROUP BY
    b.stop_month, b.perceived_race, b.perceived_gender, b.age_bucket, b.city_name, b.assignment
)

SELECT
  stop_month,
  perceived_race,
  perceived_gender,
  age_bucket,
  city_name,
  assignment,

  total_stops,
  searched_stops,
  SAFE_DIVIDE(searched_stops, total_stops) AS search_rate,
  finds,
  SAFE_DIVIDE(finds, searched_stops)      AS yield_rate
FROM agg
ORDER BY stop_month, perceived_race, perceived_gender, age_bucket, city_name, assignment;
