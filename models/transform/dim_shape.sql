WITH shape_cte AS (
  SELECT DISTINCT
    option.shapediamond AS shape
  FROM {{ source('glamira_raw', 'summary') }},
  UNNEST(option) AS option
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['shape']) }} AS shape_key,
    shape
FROM shape_cte
