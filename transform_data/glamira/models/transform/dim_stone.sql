WITH stone_cte AS (
  SELECT DISTINCT
    CASE
      WHEN option.value_label IS NOT NULL  THEN  option.value_label
      ELSE option.stone
    END AS stone
  FROM {{source('glamira_raw','summary')}},
  UNNEST(option) AS  option
  WHERE option.option_label LIKE "stone%" OR option.option_label IS NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['stone']) }} AS stone_key,
    stone
FROM stone_cte