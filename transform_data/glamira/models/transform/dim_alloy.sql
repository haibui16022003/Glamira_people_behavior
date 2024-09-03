WITH alloy_cte AS (
    SELECT DISTINCT
        CASE
            WHEN option.value_label IS NOT NULL THEN option.value_label
            ELSE option.alloy
        END AS alloy_value
    FROM {{ source('glamira_raw', 'summary') }} as summary,
    UNNEST(option) AS option
    WHERE option.option_label = 'alloy' OR option.option_label IS NULL

    UNION DISTINCT

    SELECT
        COALESCE(option.value_label, 'unknown') AS alloy_value
    FROM {{ source('glamira_raw', 'summary') }} as summary,
    UNNEST(cart_products) AS cart_products,
    UNNEST(cart_products.option) AS option
    WHERE option.option_label = 'alloy'
),
format_name AS (
    SELECT DISTINCT
        COALESCE(
            CASE
                WHEN REGEXP_CONTAINS(alloy_value, r'[0-9]') THEN
                    CONCAT(
                        REGEXP_REPLACE(
                            SUBSTR(
                                alloy_value,
                                1,
                                GREATEST(1, REGEXP_INSTR(alloy_value, r'[0-9]') - 2)
                            ),
                            r'[_-]', ' '
                        ),
                        ' ',
                        REGEXP_EXTRACT(alloy_value, r'[0-9]+')
                    )
                WHEN alloy_value LIKE '' THEN NULL
                ELSE
                    REGEXP_REPLACE(alloy_value, r'[_-]', ' ')
            END,
        'unknown') AS alloy_name
    FROM alloy_cte
)
SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['alloy_name']) }} AS alloy_key,
    alloy_name
FROM format_name
