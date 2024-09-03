-- Extract distinct alloy values from the source table
WITH alloy_cte AS (
    SELECT DISTINCT
        -- Use COALESCE to handle NULLs and provide a fallback
        COALESCE(option.value_label, option.alloy, 'unknown') AS alloy_value
    FROM {{ source('glamira_raw', 'summary') }} AS summary
    LEFT JOIN UNNEST(option) AS option  -- UNNEST 'option' array
    ON option.option_label = 'alloy' OR option.option_label IS NULL

    UNION DISTINCT

    SELECT DISTINCT
        COALESCE(option.value_label, 'unknown') AS alloy_value
    FROM {{ source('glamira_raw', 'summary') }} AS summary
    LEFT JOIN UNNEST(cart_products) AS cart_products  -- UNNEST 'cart_products' array
    LEFT JOIN UNNEST(cart_products.option) AS option  -- UNNEST 'option' array within 'cart_products'
    ON option.option_label = 'alloy'
),

-- Format alloy names: clean and structure them for consistency
format_name AS (
    SELECT DISTINCT
        COALESCE(
            CASE
                -- Format alloy values containing numbers, splitting at the first occurrence of a number
                WHEN REGEXP_CONTAINS(alloy_value, r'[0-9]') THEN
                    CONCAT(
                        REGEXP_REPLACE(
                            SUBSTR(
                                alloy_value,
                                1,
                                GREATEST(1, REGEXP_INSTR(alloy_value, r'[0-9]') - 2)
                            ),
                            r'[_-]', ' '  -- Replace underscores and hyphens with spaces
                        ),
                        ' ',
                        REGEXP_EXTRACT(alloy_value, r'[0-9]+')  -- Extract the numeric part
                    )
                -- Handle empty strings explicitly
                WHEN alloy_value = '' THEN NULL
                -- Clean non-numeric alloy values by replacing underscores and hyphens with spaces
                ELSE REGEXP_REPLACE(alloy_value, r'[_-]', ' ')
            END,
        'unknown') AS alloy_name  -- Default to 'unknown' if the alloy name is NULL
    FROM alloy_cte
)

-- Final selection: generate surrogate keys for each formatted alloy name
SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['alloy_name']) }} AS alloy_key,  -- Generate a unique key using dbt-utils
    alloy_name
FROM format_name;