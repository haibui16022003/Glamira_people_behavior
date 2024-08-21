WITH category_union AS (
    SELECT item_category AS category_name
    FROM {{ source('glamira_raw', 'product') }}
    WHERE item_category IS NOT NULL

    UNION ALL

    SELECT item_category2 AS category_name
    FROM {{ source('glamira_raw', 'product') }}
    WHERE item_category2 IS NOT NULL

    UNION ALL

    SELECT item_category3 AS category_name
    FROM {{ source('glamira_raw', 'product') }}
    WHERE item_category3 IS NOT NULL

    UNION ALL

    SELECT item_category4 AS category_name
    FROM {{ source('glamira_raw', 'product') }}
    WHERE item_category4 IS NOT NULL

    UNION ALL

    SELECT item_category5 AS category_name
    FROM {{ source('glamira_raw', 'product') }}
    WHERE item_category5 IS NOT NULL

    UNION ALL

    SELECT item_category6 AS category_name
    FROM {{ source('glamira_raw', 'product') }}
    WHERE item_category6 IS NOT NULL
)

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['category_name']) }} AS category_id,
    category_name
FROM category_union