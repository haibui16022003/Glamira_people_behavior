-- CTE to gather all categories from multiple item_category fields
WITH category_union AS (
    SELECT DISTINCT category_name
    FROM (
        -- UNNEST categories from different item_category columns using a CROSS JOIN
        SELECT 
            COALESCE(NULLIF(item_category, ''), NULL) AS category_name
        FROM {{ source('glamira_raw', 'product') }}
        
        UNION ALL

        SELECT 
            COALESCE(NULLIF(item_category2, ''), NULL) AS category_name
        FROM {{ source('glamira_raw', 'product') }}
        
        UNION ALL

        SELECT 
            COALESCE(NULLIF(item_category3, ''), NULL) AS category_name
        FROM {{ source('glamira_raw', 'product') }}
        
        UNION ALL

        SELECT 
            COALESCE(NULLIF(item_category4, ''), NULL) AS category_name
        FROM {{ source('glamira_raw', 'product') }}
        
        UNION ALL

        SELECT 
            COALESCE(NULLIF(item_category5, ''), NULL) AS category_name
        FROM {{ source('glamira_raw', 'product') }}
        
        UNION ALL

        SELECT 
            COALESCE(NULLIF(item_category6, ''), NULL) AS category_name
        FROM {{ source('glamira_raw', 'product') }}
    ) 
    WHERE category_name IS NOT NULL -- Filter out NULL values after COALESCE
)

-- Final selection to generate surrogate keys and ensure uniqueness
SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['category_name']) }} AS category_id,  -- Use dbt-utils to generate unique IDs
    category_name
FROM category_union;