-- CTE to collect all unique product IDs from both the 'summary' and 'cart_products' arrays
WITH product_cte AS (
    SELECT DISTINCT
        product_id
    FROM {{ source('glamira_raw', 'summary') }}
    WHERE product_id IS NOT NULL

    UNION ALL

    SELECT DISTINCT
        cart_products.product_id AS product_id
    FROM {{ source('glamira_raw', 'summary') }},
    UNNEST(cart_products) AS cart_products
    WHERE cart_products.product_id IS NOT NULL
),

-- Remove duplicate product IDs
product_distinct AS (
    SELECT DISTINCT
        product_id
    FROM product_cte
)

-- Final selection with LEFT JOIN to enrich product details
SELECT
    pd.product_id,
    COALESCE(p.item_name, 'Unknown') AS product_name,  -- Use COALESCE for fallback values
    COALESCE(p.price, -1) AS product_price,           -- Default price to -1 if NULL
    p.item_category AS product_category,
    p.item_category2 AS product_category_2,
    p.item_category3 AS product_category_3,
    p.item_category4 AS product_category_4,
    p.item_category5 AS product_category_5,
    p.item_category6 AS product_category_6,
    COALESCE(p.dimension5, 'Not in stock') AS is_available  -- Default availability status
FROM product_distinct AS pd
LEFT JOIN {{ source('glamira_raw', 'product') }} AS p
    ON pd.product_id = p.item_id;  -- Join on product ID to enrich product details