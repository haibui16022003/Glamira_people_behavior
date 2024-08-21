WITH product_cte AS (
    SELECT product_id
    FROM {{ source('glamira_raw', 'summary') }}
    WHERE product_id IS NOT NULL
    UNION ALL
    SELECT cart_products.product_id AS cart_product_id
    FROM {{ source('glamira_raw', 'summary') }}, 
    UNNEST(cart_products) AS cart_products
    WHERE cart_products.product_id IS NOT NULL
),
product_distinct AS (
    SELECT DISTINCT
        product_id
    FROM product_cte
)

SELECT
    pd.product_id,
    CASE WHEN p.item_name IS NOT NULL THEN p.item_name ELSE 'Unknown' END AS product_name,
    CASE WHEN p.price IS NOT NULL THEN p.price ELSE -1 END AS product_price,
    p.item_category AS product_category,
    p.item_category2 AS product_category_2,
    p.item_category3 AS product_category_3,
    p.item_category4 AS product_category_4,
    p.item_category5 AS product_category_5,
    p.item_category6 AS product_category_6,
    CASE WHEN p.dimension5 IS NOT NULL THEN p.dimension5 ELSE 'Not in stock' END AS is_available,
FROM product_distinct AS pd
LEFT JOIN {{ source('glamira_raw', 'product') }} AS p
ON pd.product_id = p.item_id