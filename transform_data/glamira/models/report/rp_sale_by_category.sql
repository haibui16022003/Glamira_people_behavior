-- Create a hierarchical structure of categories with associated revenues
WITH category_hierarchy AS (
    SELECT
        category_name,
        total_price
    FROM (
        SELECT
            CASE 
                WHEN p.product_category IS NOT NULL THEN p.product_category
                WHEN p.product_category_2 IS NOT NULL THEN p.product_category_2
                WHEN p.product_category_3 IS NOT NULL THEN p.product_category_3
                WHEN p.product_category_4 IS NOT NULL THEN p.product_category_4
                WHEN p.product_category_5 IS NOT NULL THEN p.product_category_5
                WHEN p.product_category_6 IS NOT NULL THEN p.product_category_6
            END AS category_name,
            s.total_price
        FROM {{ ref('fact_checkout_success') }} AS s
        LEFT JOIN {{ ref('dim_product') }} AS p 
            ON p.product_id = s.product_key
    )
    WHERE category_name IS NOT NULL
),

-- Aggregate revenue by category using the hierarchy
category_revenue AS (
    SELECT
        c.category_id,
        c.category_name,
        COALESCE(SUM(ch.total_price), 0) AS total_revenue
    FROM {{ ref('dim_category') }} AS c
    LEFT JOIN category_hierarchy AS ch
        ON c.category_name = ch.category_name
    GROUP BY c.category_id, c.category_name
)

-- Retrieve the ordered list of categories by total revenue
SELECT *
FROM category_revenue
ORDER BY total_revenue DESC;