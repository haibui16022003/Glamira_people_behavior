WITH category_hierarchy AS (
    SELECT
        p.product_category AS category_name,
        s.total_price
    FROM {{ ref('fact_checkout_success') }} AS s
    LEFT JOIN {{ ref('dim_product') }} AS p 
        ON p.product_id = s.product_key

    UNION ALL

    SELECT
        p.product_category_2 AS category_name,
        s.total_price
    FROM {{ ref('fact_checkout_success') }} AS s
    LEFT JOIN {{ ref('dim_product') }} AS p 
        ON p.product_id = s.product_key

    UNION ALL

    SELECT
        p.product_category_3 AS category_name,
        s.total_price
    FROM {{ ref('fact_checkout_success') }} AS s
    LEFT JOIN {{ ref('dim_product') }} AS p 
        ON p.product_id = s.product_key

    UNION ALL

    SELECT
        p.product_category_4 AS category_name,
        s.total_price
    FROM {{ ref('fact_checkout_success') }} AS s
    LEFT JOIN {{ ref('dim_product') }} AS p 
        ON p.product_id = s.product_key

    UNION ALL

    SELECT
        p.product_category_5 AS category_name,
        s.total_price
    FROM {{ ref('fact_checkout_success') }} AS s
    LEFT JOIN {{ ref('dim_product') }} AS p 
        ON p.product_id = s.product_key

    UNION ALL

    SELECT
        p.product_category_6 AS category_name,
        s.total_price
    FROM {{ ref('fact_checkout_success') }} AS s
    LEFT JOIN {{ ref('dim_product') }} AS p 
        ON p.product_id = s.product_key
),
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

SELECT *
FROM category_revenue
ORDER BY total_revenue DESC
