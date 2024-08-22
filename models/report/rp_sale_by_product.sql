WITH RevenueData AS (
    SELECT
        p.product_name,
        SUM(s.total_price) AS total_revenue,
        COUNT(s.product_key) AS products_sold
    FROM {{ref('fact_checkout_success')}} AS s
    LEFT JOIN {{ref('dim_product')}} AS p 
        ON p.product_id = s.product_key
    GROUP BY p.product_name
)

SELECT
    product_name,
    total_revenue,
    products_sold,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM RevenueData
ORDER BY revenue_rank
