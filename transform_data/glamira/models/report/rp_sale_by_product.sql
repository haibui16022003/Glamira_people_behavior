-- Aggregate revenue and product sales count by product name
WITH RevenueData AS (
    SELECT
        p.product_name,
        SUM(s.total_price) AS total_revenue,  -- Sum of total revenue for each product
        COUNT(s.product_key) AS products_sold  -- Count of sales transactions for each product
    FROM {{ ref('fact_checkout_success') }} AS s
    LEFT JOIN {{ ref('dim_product') }} AS p 
        ON p.product_id = s.product_key
    GROUP BY p.product_name
)

-- Rank products based on total revenue and order by rank
SELECT
    product_name,
    total_revenue,
    products_sold,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank  -- Rank products by revenue in descending order
FROM RevenueData
ORDER BY revenue_rank;  -- Order by revenue rank for the final output