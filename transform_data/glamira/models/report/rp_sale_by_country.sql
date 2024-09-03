-- Calculate sales count and total revenue by country
WITH RevenueData AS (
    SELECT
        l.country_long,
        COUNT(s.total_price) AS sales_count,  -- Count of sales transactions
        SUM(s.total_price) AS total_revenue    -- Sum of total revenue
    FROM {{ ref('fact_checkout_success') }} AS s
    LEFT JOIN {{ ref('dim_location') }} AS l
        ON l.location_key = s.location_key
    GROUP BY l.country_long
)

-- Rank countries based on total revenue and order by rank
SELECT
    country_long,
    sales_count,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank  -- Rank countries by revenue in descending order
FROM RevenueData
ORDER BY revenue_rank;  -- Order by revenue rank for the final output
