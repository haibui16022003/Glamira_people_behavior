-- Aggregate revenue by year and month
WITH RevenueData AS (
    SELECT
        dt.year,
        dt.month,
        SUM(s.total_price) AS total_revenue  -- Sum of total revenue for each month
    FROM {{ ref('fact_checkout_success') }} AS s
    LEFT JOIN {{ ref('dim_timestamp') }} AS dt
        ON dt.date_key = s.date_key
    GROUP BY dt.year, dt.month
)

-- Rank months by revenue within each year
SELECT
    year,
    month,
    total_revenue,
    RANK() OVER (PARTITION BY year ORDER BY total_revenue DESC) AS revenue_rank  -- Rank months within each year
FROM RevenueData
ORDER BY year, revenue_rank;  -- Order results by year and rank