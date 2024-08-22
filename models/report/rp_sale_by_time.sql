WITH RevenueData AS (
    SELECT
        dt.year,
        dt.month,
        SUM(s.total_price) AS total_revenue
    FROM {{ ref('fact_checkout_success') }} AS s
    LEFT JOIN {{ ref('dim_timestamp') }} AS dt
        ON dt.date_key = s.date_key
    GROUP BY dt.year, dt.month
)

SELECT
    year,
    month,
    total_revenue,
    RANK() OVER (PARTITION BY year ORDER BY total_revenue DESC) AS revenue_rank
FROM RevenueData
