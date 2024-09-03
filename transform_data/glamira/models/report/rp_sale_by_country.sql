WITH RevenueData AS (
    SELECT
        l.country_long,
        COUNT(s.total_price) AS sales_count,
        SUM(s.total_price) AS total_revenue
    FROM {{ ref('fact_checkout_success') }} AS s
    LEFT JOIN {{ ref('dim_location') }} AS l
        ON l.location_key = s.location_key
    GROUP BY l.country_long
)

SELECT
    country_long,
    sales_count,
    total_revenue,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM RevenueData
ORDER BY revenue_rank
