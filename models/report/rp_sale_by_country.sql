SELECT
    l.country_name
    ,COUNT(l.country_name) AS num_product
    ,ROUND(SUM(total_price),2) AS total_revenue
FROM {{ref('fact_checkout_success')}} AS s
LEFT JOIN {{ref('dim_location')}} AS l
    ON l.location_key=s.location_key
GROUP BY l.country_name