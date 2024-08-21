SELECT
    CONCAT(
        COALESCE(p.product_category, 'Unknown'), 
        ' > ',
        COALESCE(p.product_category_2, 'Unknown'), 
        ' > ',
        COALESCE(p.product_category_3, 'Unknown'), 
        ' > ',
        COALESCE(p.product_category_4, 'Unknown'), 
        ' > ',
        COALESCE(p.product_category_5, 'Unknown'), 
        ' > ',
        COALESCE(p.product_category_6, 'Unknown')
    ) AS category_hierarchy,
    SUM(s.total_price) AS total_revenue
FROM {{ ref('fact_checkout_success') }} AS s
LEFT JOIN {{ ref('dim_product') }} AS p 
    ON p.product_id = s.product_key
GROUP BY category_hierarchy
ORDER BY total_revenue DESC;
