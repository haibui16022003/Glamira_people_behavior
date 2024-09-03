-- CTE to create a unified table of products and their options
WITH original_table AS (
    -- Extract data from cart products and options
    SELECT
        s.time_stamp,
        s.ip,
        cart_products.product_id,
        cart_products.amount,
        cart_products.currency,
        cart_products.price,
        option.option_label,
        option.value_label,
        NULL AS alloy,               -- Initialize alloy-related fields as NULL
        NULL AS diamond,
        NULL AS pearlcolor,
        NULL AS stone,
        s.collection
    FROM {{ source('glamira_raw', 'summary') }} AS s,
    UNNEST(cart_products) AS cart_products,
    UNNEST(cart_products.option) AS option

    UNION DISTINCT

    -- Extract data directly from options without cart products
    SELECT
        s.time_stamp,
        s.ip,
        s.product_id,
        0 AS amount,                -- Default amount when directly from options
        s.currency,
        "0.00" AS price,            -- Default price for non-cart products
        option.option_label,
        option.value_label,
        option.alloy,
        option.diamond,
        option.pearlcolor,
        option.stone,
        s.collection
    FROM {{ source('glamira_raw', 'summary') }} AS s,
    UNNEST(option) AS option
),

-- Transforming and cleaning the product and price data
transform AS (
    SELECT
        time_stamp,
        ip,
        product_id,
        -- Handling different price formats and converting to float
        CASE 
            WHEN REGEXP_CONTAINS(price, r'^[0-9]{1,3}(,[0-9]{3})*\.[0-9]{2}$') 
                THEN CAST(REPLACE(price, ',', '') AS FLOAT64)  -- Standard price format
            ELSE 
                CAST(REGEXP_REPLACE(REPLACE(REPLACE(price, "'", ''), '.', ''), r"[٫,]", '.') AS FLOAT64)  -- Handle irregular formats
        END AS price,
        amount,
        currency,
        collection,
        -- Aggregating option values
        MAX(CASE WHEN option_label = 'alloy' THEN value_label ELSE alloy END) AS alloy_value,
        MAX(CASE WHEN option_label = 'diamond' THEN value_label ELSE diamond END) AS diamond_value,
        MAX(CASE WHEN option_label = 'pearl' THEN value_label ELSE pearlcolor END) AS pearl_value,
        MAX(CASE WHEN option_label LIKE 'stone%' THEN value_label ELSE stone END) AS stone_value
    FROM original_table
    GROUP BY time_stamp, ip, product_id, price, amount, currency, collection
),

-- Formatting alloy names for consistency
format_name AS (
    SELECT
        time_stamp,
        ip,
        product_id,
        -- Formatting alloy names: adding spaces, handling numbers, and standardizing separators
        COALESCE(
            CASE
                WHEN REGEXP_CONTAINS(alloy_value, r'[0-9]') THEN
                    CONCAT(
                        REGEXP_REPLACE(
                            SUBSTR(alloy_value, 1, GREATEST(1, REGEXP_INSTR(alloy_value, r'[0-9]') - 2)),
                            r'[_-]', ' '
                        ),
                        ' ',
                        REGEXP_EXTRACT(alloy_value, r'[0-9]+')
                    )
                WHEN alloy_value = '' THEN NULL
                ELSE REGEXP_REPLACE(alloy_value, r'[_-]', ' ')
            END,
        'unknown') AS alloy_value,
        diamond_value,
        pearl_value,
        stone_value,
        price,
        amount,
        currency,
        collection
    FROM transform
),

-- Generating keys for fact tables and analytics
genkey AS (
    SELECT
        EXTRACT(DATE FROM TIMESTAMP_SECONDS(time_stamp)) AS date_key,
        {{ dbt_utils.generate_surrogate_key(['ip']) }} AS location_key,
        product_id AS product_key,
        {{ dbt_utils.generate_surrogate_key(['alloy_value']) }} AS alloy_key,
        {{ dbt_utils.generate_surrogate_key(['diamond_value']) }} AS diamond_key,
        {{ dbt_utils.generate_surrogate_key(['pearl_value']) }} AS pearl_key,
        {{ dbt_utils.generate_surrogate_key(['stone_value']) }} AS stone_key,
        price,
        amount,
        currency,
        collection
    FROM format_name
)

-- Final selection of data with calculated total price
SELECT
    gk.*, 
    ROUND(price * amount, 2) AS total_price   -- Calculate the total price based on price and amount
FROM genkey AS gk
