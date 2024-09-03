-- Extract raw data specific to 'checkout_success' collection
WITH raw_checkout_success AS (
    SELECT
        time_stamp,
        ip,
        cart_products.*
    FROM {{ source('glamira_raw', 'summary') }},
    UNNEST(cart_products) AS cart_products
    WHERE collection = 'checkout_success'
),

-- Transform data by extracting key product details and handling price formatting
transformed_data AS (
    SELECT
        time_stamp,
        ip,
        product_id,
        -- Handle various price formats and convert to FLOAT64
        CASE 
            WHEN REGEXP_CONTAINS(price, r'^[0-9]{1,3}(,[0-9]{3})*\.[0-9]{2}$') 
                THEN CAST(REPLACE(price, ',', '') AS FLOAT64)
            ELSE CAST(REGEXP_REPLACE(REPLACE(REPLACE(price, "'", ''), '.', ''), r"[٫,]", '.') AS FLOAT64)
        END AS price,
        amount,
        currency,
        -- Extract alloy and diamond values from options
        MAX(CASE WHEN option.option_label = 'alloy' THEN option.value_label END) AS alloy_value,
        MAX(CASE WHEN option.option_label = 'diamond' THEN option.value_label END) AS diamond_value
    FROM raw_checkout_success,
    UNNEST(option) AS option
    GROUP BY time_stamp, ip, product_id, price, amount, currency
),

-- Format alloy names for consistency and readability
formatted_alloy_names AS (
    SELECT
        EXTRACT(DATE FROM TIMESTAMP_SECONDS(time_stamp)) AS date_time,
        ip,
        product_id,
        diamond_value,
        price,
        amount,
        currency,
        -- Format alloy names: remove separators, handle numbers, and standardize names
        COALESCE(
            CASE
                WHEN REGEXP_CONTAINS(alloy_value, r'[0-9]') THEN
                    CONCAT(
                        REGEXP_REPLACE(
                            SUBSTR(
                                alloy_value,
                                1,
                                GREATEST(1, REGEXP_INSTR(alloy_value, r'[0-9]') - 2)
                            ),
                            r'[_-]', ' '
                        ),
                        ' ',
                        REGEXP_EXTRACT(alloy_value, r'[0-9]+')
                    )
                WHEN alloy_value = '' THEN NULL
                ELSE REGEXP_REPLACE(alloy_value, r'[_-]', ' ')
            END,
        'unknown') AS alloy_value
    FROM transformed_data
),

-- Generate surrogate keys for primary fact table
generated_keys AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['date_time']) }} AS date_key,  -- Generate key based on date
        {{ dbt_utils.generate_surrogate_key(['ip']) }} AS location_key,    -- Generate key based on IP
        product_id AS product_key,
        {{ dbt_utils.generate_surrogate_key(['alloy_value']) }} AS alloy_key,
        {{ dbt_utils.generate_surrogate_key(['diamond_value']) }} AS diamond_key,
        price,
        amount,
        currency,
        -- Join with currency exchange rates and handle missing rates by defaulting to 1
        COALESCE(cer.exchange_rate_to_usd, 1) AS exchange_rate_to_usd
    FROM formatted_alloy_names
    LEFT JOIN {{ source('glamira_raw', 'currency_exchange') }} AS cer
        ON currency = cer.currency_code
)

-- Final selection with total price calculation
SELECT
    gk.date_key, 
    gk.location_key,
    gk.product_key,
    gk.alloy_key,
    gk.diamond_key,
    gk.price,
    gk.amount,
    gk.currency,
    gk.exchange_rate_to_usd,
    ROUND(gk.price * gk.amount * gk.exchange_rate_to_usd, 2) AS total_price  -- Calculate total price with exchange rate
FROM generated_keys AS gk
