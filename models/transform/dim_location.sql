SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['ip']) }} AS location_key,
    ip,
    CASE WHEN country_short = '-' THEN 'Unknown' ELSE country_short END AS country_short,
    CASE WHEN country_long = '-' THEN 'Unknown' ELSE country_long END AS country_long,
    CASE WHEN region = '-' THEN 'Unknown' ELSE region END AS region,
    CASE WHEN city = '-' THEN 'Unknown' ELSE city END AS city,
    CASE WHEN zipcode = '-' THEN 'Unknown' ELSE zipcode END AS zip_code
FROM {{source('glamira_raw', 'location')}}
WHERE ip IS NOT NULL
