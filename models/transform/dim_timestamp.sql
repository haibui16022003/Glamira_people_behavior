WITH date_cte AS (
    SELECT DISTINCT
        time_stamp
    FROM {{ source('glamira_raw', 'summary') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['EXTRACT(DATE FROM TIMESTAMP_SECONDS(time_stamp))']) }} AS date_key, 
    EXTRACT(DATE FROM TIMESTAMP_SECONDS(time_stamp)) AS date_time,
    time_stamp,
    EXTRACT(DATE FROM TIMESTAMP_SECONDS(time_stamp)) AS full_date,
    EXTRACT(YEAR FROM TIMESTAMP_SECONDS(time_stamp)) AS year,
    EXTRACT(MONTH FROM TIMESTAMP_SECONDS(time_stamp)) AS month,
    EXTRACT(QUARTER FROM TIMESTAMP_SECONDS(time_stamp)) AS quarter,
    EXTRACT(DAY FROM TIMESTAMP_SECONDS(time_stamp)) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(time_stamp)) AS day_of_week,
    FORMAT_TIMESTAMP('%A', TIMESTAMP_SECONDS(time_stamp)) AS day_of_week_string,
    LEFT(FORMAT_TIMESTAMP('%A', TIMESTAMP_SECONDS(time_stamp)), 3) AS day_of_week_short,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(time_stamp)) BETWEEN 2 AND 6 THEN 'Weekday'
        ELSE 'Weekend'
    END AS is_weekday_or_weekend
FROM date_cte
