-- CTE to collect distinct timestamps from the 'summary' table
WITH date_cte AS (
    SELECT DISTINCT
        time_stamp
    FROM {{ source('glamira_raw', 'summary') }}
)

-- Final selection with extracted date components and formatted fields
SELECT
    -- Generate a unique surrogate key based on the date extracted from the timestamp
    {{ dbt_utils.generate_surrogate_key([
        'EXTRACT(DATE FROM TIMESTAMP_SECONDS(time_stamp))'
    ]) }} AS date_key, 

    -- Extract the date from the timestamp
    EXTRACT(DATE FROM TIMESTAMP_SECONDS(time_stamp)) AS date_time,
    
    -- Original timestamp for reference
    time_stamp,
    
    -- Additional date components for analytics and reporting
    EXTRACT(DATE FROM TIMESTAMP_SECONDS(time_stamp)) AS full_date,
    EXTRACT(YEAR FROM TIMESTAMP_SECONDS(time_stamp)) AS year,
    EXTRACT(MONTH FROM TIMESTAMP_SECONDS(time_stamp)) AS month,
    EXTRACT(QUARTER FROM TIMESTAMP_SECONDS(time_stamp)) AS quarter,
    EXTRACT(DAY FROM TIMESTAMP_SECONDS(time_stamp)) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(time_stamp)) AS day_of_week,
    
    -- Day of the week as a string (e.g., 'Monday')
    FORMAT_TIMESTAMP('%A', TIMESTAMP_SECONDS(time_stamp)) AS day_of_week_string,
    
    -- Shortened day of the week (e.g., 'Mon')
    LEFT(FORMAT_TIMESTAMP('%A', TIMESTAMP_SECONDS(time_stamp)), 3) AS day_of_week_short,

    -- Categorize the day as 'Weekday' or 'Weekend'
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM TIMESTAMP_SECONDS(time_stamp)) BETWEEN 2 AND 6 THEN 'Weekday'
        ELSE 'Weekend'
    END AS is_weekday_or_weekend

FROM date_cte
