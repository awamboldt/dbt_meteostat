WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
		, timestamp::TIME AS time                           -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
        , EXTRACT(DOW FROM timestamp) AS weekday         -- weekday name as TEXT        
        , DATE_PART('day', timestamp) AS date_day
		, DATE_PART('month', timestamp) AS date_month
		, DATE_PART('year', timestamp) AS date_year
		, EXTRACT(WEEK FROM timestamp) AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
		,(CASE 
			WHEN time BETWEEN '20:01:00' AND '11:59:59' THEN 'night'
			WHEN time BETWEEN '00:00:00' AND '06:00:00' THEN 'night'
			WHEN time BETWEEN '06:01:00' AND '17:00:00' THEN 'day'
			WHEN time BETWEEN '17:01:00' AND '20:00:00' THEN 'evening'
		END) AS day_part
		, (CASE 
			WHEN date_month IN (0) THEN 'january'
			WHEN date_month IN (1) THEN 'february'
            WHEN date_month IN (2) THEN 'march'
            WHEN date_month IN (3) THEN 'april'
            WHEN date_month IN (4) THEN 'may'
			WHEN date_month IN (5) THEN 'june'
            WHEN date_month IN (6) THEN 'july'
            WHEN date_month IN (7) THEN 'august'
            WHEN date_month IN (8) THEN 'september'
			WHEN date_month IN (9) THEN 'october'
            WHEN date_month IN (10) THEN 'november'
            WHEN date_month IN (11) THEN 'december'
		END) AS month_name
		, (CASE 
			WHEN weekday IN (0) THEN 'sunday'
			WHEN weekday IN (1) THEN 'monday'
            WHEN weekday IN (2) THEN 'tuesday'
            WHEN weekday IN (3) THEN 'wednesday'
            WHEN weekday IN (4) THEN 'thursday'
			WHEN weekday IN (5) THEN 'friday'
            WHEN weekday IN (6) THEN 'saturday'
        END) AS day_of_the_week
    FROM add_features
)
SELECT *
FROM add_more_features