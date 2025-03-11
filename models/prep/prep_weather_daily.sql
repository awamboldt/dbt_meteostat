WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
		, DATE_PART('day', date) AS date_day 		-- number of the day of month
		, DATE_PART('month', date) AS date_month 	-- number of the month of year
		, DATE_PART('year', date) AS date_year 		-- number of year
		, EXTRACT(WEEK FROM date) AS cw 			-- number of the week of year
		, TO_CHAR(date, 'FMmonth') AS month_name	-- name of the month
		, TO_CHAR(date, 'FMday') AS weekday 		-- name of the weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN date_month in (2, 1, 12) THEN 'winter'
			WHEN date_month IN (5, 3, 4) THEN 'spring'
            WHEN date_month IN (8, 6, 7) THEN 'summer'
            WHEN date_month IN (11, 9, 10) THEN 'autumn'
		END) AS season
    FROM add_features)
SELECT *
FROM add_more_features