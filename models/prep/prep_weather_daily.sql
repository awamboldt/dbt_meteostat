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
		, EXTRACT(MONTH FROM date) AS month_code 	-- name of the month
		, EXTRACT(DOW FROM date) AS weekday 		-- name of the weekday
    FROM daily_data 
),
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN month_code in (0, 1, 11) THEN 'winter'
			WHEN month_code IN (2, 3, 4) THEN 'spring'
            WHEN month_code IN (5, 6, 7) THEN 'summer'
            WHEN month_code IN (8, 9, 10) THEN 'autumn'
		END) AS season
		, (CASE 
			WHEN month_code IN (0) THEN 'january'
			WHEN month_code IN (1) THEN 'february'
            WHEN month_code IN (2) THEN 'march'
            WHEN month_code IN (3) THEN 'april'
            WHEN month_code IN (4) THEN 'may'
			WHEN month_code IN (5) THEN 'june'
            WHEN month_code IN (6) THEN 'july'
            WHEN month_code IN (7) THEN 'august'
            WHEN month_code IN (8) THEN 'september'
			WHEN month_code IN (9) THEN 'october'
            WHEN month_code IN (10) THEN 'november'
            WHEN month_code IN (11) THEN 'december'
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
    FROM add_features)
SELECT *
FROM add_more_features
ORDER BY date