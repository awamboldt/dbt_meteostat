SELECT
	airport_code
	, station_id
	, date_year
    , season
	, AVG(avg_temp_c) AS weekly_avg_temp
	, MIN(min_temp_c) AS weekly_min_temp
	, MAX(max_temp_c) AS weekly_max_temp
	, SUM(precipitation_mm) AS weekly_sum_precipitation_mm
	, SUM(max_snow_mm) AS weekly_sum_max_snow_mm
	, AVG(avg_wind_direction) AS weekly_avg_wind_direction
	, AVG(avg_wind_speed_kmh) AS weekly_avg_wind_speed_kmh
	, MAX(wind_peakgust_kmh) AS weekly_max_wind_peakgust_kmh
	, AVG(avg_pressure_hpa) AS weekly_avg_pressure_hpa
	, SUM(sun_minutes) AS weekly_total_sun_minutes
	, cw AS current_week
    , MODE() WITHIN GROUP (ORDER BY date_month) AS month
    , MODE() WITHIN GROUP (ORDER BY month_name) AS month_name
    , MODE() WITHIN GROUP (ORDER BY season) AS season
FROM {{ref('prep_weather_daily')}}
GROUP BY (cw, date_year, airport_code, station_id, season)