WITH departure_data AS
	(SELECT 
		origin
		, flight_date
		--unique number of departures connections
		, COUNT(DISTINCT dest) AS departure_connections
		--how many flight were planned in total (departures)
		, COUNT(flight_number) AS departures
		--how many flights were canceled in total (departures)
		, COUNT(cancelled) AS departure_cancellations
		--how many flights were diverted in total (departures)
		, COUNT(diverted) AS departure_diverted
		--(optional) how many unique airplanes travelled on average
		, COUNT(DISTINCT tail_number) AS unique_departure_planes
        --(optional) how many unique airlines
		, COUNT(DISTINCT airline) AS departure_airlines
	FROM {{ref('prep_flights')}}
	WHERE origin IN ('LAX', 'JFK', 'MIA')
	GROUP BY origin, flight_date),
actually_departed AS
	(SELECT 
		origin
		, flight_date
		, COUNT(*) AS actual_departed
	FROM {{ref('prep_flights')}}
	WHERE (cancelled = 0) AND actual_elapsed_time IS NOT NULL AND origin IN ('LAX', 'JFK', 'MIA')
	GROUP BY origin, flight_date),
arrival_data AS
	(SELECT 
		dest
		, flight_date
		--unique number of arrival connections
		, COUNT(DISTINCT origin) AS arrival_connections
		--how many flight were planned in total (arrivals)
		, COUNT(flight_number) AS arrivals
		--how many flights were canceled in total (departures & arrivals)
		, COUNT(cancelled) AS arrival_cancellations
		--how many flights were diverted in total (departures & arrivals)
		, COUNT(diverted) AS arrival_diverted
		--(optional) how many unique airplanes travelled on average
		, COUNT(DISTINCT tail_number) AS unique_arrival_planes
        --(optional) how many unique airlines
		, COUNT(DISTINCT airline) AS arrival_airlines
	FROM {{ref('prep_flights')}}
	WHERE dest IN ('LAX', 'JFK', 'MIA')
	GROUP BY dest, flight_date),
actually_arrived AS
	(SELECT 
		dest
		, flight_date
		, COUNT(*) AS actual_arrived
	FROM {{ref('prep_flights')}}
	WHERE (cancelled = 0) AND actual_elapsed_time IS NOT NULL AND dest IN ('LAX', 'JFK', 'MIA')
	GROUP BY dest, flight_date),
merged_data AS
	(SELECT 
		d.origin AS airport
		, d.flight_date AS date
		, departure_connections
		, arrival_connections
		--how many flight were planned in total (departures & arrivals)
		, (departures+arrivals) AS total_planned_flights
		--how many flights were canceled in total (departures & arrivals)
		, (departure_cancellations + arrival_cancellations) AS total_cancellations
		--how many flights were diverted in total (departures & arrivals)
		, (departure_diverted + arrival_diverted) AS total_diversions
		--how many flights actually occured in total (departures & arrivals)
		, (da.actual_departed + aa.actual_arrived) AS total_actual_flights
	    --(optional) how many unique airplanes travelled on average
	    , (d.unique_departure_planes + a.unique_arrival_planes)::NUMERIC/2 AS unique_planes
	    --(optional) how many unique airlines
	    , (d.departure_airlines+a.arrival_airlines)::NUMERIC/2 AS unique_airlines
	FROM departure_data AS d
	LEFT JOIN actually_departed AS da ON (da.origin=d.origin AND da.flight_date=d.flight_date)
	LEFT JOIN arrival_data AS a ON (a.dest=d.origin and a.flight_date=d.flight_date)
	LEFT JOIN actually_arrived AS aa ON (aa.dest=d.origin AND aa.flight_date = d.flight_date)),
geo_data AS
	(SELECT 
	pa.name
	,pa.city
	,md.*
	,pa.country
	FROM merged_data AS md
	LEFT JOIN {{ref('prep_airports')}} AS pa ON pa.faa = md.airport),
weather_data AS 
	(SELECT
	airport_code
	, date
	--daily min temperature
	, min_temp_c
	--daily max temperature
	, max_temp_c
	--daily precipitation
	, precipitation_mm
	--daily snow fall
	, max_snow_mm
	--daily average wind direction
	, avg_wind_direction
	--daily average wind speed
	, avg_wind_speed_kmh
	--daily wnd peakgust
	, wind_peakgust_kmh
	FROM {{ref('prep_weather_daily')}})
SELECT
	--only the airports we collected the weather data for
	g.airport
	, w.date
	--(optional) add city, country and name of the airport
	, g.name
	, g.city
	--unique number of departures connections
	, g.departure_connections
	--unique number of arrival connections
	, g.arrival_connections
	--how many flight were planned in total (departures & arrivals)
	, g.total_planned_flights
	--how many flights were canceled in total (departures & arrivals)
	, g.total_cancellations
	--how many flights were diverted in total (departures & arrivals)
	, g.total_diversions
	--how many flights actually occured in total (departures & arrivals)
	, g.total_actual_flights
	--(optional) how many unique airplanes travelled on average
	, g.unique_planes
    --(optional) how many unique airlines
	, g.unique_airlines
	--daily min temperature
	, w.min_temp_c
	--daily max temperature
	, w.max_temp_c
	--daily precipitation
	, w.precipitation_mm
	--daily snow fall
	, w.max_snow_mm
	--daily average wind direction
	, w.avg_wind_direction
	--daily average wind speed
	, w.avg_wind_speed_kmh
	--daily wnd peakgust
	, w.wind_peakgust_kmh
FROM weather_data AS w
JOIN geo_data AS g ON (w.airport_code = g.airport AND w.date=g.date)