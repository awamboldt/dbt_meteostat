WITH departure_data AS
	(SELECT 
		origin
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
	FROM {{ref('prep_flights')}}
	GROUP BY origin),
actually_departed AS
	(SELECT 
	origin
	, COUNT(*) AS actual_departed
	FROM {{ref('prep_flights')}}
	WHERE (cancelled = 0) AND actual_elapsed_time IS NOT NULL
	GROUP BY origin),
arrival_data AS
	(SELECT 
		dest
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
	FROM {{ref('prep_flights')}}
	GROUP BY dest),
actually_arrived AS
	(SELECT 
	dest
	, COUNT(*) AS actual_arrived
	FROM {{ref('prep_flights')}}
	WHERE (cancelled = 0) AND actual_elapsed_time IS NOT NULL
	GROUP BY dest),
merged_data AS
	(SELECT 
	d.origin AS airport
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
	, (d.unique_departure_planes + a.unique_arrival_planes) AS unique_planes
	FROM departure_data AS d
	LEFT JOIN actually_departed AS da ON da.origin=d.origin
	LEFT JOIN arrival_data AS a ON a.dest=d.origin
	LEFT JOIN actually_arrived AS aa ON aa.dest=d.origin)
SELECT 
	pa.name
	,pa.city
	,md.*
	,pa.country
FROM merged_data AS md
LEFT JOIN prep_airports AS pa ON pa.faa = md.airport