WITH flight_data AS 
    (SELECT 
        --origin airport code
        origin AS origin_airport
        --destination airport code
        , dest AS dest_airport
        --total flights on this 
        , COUNT(flight_number) AS total_flights_on_route
        --unique airplanes
        , COUNT(DISTINCT tail_number) AS unique_airplanes
        --unique airlines
        , COUNT(DISTINCT airline) AS unique_airlines
        --on average what is the actual elapsed time
        , AVG(actual_elapsed_time) AS avg_elapsed_time
        --on average what is the delay on arrival
        , AVG(arr_delay_interval) as avg_arr_delay
        --what was the max delay?
        , MAX(arr_delay_interval) as max_arr_delay
        --what was the min delay?
        , MIN(arr_delay_interval) as min_arr_delay
        --total number of cancelled
        , SUM(cancelled) AS total_cancelled
        --total number of diverted
        , SUM(diverted) AS total_diverted
    FROM {{ref('prep_flights')}}
    GROUP BY (dest, origin))
SELECT
--add city, country and name for both, origin and destination, airports
    o.name AS origin_name
    , o.city AS origin_city
    , d.name AS dest_name
    , d.city AS dest_city
    , o.country AS origin_country
    , d.country AS dest_country
    , f.*
FROM flight_data as f
LEFT JOIN {{ref('prep_airports')}} AS o ON o.faa=f.origin_airport
LEFT JOIN {{ref('prep_airports')}} AS d ON d.faa=f.dest_airport