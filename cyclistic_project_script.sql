--Data cleaning
--Create 12 tables, one for each month. Then, merge all of them to create a unique database to work with
CREATE OR REPLACE TABLE cyclistic_data.final_database AS
SELECT *
FROM cyclistic_data.data_2023_03
UNION DISTINCT
SELECT *
FROM cyclistic_data.data_2023_04
UNION DISTINCT
SELECT *
FROM cyclistic_data.data_2023_05
UNION DISTINCT
SELECT *
FROM cyclistic_data.data_2023_06
UNION DISTINCT
SELECT *
FROM cyclistic_data.data_2023_07
UNION DISTINCT
SELECT *
FROM cyclistic_data.data_2023_08
UNION DISTINCT
SELECT *
FROM cyclistic_data.data_2023_09
UNION DISTINCT
SELECT *
FROM cyclistic_data.data_2023_10
UNION DISTINCT
SELECT *
FROM cyclistic_data.data_2023_11
UNION DISTINCT
SELECT *
FROM cyclistic_data.data_2023_12
UNION DISTINCT
SELECT *
FROM cyclistic_data.data_2024_01
UNION DISTINCT
SELECT *
FROM cyclistic_data.data_2024_02;

--Check if 'started_at' and 'ended_at' are in the correct format ('YYYY-MM-DD HH:MM:SS')

SELECT *
FROM cyclistic_data.final_database
WHERE TIMESTAMP(started_at) IS NULL
   OR TIMESTAMP(ended_at) IS NULL;

-- No incorrectly formatted records found
--Check if the number of unique station names and the number of unique station IDs match
--For that purpose, first create a table of all unique station names

CREATE TABLE IF NOT EXISTS cyclistic_data.names AS
SELECT DISTINCT start_station_name AS station_name FROM cyclistic_data.final_database
UNION DISTINCT
SELECT DISTINCT end_station_name AS station_name FROM cyclistic_data.final_database
ORDER BY station_name;

--Create a table of all unique station IDs
CREATE TABLE cyclistic_data.ids AS
SELECT DISTINCT start_station_id AS station_id FROM cyclistic_data.final_database
UNION DISTINCT
SELECT DISTINCT end_station_id AS station_id FROM cyclistic_data.final_database;

--The list of station names has more observations than the list of IDs. It means that in some cases, different stations share the same ID.
--Let's see wich ID have more than one station name associated
CREATE TABLE cyclistic_data.station_info AS
SELECT station_id, ARRAY_AGG(DISTINCT station_name) AS station_names
FROM (
    SELECT start_station_id AS station_id, start_station_name AS station_name FROM cyclistic_data.final_database
    UNION ALL
    SELECT end_station_id AS station_id, end_station_name AS station_name FROM cyclistic_data.final_database
)
WHERE station_id IS NOT NULL
GROUP BY station_id
HAVING COUNT(*) >=2;

CREATE TABLE cyclistic_data.names_with_same_id AS
SELECT station_id, station_names
FROM cyclistic_data.station_info
WHERE ARRAY_LENGTH(station_names) > 1;

--Let's create a new ID for those station names who need it, one by one, in the 'final_database' table, so every station name have their own id. I have written only one code as an example.

UPDATE cyclistic_data.final_database
SET start_station_id = 'ABC2'
WHERE start_station_name = 'Vernon Ave & 75th St';

UPDATE cyclistic_data.data_2023_03
SET end_station_id = 'ABC2'
WHERE end_station_name = 'Vernon Ave & 75th St';

--Remove duplicates

CREATE OR REPLACE TABLE cyclistic_data.final_database AS
SELECT DISTINCT *
FROM cyclistic_data.final_database;

--Set the latitude and longitude columns to the FLOAT64 data type to occupy less space and round them to a suitable precision.

CREATE OR REPLACE TABLE cyclistic_data.final_database AS
SELECT 
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    CAST(start_lat AS FLOAT64) AS start_latitude,
    CAST(start_lng AS FLOAT64) AS start_longitude,
    CAST(end_lat AS FLOAT64) AS end_latitude,
    CAST(end_lng AS FLOAT64) AS end_longitude,
    member_casual
FROM
    cyclistic_data.final_database;
--The database is ready for analysis. Let's see the differences between casual riders and members in the ways of usage of the bicycles.
--Usage per day
CREATE TABLE cyclistic_data.trips_per_day AS
SELECT FORMAT_DATE('%A', DATE(started_at)) AS day_of_week,
member_casual,
COUNT(*) AS trips_count
FROM cyclistic_data.final_database
GROUP BY day_of_week, member_casual
ORDER BY
    CASE
        WHEN day_of_week='Monday' THEN 1
        WHEN day_of_week='Tuesday' THEN 2
        WHEN day_of_week='Wednesday' THEN 3
        WHEN day_of_week='Thursday' THEN 4
        WHEN day_of_week='Friday' THEN 5
        WHEN day_of_week='Saturday' THEN 6
        ELSE 7 --Sunday
    END,
    member_casual;
--Export data to create a chart in Tableau Public
EXPORT DATA OPTIONS(
    uri='gs://cyclistic_bucket_jg/trips_per_day*.csv',
    format='CSV',
    overwrite=true,
    header=true,
    field_delimiter=','
) AS
SELECT *
FROM cyclistic_data.trips_per_day;

--Average ride length per day. Member vs casual rider
CREATE TABLE cyclistic_data.avg_ride_length AS
SELECT
    FORMAT_DATE('%A', DATE(started_at)) AS day_of_week,
    member_casual,
    CAST(ROUND(AVG(TIMESTAMP_DIFF(ended_at,started_at, MINUTE))) AS INT64) AS avg_trip_duration_minutes
FROM cyclistic_data.final_database
GROUP BY
    day_of_week, member_casual
ORDER BY
    CASE day_of_week
    WHEN 'Monday' THEN 1
    WHEN 'Tuesday' THEN 2
    WHEN 'Wednesday' THEN 3
    WHEN 'Thursday' THEN 4
    WHEN 'Friday' THEN 5
    WHEN 'Saturday' THEN 6
    ELSE 7 -- Sunday
END,
member_casual;

EXPORT DATA OPTIONS(
    uri='gs://cyclistic_bucket_jg/avg_ride_length_minutes*.csv',
    format='CSV',
    overwrite=true,
    header=true,
    field_delimiter=','
) AS
SELECT *
FROM cyclistic_data.avg_ride_length;

--Bicycle use per season. Member vs casual rider

CREATE TABLE cyclistic_data.trips_per_season AS
SELECT
    CASE
        WHEN EXTRACT(MONTH FROM DATE(started_at)) IN (1,2) OR
            (EXTRACT(MONTH FROM DATE(started_at))=3 AND EXTRACT(DAY FROM DATE(started_at))<21) OR
            (EXTRACT(MONTH FROM DATE(started_at))=12 AND EXTRACT(DAY FROM DATE(started_at))>=21) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM DATE(started_at)) IN(4,5) OR
            (EXTRACT(MONTH FROM DATE(started_at))=3 AND EXTRACT(DAY FROM DATE(started_at))>=21) OR
            (EXTRACT(MONTH FROM DATE(started_at))=6 AND EXTRACT(DAY FROM DATE(started_at))<21) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM DATE(started_at)) IN(7,8) OR
            (EXTRACT(MONTH FROM DATE(started_at))=6 AND EXTRACT(DAY FROM DATE(started_at))>=21) OR
            (EXTRACT(MONTH FROM DATE(started_at))=9 AND EXTRACT(DAY FROM DATE(started_at))<21) THEN 'Summer'
        ELSE 'Autumn'
    END AS season,
    member_casual,
    COUNT(*) AS trip_count
FROM cyclistic_data.final_database
GROUP BY season, member_casual
ORDER BY
    CASE season
        WHEN 'Winter' THEN 1
        WHEN 'Spring' THEN 2
        WHEN 'Summer' THEN 3
        ELSE 4
    END,
    member_casual;

EXPORT DATA OPTIONS(
    uri='gs://cyclistic_bucket_jg/trips_per_season*.csv',
    format='CSV',
    overwrite=true,
    header=true,
    field_delimiter=','
) AS
SELECT *
FROM cyclistic_data.trips_per_season;

--Names of the most used stations. Members vs. Casual rider
CREATE TABLE cyclistic_data.member_most_used_stations AS
SELECT station_name,COUNT(*) AS trip_count
FROM (
    SELECT start_station_name AS station_name
    FROM cyclistic_data.final_database
    WHERE member_casual='member'
    UNION ALL
    SELECT end_station_name AS station_name
    FROM cyclistic_data.final_database
    WHERE member_casual='member'
)
WHERE station_name IS NOT NULL
GROUP BY station_name
ORDER BY trip_count DESC
LIMIT 10;

CREATE TABLE cyclistic_data.casual_most_used_stations AS
SELECT station_name,COUNT(*) AS trip_count
FROM (
    SELECT start_station_name AS station_name
    FROM cyclistic_data.final_database
    WHERE member_casual='casual'
    UNION ALL
    SELECT end_station_name AS station_name
    FROM cyclistic_data.final_database
    WHERE member_casual='casual'
)
WHERE station_name IS NOT NULL
GROUP BY station_name
ORDER BY trip_count DESC
LIMIT 10;

EXPORT DATA OPTIONS(
    uri='gs://cyclistic_bucket_jg/top10_station_names*.csv',
    format='CSV',
    overwrite=true,
    header=true,
    field_delimiter=','
) AS
SELECT *
FROM cyclistic_data.casual_most_used_stations;