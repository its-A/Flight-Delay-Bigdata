""" STANDARDIZE WEAHTER DATA FROM NOAA INTO PARQUET FORMAT """

import duckdb
import os

os.makedirs("data/curated/weather", exist_ok=True)
con = duckdb.connect()

print("Cleaning and standardizing 2025 weather observations...")

query = """
CREATE OR REPLACE TABLE curated_weather AS
SELECT 
    -- Map station ID back to Airport code if using filename or station
    CASE 
        WHEN STATION = '72530094846' THEN 'ORD'
        WHEN STATION = '72219013874' THEN 'ATL'
        WHEN STATION = '72259003927' THEN 'DFW'
        WHEN STATION = '72469523062' THEN 'DEN'
        WHEN STATION = '74486094789' THEN 'JFK'
        WHEN STATION = '72295023174' THEN 'LAX'
        ELSE 'OTHER'
    END AS AirportCode,
    
    -- Parse observation timestamp and truncate to nearest hour for joining
    TRY_CAST(DATE AS TIMESTAMP) AS ObservationTime,
    DATE_TRUNC('hour', TRY_CAST(DATE AS TIMESTAMP)) AS WeatherHour,
    
    -- Clean numeric metrics (NOAA uses 's', '*', or text flags for estimates)
    TRY_CAST(REGEXP_REPLACE(CAST(HourlyDryBulbTemperature AS VARCHAR), '[^0-9.-]', '', 'g') AS DOUBLE) AS Temp_F,
    TRY_CAST(REGEXP_REPLACE(CAST(HourlyWindSpeed AS VARCHAR), '[^0-9.-]', '', 'g') AS DOUBLE) AS WindSpeed_mph,
    TRY_CAST(REGEXP_REPLACE(CAST(HourlyVisibility AS VARCHAR), '[^0-9.-]', '', 'g') AS DOUBLE) AS Visibility_miles,
    TRY_CAST(REGEXP_REPLACE(CAST(HourlyPrecipitation AS VARCHAR), '[^0-9.-]', '', 'g') AS DOUBLE) AS Precipitation_inches,
    HourlyPresentWeatherType AS WeatherType
FROM read_csv_auto('data/raw/weather/*.csv', union_by_name=True, ignore_errors=True)
WHERE AirportCode != 'OTHER';

-- Deduplicate in case a station records multiple observations within the same hour
CREATE OR REPLACE TABLE curated_weather_hourly AS
SELECT 
    AirportCode,
    WeatherHour,
    ROUND(AVG(Temp_F), 1) AS Temp_F,
    ROUND(AVG(WindSpeed_mph), 1) AS WindSpeed_mph,
    ROUND(AVG(Visibility_miles), 1) AS Visibility_miles,
    COALESCE(ROUND(MAX(Precipitation_inches), 2), 0.0) AS Precipitation_inches,
    MAX(WeatherType) AS WeatherType
FROM curated_weather
GROUP BY AirportCode, WeatherHour;

-- Export to Parquet
COPY curated_weather_hourly 
TO 'data/curated/weather/weather_2025.parquet' (FORMAT PARQUET);
"""

con.execute(query)
print("Curated weather saved to data/curated/weather/weather_2025.parquet")