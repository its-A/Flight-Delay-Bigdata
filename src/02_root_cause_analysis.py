import duckdb
import os

os.makedirs("data/mart", exist_ok=True)
con = duckdb.connect()

print("Executing Root Cause Decomposition on BTS summary data...")

query = """
CREATE OR REPLACE TABLE mart_root_cause_summary AS
WITH base AS (
    SELECT 
        Year,
        Month,
        Carrier,
        CarrierName,
        Airport,
        AirportName,
        TotalArrivals,
        DelayedArrivals15,
        CancelledArrivals,
        DivertedArrivals,
        TotalDelayMin,
        CarrierDelayMin,
        WeatherDelayMin,
        NASDelayMin,
        SecurityDelayMin,
        LateAircraftDelayMin
    FROM parquet_scan('data/curated/flights/**/*.parquet')
)
SELECT 
    Carrier,
    CarrierName,
    Airport,
    AirportName,
    Year,
    Month,
    CAST(SUM(TotalArrivals) AS BIGINT) AS TotalFlights,
    CAST(SUM(DelayedArrivals15) AS BIGINT) AS DelayedFlights,
    ROUND(100.0 * SUM(DelayedArrivals15) / NULLIF(SUM(TotalArrivals), 0), 2) AS DelayRatePct,
    ROUND(SUM(TotalDelayMin) / NULLIF(SUM(DelayedArrivals15), 0), 1) AS AvgDelayMinPerDelayedFlight,
    -- Delay minutes by root cause
    ROUND(SUM(CarrierDelayMin), 0) AS TotalCarrierDelayMin,
    ROUND(SUM(WeatherDelayMin), 0) AS TotalWeatherDelayMin,
    ROUND(SUM(NASDelayMin), 0) AS TotalNASDelayMin,
    ROUND(SUM(SecurityDelayMin), 0) AS TotalSecurityDelayMin,
    ROUND(SUM(LateAircraftDelayMin), 0) AS TotalLateAircraftDelayMin,
    -- Root cause attribution percentages (% of total delay minutes)
    ROUND(100.0 * SUM(CarrierDelayMin) / NULLIF(SUM(TotalDelayMin), 0), 2) AS CarrierAttributionPct,
    ROUND(100.0 * SUM(WeatherDelayMin) / NULLIF(SUM(TotalDelayMin), 0), 2) AS WeatherAttributionPct,
    ROUND(100.0 * SUM(NASDelayMin) / NULLIF(SUM(TotalDelayMin), 0), 2) AS NASAttributionPct,
    ROUND(100.0 * SUM(LateAircraftDelayMin) / NULLIF(SUM(TotalDelayMin), 0), 2) AS PropagationAttributionPct
FROM base
GROUP BY Carrier, CarrierName, Airport, AirportName, Year, Month;

-- Export to Parquet for Tableau and analysis
COPY mart_root_cause_summary 
TO 'data/mart/mart_root_cause_summary.parquet' (FORMAT PARQUET);

-- Also export a CSV version for quick inspection / Tableau import
COPY mart_root_cause_summary 
TO 'data/mart/mart_root_cause_summary.csv' (HEADER, DELIMITER ',');
"""

con.execute(query)
print("Root cause analysis complete!")
print("Saved to:")
print(" - data/mart/mart_root_cause_summary.parquet")
print(" - data/mart/mart_root_cause_summary.csv")