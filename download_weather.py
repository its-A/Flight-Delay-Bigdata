import os
import requests

# Map IATA airport codes to NOAA LCD Station IDs
STATIONS = {
    "ORD": "72530094846",
    "ATL": "72219013874",
    "DFW": "72259003927",
    "DEN": "72469523062",
    "JFK": "74486094789",
    "LAX": "72295023174",
}

# Choose the year that matches your flight data (e.g., 2023)
YEAR = "2026"
OUTPUT_DIR = "data/raw/weather"

os.makedirs(OUTPUT_DIR, exist_ok=True)

base_url = f"https://www.ncei.noaa.gov/data/local-climatological-data/access/{YEAR}"

for airport, station_id in STATIONS.items():
    file_name = f"{station_id}.csv"
    download_url = f"{base_url}/{file_name}"
    target_path = os.path.join(OUTPUT_DIR, f"{airport}_{YEAR}_weather.csv")
    
    print(f"Downloading {airport} ({file_name})...")
    response = requests.get(download_url)
    
    if response.status_code == 200:
        with open(target_path, "wb") as f:
            f.write(response.content)
        print(f" Saved to {target_path}")
    else:
        print(f" Failed to download {airport}: HTTP {response.status_code}")

print("All weather downloads complete!")


