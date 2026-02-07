# Full ETL pipleline - Busiest US Flight Delays (2020-2024)

# importing the libraries 
import os
import pandas as pd
import numpy as np
import dask.dataframe as dd
import requests
from pathlib import Path

import time
import zipfile
from typing import List

# Extract: downloading data from the source

def downloading_flight_data(start_year: int, end_year: int, output_dir: str | Path) -> List[Path]:
    """
    Download complete USDOT flight data without filtering
    """
    # Conifg
    DATA_DIR = Path(output_dir)
    DATA_DIR.mkdir(parents = True,exist_ok=True) # checks if the direcory already exists

    BASE_URL = "https://transtats.bts.gov/PREZIP/"

    current_file = 0
    total_files = (end_year - start_year + 1) *12 # for all years and months
    successful_downloads = []


    print(f"Starting download from {start_year}-{end_year}")
    

    for year in range(start_year,end_year + 1):
        for month in range(1,13):
            current_file +=1
            filename = f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
            url = BASE_URL + filename
            file_path = DATA_DIR / filename

            
           # If file exists and is valid → skip
            if file_path.exists(): 
                print(f"[{current_file}/{total_files}] Already Exists: {year}-{month}")
                successful_downloads.append(file_path)
                continue


            print(f"[{current_file}/{total_files}] Downloading {year}-{month:}...", end=" ")

            try:
                r = requests.get(url, stream=True, timeout=300)

                if r.status_code != 200:
                    print(f"Unavailable (HTTP {r.status_code})")
                    continue

                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        f.write(chunk)

                
                size_mb = file_path.stat().st_size / (1024*1024)
                print(f"OK ({size_mb:.0f} MB)")
                successful_downloads.append(file_path)
                time.sleep(1)

            except Exception as e:
                print(f"Failed: {e}")
                if file_path.exists():
                    file_path.unlink()

    # Summary
    print("\n" + "="*60)
    print("DOWNLOAD SUMMARY")
    print("="*60)
    print(f"Total files attempted: {total_files}")
    print(f"Successful valid ZIPs: {len(successful_downloads)}")
    return successful_downloads




# Transform: cleaning data 
def extract_and_filter(zip_files: List[Path], processed_dir: str | Path):
    processed_path = Path(processed_dir)
    processed_path.mkdir(exist_ok=True, parents = True)

    processed_file = processed_path / "flights_2020_2025_canada_cleaned.parquet" # creates a .parquet file (uses columnar data storage)


    # To check if the file already exists
    if processed_file.exists():
        print(f"Cleaned file already exists: {processed_file}")
        print("Skipping processing.")
        return processed_file
    
    print("\n" + "="*60)
    print("Extracting and Combining all CSVs using Dask")
    print("\n" + "="*60)

    # Use Dask to read all CSVs in parallel
    csv_files = []
    for i, file in enumerate(zip_files, 1):
        print(f"[{i}/{len(zip_files)}] Extracting {file.name}...", end=" ")
        try:
            with zipfile.ZipFile(file) as z:
                csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
                temp_csv = file.parent / csv_name
                if not temp_csv.exists():
                    z.extract(csv_name, file.parent)
                csv_files.append(str(temp_csv))
                print("OK")
        except Exception as e:
            print(f"Error: {e}")
            continue

    if not csv_files:
        print("\n No CSV files extracted!")
        return None
    
    print(f"\nExtracted {len(csv_files)} CSV files\n")


    # critical columns we need for Canada analysis 
    use_cols = [
        'FlightDate', 'Reporting_Airline', 'Origin', 'Dest', 'DepTime', 'ArrTime',
        'DepDelay', 'ArrDelay', 'ArrDelayMinutes', 'Cancelled', 'Diverted',
        'Distance', 'AirTime', 'CRSDepTime', 'CRSArrTime']
    
    # Read with Dask
    ddf = dd.read_csv(csv_files, usecols = lambda c: c in use_cols, assume_missing=True, dtype = {'DepTime':'object', 'ArrTime':'object'})
    print(f"Total partitions: {ddf.npartitions}")
    print(f"Total rows (approx): {len(ddf):,}")

    # cleaning with Dask
    ddf['FlightDate'] = dd.to_datetime(ddf["FlightDate"], errors = 'coerce')


    # Busiest U.S. Airports (2020–2024) 
    top_us_airports = {
    'ATL',  # Atlanta Hartsfield–Jackson    - #1 in the world
    'DFW',  # Dallas/Fort Worth
    'DEN',  # Denver
    'ORD',  # Chicago O'Hare
    'LAX',  # Los Angeles
    'CLT',  # Charlotte
    
    }

    # boolean mask (a series of True/False values for every row)
    mask = ddf['Origin'].isin(top_us_airports) | ddf['Dest'].isin(top_us_airports)
    main_flights = ddf[mask].compute() # all operations applied, data filtered, now would be safe to use pandas

    print(f"Busiest US Flight: {len(main_flights):,} out of {len(ddf):,}")

    # saving dataset
    main_flights.to_parquet(processed_file, index=False, compression = 'snappy')
    sized_gb = processed_file.stat().st_size / (1024**3)
    print(f"Final cleaned data saved:{processed_file} ({sized_gb:.2f}) GB")


    # Cleanup extracted CSVs from data folder
    if DATA_DIR:  # Only if we know where they are
        for csv in DATA_DIR.rglob("*.csv"):
            try:
                csv.unlink()
            except:
                pass

    return processed_file

    



if __name__ == "__main__":

    DATA_DIR = Path("data/raw")
    

    # Step 1: Download + auto-heal corrupted files
    valid_zips = downloading_flight_data(2020, 2024, DATA_DIR)

    if not valid_zips:
        print("No valid files. Exiting.")
        exit()


    
    PROCESSED_DIR = Path("processed")
    transformed_file = extract_and_filter(valid_zips, PROCESSED_DIR)

    if not transformed_file:
        print("\n Processing failed. Exiting.")
        exit(1)

    
    print(f"\n{'='*60}")
    print("ETL COMPLETE! Ready for EDA")
    print(f"{'='*60}")
    



