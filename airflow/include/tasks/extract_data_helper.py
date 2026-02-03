
import pandas as pd
import requests

def extract_data_helper(year: int = 2019, chunksize: int = 100_000, color: str = "green"):
    # store urls
    urls = []

    for month in range(1, 13):
        try:
            url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02d}.csv.gz"
            urls.append(url)
        
        except Exception as e:
            print(f"Error constructing URL for {year}-{month:02d}: {e}")
            continue
    print(f"Constructed URLs for year {year}: {urls}")

    # define data types for columns
    dtypes = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "object",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
    }



    # 1. Generalize date column names based on color
    if color == "yellow":
        date_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    else:
        date_cols = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]

    for url in urls:
        try:
            response = requests.head(url)
            if response.status_code in [200, 302]:
                reader = pd.read_csv(
                    url,
                    iterator=True,
                    chunksize=chunksize,
                    dtype=dtypes,
                    parse_dates=date_cols, # Use the dynamic list here
                )
                for chunk in reader:
                    # To make downstream tasks easier, rename the date columns 
                    # to a generic name like 'pickup_datetime'
                    chunk = chunk.rename(columns={
                        date_cols[0]: "pickup_datetime",
                        date_cols[1]: "dropoff_datetime"
                    })
                    yield chunk
            else:
                print(f"URL not accessible: {url}")
        except Exception as e:
            print(f"Error processing {url}: {e}")