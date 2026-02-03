import pandas as pd
import hashlib

def transform_df_helper(df: pd.DataFrame, source) -> pd.DataFrame:
  print(type(df))
    concat_cols = (
        df["VendorID"].astype(str)
        + df["lpep_pickup_datetime"].astype(str)
        + df["lpep_dropoff_datetime"].astype(str)
        + df["PULocationID"].astype(str)
        + df["DOLocationID"].astype(str)
        + df["fare_amount"].astype(str)
        + df["trip_distance"].astype(str)
    )

    df["unique_row_id"] = concat_cols.apply(
        lambda x: hashlib.md5(x.encode()).hexdigest()
    )
  
    df['source'] = source

    return df