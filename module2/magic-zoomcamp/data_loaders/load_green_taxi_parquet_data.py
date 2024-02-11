import io
import pandas as pd
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    
    taxi_dtypes = {
     "VendorID" : pd.Int64Dtype(),
     "store_and_fwd_flag" : str,
     "RatecodeID" : pd.Int64Dtype(),
     "PULocationID" : pd.Int64Dtype(),
     "DOLocationID" : pd.Int64Dtype(),
     "passenger_count" : pd.Int64Dtype(),
     "trip_distance": float,
     "fare_amount" : float,
     "extra" : float,
     "mta_tax" : float,
     "tip_amount" : float,
     "tolls_amount" : float,
     "ehail_fee" : float,
     "congestion_surcharge" : float,
     "improvement_surcharge" : float,
     "total_amount" : float,
     "payment_type" : pd.Int64Dtype() ,
     "trip_type" : pd.Int64Dtype()
     }

    parse_dates = ["lpep_pickup_datetime","lpep_dropoff_datetime"]

    df_final = []
    months_list = list(i if len(str(i)) == 2 else '0' + str(i) for i in range(1,13))

    for month in months_list:
        print(month)
    
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{month}.parquet'

        print(url)

        df = pd.read_parquet(url, engine='pyarrow').astype(taxi_dtypes)

        print(f'Month {month} is imported...{df.shape[0]} rows were present in the parquet file of month {month}!')

        df_final.append(df)
        
        
    return pd.concat(df_final)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
