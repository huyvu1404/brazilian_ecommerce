from dagster import asset, Output, RetryPolicy
import pandas as pd

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze_layer"],
    compute_kind="MySQL",
    group_name = 'bronze',
    retry_policy=RetryPolicy(max_retries=3)
)
def bronze_olist_sellers_dataset(context) -> Output[pd.DataFrame]:
    
    query = "SELECT * FROM olist_sellers_dataset"
    df = context.resources.mysql_io_manager.extract_data(query)
    
    return Output(
        df,
        metadata = {
            "table": "olist_sellers_dataset",
            "records count": len(df)
        }
    )