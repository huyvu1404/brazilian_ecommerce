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
def bronze_state_name(context) -> Output[pd.DataFrame]:
    
    query = "SELECT * FROM brazil_state_name"
    df = context.resources.mysql_io_manager.extract_data(query)
    
    return Output(
        df,
        metadata = {
            "table": "brazil_state_name",
            "records count": len(df)
        }
    )