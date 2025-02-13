from dagster import (
    multi_asset, 
    AssetIn, 
    AssetOut, 
    Output,
    RetryPolicy
)
import pandas as pd


@multi_asset(
    ins={
    "bronze_olist_sellers_dataset": AssetIn(
        key_prefix=["bronze_layer"],
        ),
    "bronze_state_name": AssetIn(
        key_prefix=["bronze_layer"],
        )
    },
    outs={
        "dim_sellers": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["silver_layer"],
            )
        },
    compute_kind="Minio",
    group_name = 'silver',
    retry_policy=RetryPolicy(max_retries=3)
)
def dim_sellers(bronze_olist_sellers_dataset, bronze_state_name) -> Output[pd.DataFrame]:
    merge_df = pd.merge(
            bronze_olist_sellers_dataset, 
            bronze_state_name, 
            left_on='seller_state', 
            right_on='state_id',
            how='inner'
        )
    selected_df = merge_df[[
             'seller_id', 
             'seller_city',
             'state_name',
             'seller_zip_code_prefix',

         ]]
    return Output(
        selected_df,
        metadata={
            "schema": "public",
            "table": "dim_sellers",
            "records counts": len(selected_df),
            },
        )
