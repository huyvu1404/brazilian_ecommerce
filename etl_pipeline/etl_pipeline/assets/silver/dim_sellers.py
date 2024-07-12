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
def dim_sellers(bronze_olist_sellers_dataset) -> Output[pd.DataFrame]:
    selected_df = bronze_olist_sellers_dataset[[
             'seller_id', 
             'seller_city',
             'seller_state'
         ]]
    return Output(
        selected_df,
        metadata={
            "schema": "public",
            "table": "dim_sellers",
            "records counts": len(selected_df),
            },
        )
