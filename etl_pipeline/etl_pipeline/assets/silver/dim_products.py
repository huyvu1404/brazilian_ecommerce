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
    "bronze_olist_products_dataset": AssetIn(
        key_prefix=["bronze_layer"],
        ),
    "product_category_name_translation": AssetIn(
        key_prefix=["bronze_layer"],
        ),
    },
    outs={
        "dim_products": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["silver_layer"],
            )
        },
    compute_kind="Minio",
    group_name = 'silver',
    retry_policy=RetryPolicy(max_retries=3)
)
def dim_products(bronze_olist_products_dataset, product_category_name_translation) -> Output[pd.DataFrame]:
    merge_df = pd.merge(
            bronze_olist_products_dataset, 
            product_category_name_translation, 
            on='product_category_name', 
            how='inner'
        )
    selected_df = merge_df[[
            'product_id', 
            'product_category_name_english'
        ]]
    return Output(
        selected_df,
        metadata={
            "schema": "public",
            "table": "dim_products",
            "records counts": len(selected_df),
            },
        )
