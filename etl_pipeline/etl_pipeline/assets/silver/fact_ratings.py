from dagster import (
    multi_asset, 
    AssetIn, 
    AssetOut, 
    Output,
    RetryPolicy
)
import pandas as pd


@multi_asset(
    ins = {
        "bronze_olist_order_reviews_dataset": AssetIn(
        key_prefix=["bronze_layer"],
        ),
        "bronze_olist_order_items_dataset": AssetIn(
        key_prefix=["bronze_layer"],
        ),
        "bronze_olist_orders_dataset": AssetIn(
        key_prefix=["bronze_layer"],
        )
    },
    outs={
        "fact_ratings": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["silver_layer"],
            )
        },
    compute_kind="Minio",
    group_name = 'silver',
    retry_policy=RetryPolicy(max_retries=3)
)
def fact_ratings(bronze_olist_order_reviews_dataset, bronze_olist_order_items_dataset, bronze_olist_orders_dataset) -> Output[pd.DataFrame]:
    
    merge_df = pd.merge(
        bronze_olist_order_reviews_dataset, 
        bronze_olist_order_items_dataset, 
        on = 'order_id',
        how = 'inner')
    
    merge_df = pd.merge(
        merge_df, 
        bronze_olist_orders_dataset, 
        on = 'order_id',
        how = 'inner')
        
    selected_df = merge_df[[
            'order_purchase_timestamp',
            'review_id', 
            'order_id', 
            'product_id', 
            'review_score'
        ]]
    return Output(
        selected_df,
        metadata={
            "schema": "public",
            "table": "fact_ratings",
            "records counts": len(selected_df),
            },
        )
