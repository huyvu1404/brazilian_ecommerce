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
        "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze_layer"],
        ),
        "bronze_olist_order_payments_dataset": AssetIn(
        key_prefix=["bronze_layer"],
        ),
        "bronze_olist_order_items_dataset": AssetIn(
        key_prefix=["bronze_layer"],
        )
    },
    outs={
        "fact_sales": AssetOut(
            io_manager_key="minio_io_manager",
            key_prefix=["silver_layer"],
            )
        },
    compute_kind="Minio",
    group_name = 'silver',
    retry_policy=RetryPolicy(max_retries=3)
)
def fact_sales(bronze_olist_orders_dataset, bronze_olist_order_payments_dataset, bronze_olist_order_items_dataset) -> Output[pd.DataFrame]:
    merge_df = pd.merge(
            bronze_olist_orders_dataset, 
            bronze_olist_order_payments_dataset, 
            on='order_id', 
            how='inner'
        )
    merge_df = pd.merge(
            merge_df, 
            bronze_olist_order_items_dataset, 
            on='order_id', 
            how='inner'
        )
    selected_df = merge_df[[
            'order_id', 
            'customer_id', 
            'seller_id', 
            'order_purchase_timestamp', 
            'product_id', 
            'price',
            'payment_value',
            'payment_type', 
            'order_status'
        ]]
    return Output(
        selected_df,
        metadata={
            "schema": "public",
            "table": "fact_sales",
            "records counts": len(selected_df),
            },
        )
