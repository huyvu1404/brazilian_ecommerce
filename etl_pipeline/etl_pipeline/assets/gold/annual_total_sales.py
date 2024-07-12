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
    "fact_sales": AssetIn(
        key_prefix=["silver_layer"],
        )
    },
    outs={
        "annual_total_sales": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["gold_layer"],
            metadata={
                "primary_keys": [
                    "year",
                    ],
                "columns": [
                    "total_sales",
                    "total_bills"
                    ],
                },
            )
        },
    compute_kind="PostgreSQL",
    group_name = 'gold',
    retry_policy=RetryPolicy(max_retries=3)
    )
def annual_total_sales(fact_sales) -> Output[pd.DataFrame]:
    
    successful_orders = fact_sales[fact_sales['order_status'] == 'delivered']
    successful_orders['order_purchase_timestamp'] = pd.to_datetime(successful_orders['order_purchase_timestamp']).dt.strftime("%Y")
    

    annual_sales = successful_orders.groupby(['order_purchase_timestamp']).agg({
        'payment_value': 'sum',
        'order_id': pd.Series.nunique
    }).reset_index()
    
    annual_sales = annual_sales.rename(columns = {
        'order_purchase_timestamp': 'year',
        'payment_value': 'total_sales',
        'order_id': 'total_bills',
    })

    annual_sales = annual_sales[[
        'year', 
        'total_sales',
        'total_bills',
    ]].sort_values(
        by=['year', 'total_sales', 'total_bills'], 
        ascending = [True, False, False]
    )
    return Output(
        annual_sales,
        metadata={
            "schema": "public",
            "table": "annual_sales",
            "records counts": len(annual_sales),
            },
        )
    