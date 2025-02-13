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
        ),
    "dim_customers": AssetIn(
        key_prefix=["silver_layer"], 
        )
    },
    outs={
        "total_payments_by_customers": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["gold_layer"],
            metadata={
                "primary_keys": [
                    "customer_id",
                    ],
                "columns": [
                    "total_sales",
                    "total_bills",
                    "rank",
                    ],
                },
            )
        },
    compute_kind="PostgreSQL",
    group_name = 'gold',
    retry_policy=RetryPolicy(max_retries=3)
    )
def total_payments_by_customers(fact_sales, dim_customers) -> Output[pd.DataFrame]:
    
    # successful_orders = fact_sales[fact_sales['order_status'] == 'delivered']
    # successful_orders['order_purchase_timestamp'] = pd.to_datetime(successful_orders['order_purchase_timestamp']).dt.strftime("%Y")

    merge_df = pd.merge(
        fact_sales,
        dim_customers[['customer_id', 'customer_city', 'customer_state']],
        on='customer_id',
        how='inner'
    )
    
    grouped_df = merge_df.groupby(['order_purchase_timestamp', 'customer_id', 'customer_city', 'customer_state']).agg({
        'payment_value': 'sum',
        'order_id': pd.Series.nunique
    }).reset_index()
    
    grouped_df = grouped_df.rename(columns = {
        'order_purchase_timestamp': 'year',
        'payment_value': 'total_payments',
        'order_id': 'total_bills',
    })
  
    total_payments_by_customers = grouped_df[[
        'year', 
        'customer_id', 
        'customer_city', 
        'customer_state', 
        'total_payments',
        'total_bills'
    ]].sort_values(
        by=['year', 'total_payments', 'total_bills'], 
        ascending = [True, False, False]
    )
    return Output(
        total_payments_by_customers,
        metadata={
            "schema": "public",
            "table": "total_payments_by_customers",
            "records counts": len(total_payments_by_customers),
            },
        )
    