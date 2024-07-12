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
    "dim_sellers": AssetIn(
        key_prefix=["silver_layer"], 
        )
    },
    outs={
        "seller_status": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["gold_layer"],
            metadata={
                "primary_keys": [
                    "year",
                    "seller_id",
                    ],
                "columns": [
                    "seller_id",
                    "seller_state",
                    "status",
                    ],
                },
            )
        },
    compute_kind="PostgreSQL",
    group_name = 'gold',
    retry_policy=RetryPolicy(max_retries=3)
    )
def seller_status(fact_sales, dim_sellers) -> Output[pd.DataFrame]:
    
    successful_orders = fact_sales[fact_sales['order_status'] == 'delivered']
    
    successful_orders['order_purchase_timestamp'] = pd.to_datetime(successful_orders['order_purchase_timestamp']).dt.date
    
    daily_sales_products = successful_orders.groupby([
        'order_purchase_timestamp', 
        'seller_id']).agg({
                        'order_id': pd.Series.nunique  # Count distinct order_id
    }).reset_index()

    daily_sales_products = daily_sales_products.rename(columns={
        'order_purchase_timestamp': 'daily',
        'order_id': 'bills'
    })
    
    daily_sales_products['daily'] = pd.to_datetime(daily_sales_products['daily'])
    daily_sales_products['year'] = daily_sales_products['daily'].dt.strftime("%Y")

    merge_df = pd.merge(
        dim_sellers[['seller_id', 'seller_city', 'seller_state']],
        daily_sales_products,
        on='seller_id',
        how='left'
    )
    merge_df['status'] = merge_df['bills'].apply(lambda x: 'active' if pd.notnull(x) else 'inactive')
    
    seller_status = merge_df[[
        'seller_id', 
        'status',
        'year'
    ]].sort_values(
        by=[ 'year','seller_id'], 
        ascending = [ True, False]
    )
    return Output(
        seller_status,
        metadata={
            "schema": "public",
            "table": "seller_status",
            "records counts": len(seller_status),
            },
        )
    