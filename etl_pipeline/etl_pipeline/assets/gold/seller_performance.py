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
        "seller_performance": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["gold_layer"],
            metadata={
                "primary_keys": [
                    "year",
                    "seller_id",
                    ],
                "columns": [
                    "state_name",
                    "seller_city",
                    'total_sales',
                    'total_bills',
                    "values_per_bills",
                    ],
                },
            )
        },
    compute_kind="PostgreSQL",
    group_name = 'gold',
    retry_policy=RetryPolicy(max_retries=3)
    )
def seller_performance(fact_sales, dim_sellers) -> Output[pd.DataFrame]:
    
    successful_orders = fact_sales[fact_sales['order_status'] == 'delivered']
    successful_orders['order_purchase_timestamp'] = pd.to_datetime(successful_orders['order_purchase_timestamp']).dt.date
    
    daily_sales_products = successful_orders.groupby([
        'order_purchase_timestamp', 
        'seller_id']).agg({
                        'payment_value': 'sum',
                        'order_id': pd.Series.nunique  # Count distinct order_id
    }).reset_index()

    daily_sales_products = daily_sales_products.rename(columns={
        'order_purchase_timestamp': 'daily',
        'payment_value': 'sales',
        'order_id': 'bills'
    })
    
    daily_sales_products['daily'] = pd.to_datetime(daily_sales_products['daily'])
    daily_sales_products['year'] = daily_sales_products['daily'].dt.strftime("%Y")

    merge_df = pd.merge(
        daily_sales_products,
        dim_sellers[['seller_id', 'seller_city', 'state_name']],
        on='seller_id',
        how='inner'
    )

    grouped_df = merge_df.groupby(['year', 'seller_id', 'seller_city', 'state_name']).agg({
        'sales': 'sum',
        'bills': 'sum'
    }).reset_index()
    
    grouped_df = grouped_df.rename(columns = {
        'sales': 'total_sales',
        'bills': 'total_bills',
    })
    grouped_df['values_per_bills'] = grouped_df['total_sales'] / grouped_df['total_bills']
    seller_performance = grouped_df[[
        'year', 
        'seller_id', 
        'state_name', 
        'seller_city', 
        'total_sales',
        'total_bills', 
        'values_per_bills'
    ]].sort_values(
        by=['year', 'total_sales', 'total_bills'], 
        ascending = [True, False, False]
    )
   
    return Output(
        seller_performance,
        metadata={
            "schema": "public",
            "table": "seller_performance",
            "records counts": len(seller_performance),
            },
        )
    