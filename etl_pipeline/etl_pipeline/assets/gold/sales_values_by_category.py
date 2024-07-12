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
    "dim_products": AssetIn(
        key_prefix=["silver_layer"], 
        )
    },
    outs={
        "sales_values_by_category": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["gold_layer"],
            metadata={
                "primary_keys": [
                    "monthly",
                    "category",
                    ],
                "columns": [
                    "total_sales",
                    "total_bills",
                    "values_per_bills",
                    ],
                },
            )
        },
    compute_kind="PostgreSQL",
    group_name = 'gold',
    retry_policy=RetryPolicy(max_retries=3)
    )
def sales_values_by_category(fact_sales, dim_products) -> Output[pd.DataFrame]:
    
    successful_orders = fact_sales[fact_sales['order_status'] == 'delivered']
    successful_orders['order_purchase_timestamp'] = pd.to_datetime(successful_orders['order_purchase_timestamp']).dt.date
    
    daily_sales_products = successful_orders.groupby([
        'order_purchase_timestamp', 
        'product_id']).agg({
                        'payment_value': 'sum',
                        'order_id': pd.Series.nunique  
    }).reset_index()

    daily_sales_products = daily_sales_products.rename(columns={
        'order_purchase_timestamp': 'daily',
        'payment_value': 'sales',
        'order_id': 'bills'
    })
    
    daily_sales_products['daily'] = pd.to_datetime(daily_sales_products['daily'])
    daily_sales_products['monthly'] = daily_sales_products['daily'].dt.strftime("%Y-%m")

    merge_df = pd.merge(
        daily_sales_products,
        dim_products[['product_id', 'product_category_name_english']],
        on='product_id',
        how='inner'
    )

    grouped_df = merge_df.groupby(['monthly', 'product_category_name_english']).agg({
        'sales': 'sum',
        'bills': 'sum'
    }).reset_index()
    
    grouped_df = grouped_df.rename(columns = {
        'sales': 'total_sales',
        'bills': 'total_bills',
        'product_category_name_english': 'category'
    })
    grouped_df['values_per_bills'] = grouped_df['total_sales'] / grouped_df['total_bills']
    monthly_sales_categories = grouped_df[['monthly', 'category', 'total_sales', 'total_bills', 'values_per_bills']]
   
    return Output(
        monthly_sales_categories,
        metadata={
            "schema": "public",
            "table": "monthly_sales_categories",
            "records counts": len(monthly_sales_categories),
            },
        )
    