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
    "fact_reviews": AssetIn(
        key_prefix=["silver_layer"],
        ),
    "dim_products": AssetIn(
        key_prefix=["silver_layer"], 
        )
    },
    outs={
        "average_customer_rating": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["gold_layer"],
            metadata={
                "primary_keys": [
                    "month",
                    "product_id"
                    ],
                "columns": [
                    "product_category_name_english",
                    "avg_score"                 
                    ],
                },
            )
        },
    compute_kind="PostgreSQL",
    group_name = 'etl_pipeline',
    retry_policy=RetryPolicy(max_retries=3)
    )
def average_customer_rating(fact_reviews, dim_products) -> Output[pd.DataFrame]:
    
    fact_reviews['order_purchase_timestamp'] = pd.to_datetime(fact_reviews['order_purchase_timestamp']).dt.strftime("%Y-%m")
    
    merge_df = pd.merge(
        fact_reviews,
        dim_products[['product_id', 'product_category_name_english']],
        on='product_id',
        how='inner'
    )

    grouped_df = merge_df.groupby(['order_purchase_timestamp','product_id', 'product_category_name_english']).agg({
        'review_score': 'mean'
    }).reset_index()
    
    grouped_df = grouped_df.rename(columns = {
        'order_purchase_timestamp': 'month',
        'review_score': 'avg_score'
    })
   
    average_customer_rating = grouped_df[[
        'month',
        'product_id', 
        'product_category_name_english', 
        'avg_score'
        ]].sort_values(
            by = ['product_id', 'product_category_name_english', 'month']
        )
   
    return Output(
        average_customer_rating,
        metadata={
            "schema": "public",
            "table": "monthly_sales_categories",
            "records counts": len(average_customer_rating),
            },
        )
    