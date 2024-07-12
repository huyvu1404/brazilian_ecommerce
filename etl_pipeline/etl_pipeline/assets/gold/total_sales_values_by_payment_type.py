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
        "average_order_value_by_payment_type": AssetOut(
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
def total_sales_values_by_payment_type(fact_sales) -> Output[pd.DataFrame]:
    
    successful_orders = fact_sales[fact_sales['order_status'] == 'delivered']
    total_sales = successful_orders['payment_value'].sum()
    average_order_value = successful_orders.groupby([
        'payment_type'
        ]).agg({
                'payment_value': 'sum'
    }).reset_index()

    average_order_value['percent'] = round(average_order_value['payment_value']/total_sales*100, 2)
    average_order_value = average_order_value.rename(columns = {
        'payment_value': 'total_sales'
    })    

    average_order_value_by_payment_type = average_order_value[[
        'payment_type', 
        'total_sales', 
        'percent']]
   
    return Output(
        average_order_value_by_payment_type,
        metadata={
            "schema": "public",
            "table": "average_order_value_by_payment_type",
            "records counts": len(average_order_value_by_payment_type),
            },
        )
    