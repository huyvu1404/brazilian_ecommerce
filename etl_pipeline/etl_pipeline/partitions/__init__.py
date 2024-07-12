from dagster import MonthlyPartitionsDefinition

monthly_partition = MonthlyPartitionsDefinition(
    start_date='2024-07-01'
)