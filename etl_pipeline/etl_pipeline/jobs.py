from dagster import (
    define_asset_job, 
    build_schedule_from_partitioned_job,
    AssetSelection,
    DailyPartitionsDefinition
)



from datetime import datetime



etl_job = define_asset_job(
    "etl_job",
    selection = AssetSelection.groups("etl_pipeline"),
    partitions_def=DailyPartitionsDefinition(start_date=datetime(2023, 12, 27))
)

etl_schedule = build_schedule_from_partitioned_job(job = etl_job)
