from dagster import (
    define_asset_job, 
    AssetSelection,
)
from ..partitions import monthly_partition

bronze_assets_job = define_asset_job(
    name="bronze_assets_job",
    selection=AssetSelection.groups("bronze"),
    partitions_def=monthly_partition
)

silver_assets_job = define_asset_job(
    name="silver_assets_job",
    selection=AssetSelection.groups("silver"),
    partitions_def=monthly_partition
)

gold_assets_job = define_asset_job(
    name="gold_assets_job",
    selection=AssetSelection.groups("gold"),
    partitions_def=monthly_partition
)
