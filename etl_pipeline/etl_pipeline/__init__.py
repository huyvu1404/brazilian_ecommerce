from .resources import my_resources
from .assets import (
    bronze_layer_assets,
    silver_layer_assets,
    gold_layer_assets
)
from .jobs import bronze_assets_job,silver_assets_job, gold_assets_job
from .schedules import bronze_assets_schedule, silver_assets_schedule, gold_assets_schedule

from dagster import Definitions

all_assets = [
    *bronze_layer_assets,
    *silver_layer_assets,
    *gold_layer_assets
]

defs = Definitions(
    assets=all_assets,
    schedules=[bronze_assets_schedule, silver_assets_schedule, gold_assets_schedule],
    jobs=[bronze_assets_job, gold_assets_job, silver_assets_job],
    resources=my_resources 
)
