from .resources import my_resources
from .assets import (
    bronze_layer_assets,
    silver_layer_assets,
    gold_layer_assets
)
from .jobs import etl_schedule
from dagster import Definitions


all_assets = [
    *bronze_layer_assets,
    *silver_layer_assets,
    *gold_layer_assets
]

defs = Definitions(
    assets=all_assets,
    resources=my_resources,
    schedules=[etl_schedule]
)
