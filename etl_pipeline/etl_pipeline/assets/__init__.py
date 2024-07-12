from dagster import load_assets_from_package_module
from . import bronze, gold, silver

bronze_layer_assets = load_assets_from_package_module(
    package_module=bronze,
)

silver_layer_assets = load_assets_from_package_module(
    package_module=silver,
)

gold_layer_assets = load_assets_from_package_module(
    package_module=gold,
)