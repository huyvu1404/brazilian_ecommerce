from dagster import  ScheduleDefinition
from ..jobs import bronze_assets_job, gold_assets_job, silver_assets_job

bronze_assets_schedule = ScheduleDefinition(
    name="bronze_assets_schedule",
    job = bronze_assets_job,
    cron_schedule="0 0 * * 1"
)

silver_assets_schedule = ScheduleDefinition(
    name="silver_assets_schedule",
    job = silver_assets_job,
    cron_schedule="0 0 * * 1"
)

gold_assets_schedule = ScheduleDefinition(
    name="gold_assets_schedule",
    job = gold_assets_job,
    cron_schedule="0 0 * * 1"
)
