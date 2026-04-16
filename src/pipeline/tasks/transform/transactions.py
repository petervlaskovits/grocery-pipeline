from prefect import task
from pyspark.sql import DataFrame

@task(tags=['clean'])
async def clean_transactions(raw_df: DataFrame):
    pass