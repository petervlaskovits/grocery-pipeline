from pyspark.sql import DataFrame
from utils.cleaning import map_and_clean_column, clean_suppliers, flags_map, store_location_map, category_map

from prefect import task

@task(tags=['clean'])
async def clean_inventory(raw_df: DataFrame):
    markdown = map_and_clean_column(raw_df, 'markdown_flag', flags_map)
    location = map_and_clean_column(markdown, 'store_location', store_location_map)
    categories = map_and_clean_column(location, 'category', category_map)
    cleaned = clean_suppliers(categories)
    return cleaned