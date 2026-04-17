from pyspark.sql import DataFrame
from pyspark.sql.functions import col, try_to_date, try_to_timestamp

from utils.cleaning import map_and_clean_column, clean_suppliers, flags_map, store_location_map, category_map

from prefect import task

@task(tags=['clean'])
async def clean_inventory(raw_df: DataFrame):
    markdown = map_and_clean_column(raw_df, 'markdown_flag', flags_map)
    location = map_and_clean_column(markdown, 'store_location', store_location_map)
    categories = map_and_clean_column(location, 'category', category_map)
    timestamp_converted = categories.withColumns({
        "last_restock_date": try_to_timestamp(col("last_restock_date")),
        "expiry_date": try_to_date(col("expiry_date"))
    })
    cleaned = clean_suppliers(timestamp_converted)
    return cleaned