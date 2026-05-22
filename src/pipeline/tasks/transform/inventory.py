from prefect import task
from prefect.logging import get_run_logger
from prefect.cache_policies import NO_CACHE

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, try_to_date, try_to_timestamp

from utils.cleaning import store_location_map, flags_map, category_map, uppercase_columns_for_mapping, map_lookup_to_df
from utils.quality_checks import inventory_schema, apply_schema

import json

@task(cache_policy=NO_CACHE)
def clean_inventory(raw_df: DataFrame) -> DataFrame:
    logger = get_run_logger()

    uppercased = uppercase_columns_for_mapping(raw_df)
    markdown_cleaned = map_lookup_to_df(uppercased, flags_map, 'markdown_flag')
    category_cleaned = map_lookup_to_df(markdown_cleaned, category_map, 'category')
    location_cleaned = map_lookup_to_df(category_cleaned, store_location_map, 'store_location')

    cleaned_units_days = location_cleaned.withColumns(
        {
            'units_on_hand': when(col('units_on_hand') < 0, None).otherwise(col('units_on_hand')),
            'days_on_shelf': when(col('days_on_shelf') < 0, None).otherwise(col('days_on_shelf')) 
        }
    )

    cleaned = cleaned_units_days.withColumns({
        'expiry_date': try_to_date(col('expiry_date')),
        'last_restock_date': try_to_timestamp(col('last_restock_date'))
    })

    validated, validation_errors = apply_schema(inventory_schema, cleaned)
    logger.error(validation_errors)

    return validated