from prefect import task
from prefect.logging import get_run_logger
from prefect.cache_policies import NO_CACHE

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, try_to_date, try_to_timestamp

from pipeline.utils.cleaning import store_location_map, flags_map, category_map, uppercase_columns_for_mapping, map_lookup_to_df
from pipeline.utils.quality_checks import inventory_schema, apply_schema

@task(cache_policy=NO_CACHE)
def clean_and_validate_inventory(raw_df: DataFrame) -> DataFrame:
    """Cleans and validates the inventory DataFrame.

    Args:
        raw_df (DataFrame): The raw inventory DataFrame to be cleaned and validated.

    Returns:
        DataFrame: The cleaned and validated inventory DataFrame.
    """
    logger = get_run_logger()
    logger.info("Transforming inventory table...")

    before = raw_df.count()

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

    logger.info("Finished transformations, validating inventory table...")
    validated, validation_errors = apply_schema(inventory_schema, cleaned)

    after = validated.count()

    if validation_errors != "{}":
        logger.error(validation_errors)
    else:
        logger.info("Table successfully validated")


    if before != after:
        logger.warn("Imbalanced row counts for inventory table after transformation!")

    logger.info({
        "before_transform_row_count": before,
        "after_transform_row_count": after
    })

    return validated, validation_errors