from prefect import task
from prefect.logging import get_run_logger
from prefect.cache_policies import NO_CACHE

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, upper, regexp_replace, try_to_date, try_to_timestamp

from pipeline.utils.cleaning import category_map, store_location_map, flags_map, map_lookup_to_df, uppercase_columns_for_mapping
from pipeline.utils.quality_checks import suppliers_schema, apply_schema

@task(cache_policy=NO_CACHE)
def clean_and_validate_suppliers(raw_df: DataFrame) -> DataFrame:
    """Cleans and validates the suppliers DataFrame.

    Args:
        raw_df (DataFrame): The raw suppliers DataFrame to be cleaned and validated.

    Returns:
        DataFrame: The cleand and validated suppliers DataFrame.
    """

    logger = get_run_logger()
    logger.info("Transforming suppliers table...")

    before = raw_df.count()

    cleaned_qty_received = raw_df.withColumn("qty_received", 
        regexp_replace("qty_received", " units", "").try_cast(IntegerType())
    )

    cleaned_order_dates = cleaned_qty_received.withColumns({
        "order_date": try_to_timestamp("order_date"),
        "expected_delivery_date": try_to_date("expected_delivery_date"),
        "actual_delivery_date": try_to_timestamp("actual_delivery_date")
    })

    cleaned_flag = map_lookup_to_df(cleaned_order_dates, flags_map, "invoice_matched_flag")
    uppered = uppercase_columns_for_mapping(cleaned_flag)

    cleaned_categories = map_lookup_to_df(uppered, category_map, "category")
    cleaned_suppliers = map_lookup_to_df(cleaned_categories, store_location_map, "store_location")

    logger.info("Finished transformations, validating suppliers table...")
    validated, validation_errors = apply_schema(suppliers_schema, cleaned_suppliers)

    after = validated.count()

    if before != after:
        logger.warn("Imbalanced row counts for inventory table after transformation!")

    if validation_errors != "{}":
        logger.error(validation_errors)
    else:
        logger.info("Table successfully validated")

    logger.info({
        "before_transform_row_count": before,
        "after_transform_row_count": after
    })

    return validated, validation_errors