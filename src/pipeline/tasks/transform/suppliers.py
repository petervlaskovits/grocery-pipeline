from prefect import task
from prefect.logging import get_run_logger
from prefect.cache_policies import NO_CACHE

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, upper, regexp_replace, try_to_date, try_to_timestamp

from utils.cleaning import category_map, store_location_map, flags_map, map_lookup_to_df, uppercase_columns_for_mapping
from utils.quality_checks import suppliers_schema, apply_schema

@task(cache_policy=NO_CACHE)
def clean_suppliers(raw_suppliers_df: DataFrame) -> DataFrame:
    """
    Cleans up the suppliers DataFrame to make it prepared for further data analysis.

    Parameters:
    raw_suppliers_df: The raw suppliers DataFrame.

    Returns:
    cleaned_suppliers: The cleaned suppliers DataFrame.
    """

    logger = get_run_logger()
    logger.info("Transforming suppliers table...")

    cleaned_qty_received = raw_suppliers_df.withColumn("qty_received", 
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

    validated, validation_errors = apply_schema(suppliers_schema, cleaned_suppliers)

    if validation_errors != "{}":
        logger.error(validation_errors)
    else:
        logger.info("Table successfully validated")


    return validated