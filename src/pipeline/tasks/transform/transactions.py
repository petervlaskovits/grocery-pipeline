from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect.logging import get_run_logger

from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, col, regexp_extract, format_string, upper, when, try_to_timestamp

from pipeline.utils.cleaning import store_location_map, flags_map, category_map, map_lookup_to_df, uppercase_columns_for_mapping
from pipeline.utils.quality_checks import transactions_schema, apply_schema

def standardize_customer_ids(raw_df: DataFrame) -> DataFrame:
    """Uses Regex to clean up and standardize customer IDs from messy values to a consistent format (CUST-1234).
    Args:
        raw_df (DataFrame): The DataFrame that contains the customer_id column, to be cleaned up.

    Returns:
        DataFrame: The cleaned DataFrame with the standardized customer IDs.
    """

    return raw_df.withColumn(
        "customer_id",
        upper(
            regexp_replace(
                col("customer_id"),
                r"CUST[0-9]{4}",
                format_string('CUST-%s', regexp_extract(col('customer_id'), r'[0-9]{4}', 0))
            )
        )
    ).fillna("N/A", subset='customer_id')

@task(tags=['clean'], cache_policy=NO_CACHE)
def clean_and_validate_transactions(raw_df: DataFrame) -> DataFrame:
    """Cleans up and validates the transactions DataFrame.

    Args:
        raw_df (DataFrame): The raw transactions DataFrame to be cleaned up.

    Returns:
        DataFrame: The cleaned and validated transactions DataFrame.
    """
    logger = get_run_logger()
    logger.info("Transforming transactions table...")

    before = raw_df.count()

    cleaned_customers = standardize_customer_ids(raw_df)
    cleaned_payments = cleaned_customers.withColumn("payment_method",
        upper(col("payment_method"))
    )

    uppered = uppercase_columns_for_mapping(cleaned_payments)
    mapped_locations = map_lookup_to_df(uppered, store_location_map, 'store_id')
    mapped_flags = map_lookup_to_df(mapped_locations, flags_map, 'return_flag')
    cleaned = map_lookup_to_df(mapped_flags, category_map, 'category')

    consistent_returns = cleaned.withColumns(
        {
            "quantity_sold": when(
                col("return_flag") == "TRUE", -1 * col("quantity_sold")
            ).otherwise(col("quantity_sold")),
            "unit_price_usd": when(
                col("return_flag") == "TRUE", -1 * col("unit_price_usd")
            ).otherwise(col("unit_price_usd")),
            "total_sales_usd": when(
                col("return_flag") == "TRUE", -1 * col("total_sales_usd")
            ).otherwise(col("total_sales_usd"))
        }
    ).withColumn("quantity_sold",
        when(
                (col("return_flag") == "FALSE") & (col("quantity_sold") <= 0), 
                -1 * col("quantity_sold")
            )
            .otherwise(col("quantity_sold")
        )
    )

    cleaned_dates = consistent_returns.withColumn(
        "transaction_date",
        try_to_timestamp(col("transaction_date"))
        
    )

    logger.info("Finished transformations, validating transactions table...")
    validated, validation_errors = apply_schema(transactions_schema, cleaned_dates)
    after = validated.count()

    if before != after:
        logger.warn("Imbalanced row counts for inventory table after transformation!")
    
    if validation_errors != "{}":
        logger.error(validation_errors)
    else:
        logger.info("Table successfully validated")


    logger.info(({
        "before_transform_row_count": before,
        "after_transform_row_count": after
    }))

    return validated, validation_errors