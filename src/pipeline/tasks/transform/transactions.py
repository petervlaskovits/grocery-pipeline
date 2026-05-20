from prefect import task
from prefect.cache_policies import NO_CACHE

from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, col, regexp_extract, format_string, upper, when, try_to_timestamp
from utils.cleaning import store_location_map, flags_map, category_map, map_lookup_to_df, uppercase_columns_for_mapping

def standardize_customer_ids(raw_df: DataFrame) -> DataFrame:
    """
    Uses Regex to clean up and standardize customer IDs in the customer_id column.

    Parameters:
    raw_df: The DataFrame to be cleaned up, with the customer_id column.

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
def clean_transactions(raw_df: DataFrame) -> DataFrame:
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
            (col("return_flag") == "FALSE") & (col("quantity_sold") <= 0), -1 * col("quantity_sold")).otherwise(col("quantity_sold"))
    )

    cleaned_dates = consistent_returns.withColumn(
        "transaction_date",
        try_to_timestamp(col("transaction_date"))
        
    )

    return consistent_returns