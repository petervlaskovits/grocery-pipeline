from prefect import task
from prefect.cache_policies import NO_CACHE

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, upper, regexp_replace, try_to_timestamp
from utils.cleaning import category_map, store_location_map, flags_map, assign_lookup_df, uppercase_columns_for_mapping

@task(cache_policy=NO_CACHE)
def clean_suppliers(raw_suppliers_df: DataFrame) -> DataFrame:
    """
    """
    cleaned_qty_received = raw_suppliers_df.withColumn("qty_received", 
        regexp_replace("qty_received", " units", "").try_cast(IntegerType())
    )

    cleaned_order_date = cleaned_qty_received.withColumn("order_date", 
        try_to_timestamp("order_date")
    )

    cleaned_flag = assign_lookup_df(cleaned_order_date, flags_map, "invoice_matched_flag")
    uppered = uppercase_columns_for_mapping(cleaned_flag)

    cleaned_categories = assign_lookup_df(uppered, category_map, "category")
    cleaned_locations = assign_lookup_df(cleaned_categories, store_location_map, "store_location")

    return cleaned_locations