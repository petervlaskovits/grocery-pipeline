from prefect import task
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType, DatetimeType, DateType
from pyspark.sql.functions import col, regexp_replace

from utils.cleaning import map_and_clean_column, category_map, store_location_map, flags_map

def transform_quantity_recieved(raw_df: DataFrame):
    return raw_df.withColumns({
        "order_is_pending": raw_df["qty_recieved"].like("pending"), # worth including because we lose this data after it gets casted to a null 
        "qty_recieved": regexp_replace(
            col("qty_recieved"), r" units", ""
        ).try_cast(LongType()),
    })

@task(tags=['clean'])
async def clean_suppliers(raw_df: DataFrame):
    pending = transform_quantity_recieved(raw_df)
    dates_transfomed = pending.withColumn({
            "order_date": col("order_date").try_cast(DatetimeType()),
            "actual_delivery_date": col("actual_delivery_date").try_cast(DatetimeType()),
            "expected_delivery_date": col("expected_delivery_date").try_cast(DateType())
        }
    )
    suppliers_cleaned = clean_suppliers(dates_transfomed)
    categories_cleaned = map_and_clean_column(suppliers_cleaned, "category", category_map)
    stores_cleaned = map_and_clean_column(categories_cleaned, "store_location", store_location_map)
    suppliers_df = map_and_clean_column(stores_cleaned, "invoice_matched_flag", flags_map)
    return suppliers_df