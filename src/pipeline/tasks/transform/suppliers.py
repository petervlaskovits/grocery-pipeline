from prefect import task
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType
from pyspark.sql.functions import col, regexp_replace, try_to_timestamp, try_to_date

from utils.cleaning import map_and_clean_column, category_map, store_location_map, flags_map

def transform_quantity_recieved(raw_df: DataFrame):
    return raw_df.withColumns({
        "order_is_pending": raw_df["qty_received"].like("pending"), # worth including because we lose this data after it gets casted to a null 
        "qty_received": regexp_replace(
            col("qty_received"), r" units", ""
        ).try_cast(LongType()),
    })

@task(tags=['clean'])
async def clean_suppliers(raw_df: DataFrame):
    pending = transform_quantity_recieved(raw_df)
    dates_transfomed = pending.withColumn({
            "order_date": try_to_timestamp(col("order_date")),
            "actual_delivery_date": try_to_timestamp(col("actual_delivery_date")),
            "expected_delivery_date": try_to_date(col("expected_delivery_date"))
        }
    )
    suppliers_cleaned = clean_suppliers(dates_transfomed)
    categories_cleaned = map_and_clean_column(suppliers_cleaned, "category", category_map)
    stores_cleaned = map_and_clean_column(categories_cleaned, "store_location", store_location_map)
    suppliers_df = map_and_clean_column(stores_cleaned, "invoice_matched_flag", flags_map)
    return suppliers_df