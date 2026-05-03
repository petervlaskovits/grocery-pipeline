from prefect import task
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, col, regexp_extract, format_string

def standardize_customers(raw_df: DataFrame):
    return raw_df.withColumn(
        "customer_id",
        regexp_replace(col("customer_id"), r"CUST[0-9]{4}", regexp_extract(col("customer_id"), r"[0-9]{4}"))
    )

@task(tags=['clean'])
def clean_transactions(raw_df: DataFrame):
    return None