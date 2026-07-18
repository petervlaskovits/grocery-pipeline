from prefect import flow, get_run_logger
from pyspark.sql import SparkSession, DataFrame

from tasks.extract import extract_table

from tasks.transform.inventory import clean_and_validate_inventory
from tasks.transform.suppliers import clean_and_validate_suppliers
from tasks.transform.transactions import clean_and_validate_transactions

from tasks.load import load_dataframe_to_s3

import time

@flow
def extract() -> list[DataFrame, DataFrame, DataFrame]:
    """A subflow for the extract phase of the data pipeline. 
    Extracts the transactions, supplier_orders, and inventory tables from a PostgreSQL database.

    Returns:
        list[DataFrame, DataFrame, DataFrame]: A list of the raw DataFrames in the following order: raw_transactions_df, raw_suppliers_df, raw_inventory_df.
    """

    logger = get_run_logger()

    logger.info("Starting to extract all tables...")

    raw_transactions_df = extract_table("transactions")
    raw_suppliers_df = extract_table("supplier_orders")
    raw_inventory_df = extract_table("inventory")

    logger.info("Finished extracting all tables")

    return [raw_transactions_df, raw_suppliers_df, raw_inventory_df]


@flow
def transform(raw_dfs: list[DataFrame, DataFrame, DataFrame]) -> list[DataFrame, DataFrame, DataFrame]:
    """A subflow for the transformation phase of the data pipeline. Runs table-specific transformation and validation tasks.

    Args:
        raw_dfs (list[DataFrame, DataFrame, DataFrame]): The list of raw DataFrames from the extract subflow.

    Returns:
        list[DataFrame, DataFrame, DataFrame]: A list of the transformed and validated DataFrames in the following order: cleaned_transactions_df, cleaned_suppliers_df, cleaned_inventory_df.
    """

    logger = get_run_logger()
    logger.info("Beginning transformation process...")

    cleaned_transactions_df = clean_and_validate_transactions(raw_dfs[0])
    cleaned_suppliers_df = clean_and_validate_suppliers(raw_dfs[1])
    cleaned_inventory_df = clean_and_validate_inventory(raw_dfs[2])

    logger.info("Finished transformation process")
    return [cleaned_transactions_df, cleaned_suppliers_df, cleaned_inventory_df]

@flow
def load(validated_dfs: list[DataFrame, DataFrame, DataFrame]):
    """A subflow for the load phase of the data pipeline. Just loads Parquet partitions to an S3 bucket folder specific to each table."""
    load_dataframe_to_s3(validated_dfs[0], "transactions")
    load_dataframe_to_s3(validated_dfs[1], "suppliers")
    load_dataframe_to_s3(validated_dfs[2], "inventory")


@flow
def pipeline():
    """
    The main pipeline flow. Extracts everything, transforms everything, and then loads it into an S3 bucket with Parquet files, where it will be used by Redshift.
    """
    logger = get_run_logger()

    logger.info("Pipeline started")

    spark = (
        SparkSession.builder.appName("Grocery Pipeline")
        .config("spark.jars", 
        "/home/peter/spark/jars/postgresql-42.7.10.jar",
        )
        .config(
            "spark.driver.extraClassPath",
            "/home/peter/spark/jars/postgresql-42.7.10.jar"
        )
        .getOrCreate()
    )

    raw_dataframes = extract()

    cleaned_dfs = transform(raw_dataframes)
    
    load(cleaned_dfs)

    spark.stop()

if __name__ == "__main__":
    pipeline()