from prefect import flow, get_run_logger
from pyspark.sql import SparkSession, DataFrame
from tasks.extract import extract_table

from tasks.transform.inventory import clean_inventory
from tasks.transform.suppliers import clean_suppliers
from tasks.transform.transactions import clean_transactions

@flow
def extract():
    """
    Extracts all of the PostgreSQL tables from the retail database using extract_table.

    Returns:
    list[DataFrame, DataFrame, DataFrame]: All of the original PostgreSQL tables in the following order: transactions, supplier_orders, and inventory.

    """

    logger = get_run_logger()

    logger.info("Starting to extract all tables...")

    transactions_df = extract_table("transactions")
    suppliers_df = extract_table("supplier_orders")
    inventory_df = extract_table("inventory")

    logger.info("Finished extracting all tables")

    return [transactions_df, suppliers_df, inventory_df]


@flow
def transform(raw_dfs: list[DataFrame, DataFrame, DataFrame]) -> list[DataFrame, DataFrame, DataFrame]:
    """
    Transforms all raw DataFrames from the raw DataFrames list from extract with a task that appropriately transforms a single table based on what it is.

    Parameters:
    raw_dfs (list): The list of raw DataFrames from extract().

    Returns:
    list[DataFrame, DataFrame, DataFrame]: All of the cleaned DataFrames in the following order: transactions, supplier_orders, and inventory.
    """

    logger = get_run_logger()
    logger.info("Beginning transformation process...")

    cleaned_transactions = clean_transactions(raw_dfs[0])
    #cleaned_suppliers = clean_suppliers(raw_dfs[1])
    #cleaned_inventory = clean_inventory(raw_dfs[2])

    logger.info("Finished transformation process")
    return [cleaned_transactions]#, cleaned_suppliers, cleaned_inventory]


@flow
def pipeline():
    logger = get_run_logger()

    logger.info("Pipeline started")

    spark = (
        SparkSession.builder.appName("Grocery Pipeline")
        .config("spark.jars", "/home/peter/spark/jars/postgresql-42.7.10.jar")
        .config(
            "spark.driver.extraClassPath",
            "/home/peter/spark/jars/postgresql-42.7.10.jar",
        )
        .getOrCreate()
    )

    raw_dataframes = extract()

    cleaned_dfs = transform(raw_dataframes)
    
    cleaned_transactions = cleaned_dfs[0]
    #cleaned_suppliers = cleaned_dfs[1]
    #cleaned_inventory = cleaned_dfs[2]

    spark.stop()

if __name__ == "__main__":
    pipeline()
