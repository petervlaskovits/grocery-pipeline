from prefect import flow, get_run_logger
from prefect.futures import wait
from pyspark.sql import SparkSession
from tasks.extract import extract_table
from tasks.transform.inventory import clean_inventory
from tasks.transform.suppliers import clean_suppliers
from tasks.transform.transactions import clean_transactions


@flow
def extract(spark: SparkSession):
    transactions_df = extract_table(spark, "transactions")
    suppliers_df = extract_table(spark, "supplier_orders")
    inventory_df = extract_table(spark, "inventory")

    return transactions_df, suppliers_df, inventory_df


@flow
def clean(transactions, suppliers, inventory):
    cleaned_transactions = clean_transactions(transactions)
    cleaned_suppliers = clean_suppliers(suppliers)
    cleaned_inventory = clean_inventory.submit(inventory)

    return cleaned_transactions, cleaned_suppliers, cleaned_inventory


@flow
def transform():
    pass


@flow
def load():
    pass


@flow
def pipeline():
    spark = (
        SparkSession.builder.appName("Grocery Pipeline")
        .config("spark.jars", "file:///C:/spark/jars/postgresql-42.7.10.jar")
        .getOrCreate()
    )

    logger = get_run_logger()
    raw_transactions, raw_suppliers, raw_inventory = extract(spark)
    cleaned_transactions, cleaned_suppliers, cleaned_inventory = clean(
        raw_transactions, raw_suppliers, raw_inventory
    )
    logger.info(cleaned_inventory.head(15))
    logger.info(cleaned_suppliers.head(15))


if __name__ == "__main__":
    pipeline.serve(
        name="grocery-pipeline",
        # cron='0 6 * * 0',
        version="1.0.0",
    )
