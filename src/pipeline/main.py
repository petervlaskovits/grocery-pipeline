from prefect import flow, get_run_logger
from pyspark.sql import SparkSession

from tasks.extract import extract_table
from tasks.transform.inventory import transform_inventory

@flow
def extract():
    future_transactions = extract_table.submit('transactions')
    future_suppliers = extract_table.submit('supplier_orders')
    future_inventory = extract_table.submit('inventory')

    transactions_df = future_transactions.result()
    suppliers_df = future_suppliers.result()
    inventory_df = future_inventory.result()

    return (transactions_df, suppliers_df, inventory_df)

@flow
def transform(transactions, suppliers, inventory):
    cleaned_inventory = transform_inventory(inventory)
    return cleaned_inventory

@flow
def load():
    pass

@flow
def pipeline():
    logger = get_run_logger()
    dataframes = extract()
    cleaned_inventory = transform(None, None, dataframes[2])
    logger.info(cleaned_inventory.head(15))

if __name__ == "__main__":
    pipeline.serve(
        name='grocery-pipeline',
        #cron='0 6 * * 0',
        version='1.0.0'
    )