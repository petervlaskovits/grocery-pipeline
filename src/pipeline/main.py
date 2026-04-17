from prefect import flow, get_run_logger
from pyspark.sql import SparkSession

from tasks.extract import extract_table

from tasks.transform.inventory import clean_inventory
from tasks.transform.transactions import clean_transactions
from tasks.transform.suppliers import clean_suppliers

@flow
def extract():
    future_transactions = extract_table.submit('transactions')
    future_suppliers = extract_table.submit('supplier_orders')
    future_inventory = extract_table.submit('inventory')

    transactions_df = future_transactions.result()
    suppliers_df = future_suppliers.result()
    inventory_df = future_inventory.result()

    return transactions_df, suppliers_df, inventory_df

@flow
def clean(transactions, suppliers, inventory):
    future_clean_transactions = clean_transactions.submit(transactions)
    future_clean_suppliers = clean_suppliers.submit(suppliers)
    future_clean_inventory = clean_inventory.submit(inventory)

    cleaned_transactions = future_clean_transactions.result()
    cleaned_suppliers = future_clean_suppliers.result()
    cleaned_inventory = future_clean_inventory.result()

    return cleaned_transactions, cleaned_suppliers, cleaned_inventory

@flow
def transform():
    pass

@flow
def load():
    pass

@flow
def pipeline():
    logger = get_run_logger()
    raw_transactions, raw_suppliers, raw_inventory = extract()
    cleaned_transactions, cleaned_suppliers, cleaned_inventory = clean(raw_transactions, raw_suppliers, raw_inventory)
    logger.info(cleaned_inventory.head(15))
    logger.info(cleaned_suppliers.head(15))

if __name__ == "__main__":
    pipeline.serve(
        name='grocery-pipeline',
        #cron='0 6 * * 0',
        version='1.0.0'
    )