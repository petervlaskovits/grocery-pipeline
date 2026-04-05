from prefect import flow, get_run_logger
from pyspark.sql import SparkSession
from tasks.extract import extract_table

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
def pipeline():
    dataframes = extract()

if __name__ == "__main__":
    pipeline.serve(
        name='grocery-pipeline',
        #cron='0 6 * * 0',
        version='1.0.0'
    )