from prefect import task, get_run_logger
from prefect.blocks.system import Secret

from pyspark.sql import SparkSession

@task(retries=3, retry_delay_seconds=15, tags=["extract_tables"])
def extract_table(table_name: str):
    spark = SparkSession.builder.appName("Grocery Pipeline").getOrCreate()
    logger = get_run_logger()
    
    login_credentials_block = Secret.load('retail-db-credentials')
    login_credentials = login_credentials_block.get()

    transactions_df = spark.read.jdbc(
        url='jdbc:postgresql://localhost:5432/retail', 
        table=table_name,
        properties={
            'user': login_credentials['user'],
            'password': login_credentials['password']
        }
    )

    try:
        logger.info(transactions_df.head(1)) # verify that we successfully loaded the DataFrame
    except Exception as e:
        logger.error(f"Failed to extract table {table_name}")
        logger.error(e)
        transactions_df = None

    return transactions_df