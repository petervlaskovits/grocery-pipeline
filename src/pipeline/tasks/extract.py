from prefect import get_run_logger, task
from prefect.blocks.system import Secret

from pyspark.sql import SparkSession, DataFrame


@task(retries=2, retry_delay_seconds=5, tags=["extract"])
def extract_table(table_name: str) -> DataFrame:
    """
    Extracts a single individual table from the retail database, using login credentials from the retail-db-credentials Secrets block.
    If the table fails to be extracted, then this task will restart 2 times until it succeeds or fails.

    Args:
        table_name (str): The name of the table inside a PostgreSQL database to be extracted.

    Raises:
        Exception: Base exception that is raised if the DataFrame fails to be extracted.

    Returns:
        DataFrame: If the DataFrame is successfully extracted, a DataFrame containing data from the PostgreSQL table is returned. 
    """
    spark = SparkSession.getActiveSession()

    logger = get_run_logger()

    login_credentials_block = Secret.load("db-creds")
    login_credentials = login_credentials_block.get()

    logger.info(f"Extracting table {table_name}")

    try:
        df = spark.read.jdbc(
            url="jdbc:postgresql://localhost:5432/retail",
            table=table_name,
            properties={
                "user": login_credentials["user"],
                "password": login_credentials["password"],
            },
        )
        logger.info(f"Successfully extracted table {table_name}")
        return df
    except Exception as e: # might be a terrible idea, but at the same time I can't just keep the entire pipeline running when accessing one table fails...
        logger.error(f"Failed to extract table {table_name}")
        raise e