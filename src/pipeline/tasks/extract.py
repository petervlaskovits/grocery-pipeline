from prefect import get_run_logger, task
from prefect.blocks.system import Secret
from pyspark.sql import SparkSession


@task(retries=2, retry_delay_seconds=5, tags=["extract"])
def extract_table(table_name: str):
    """
    Extracts a single individual table from the retail database, using login credentials from the retail-db-credentials Secrets block.
    If the table fails to be extracted, then this task will restart 2 times until it succeeds or fails.

    Parameters:
    table_name (str): The name of the table to be extracted.

    Returns:
    DataFrame: DataFrame of the table. If the table fails to be extracted
    """
    spark = SparkSession.getActiveSession()

    logger = get_run_logger()

    login_credentials_block = Secret.load("retail-db-credentials")
    login_credentials = login_credentials_block.get()

    df = spark.read.jdbc(
        url="jdbc:postgresql://localhost:5432/retail",
        table=table_name,
        properties={
            "user": login_credentials["user"],
            "password": login_credentials["password"],
        },
    )

    try:
        logger.info(df.head(1))  # verify that we successfully loaded the DataFrame
    except Exception as e:
        logger.error(f"Failed to extract table {table_name}")
        raise Exception(e)

    return df
