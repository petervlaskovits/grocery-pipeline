from prefect import get_run_logger, task
from prefect.cache_policies import NO_CACHE

from prefect.blocks.system import Secret

from pyspark.sql import DataFrame

import boto3
import os
import time

@task(cache_policy=NO_CACHE)
def load_dataframe_to_s3(df: DataFrame) -> None:
    """Loads a single DataFrame into an S3 bucket by turning a DataFrame into a collection of Parquet files on the local disk, then uploads each individual file into an S3 bucket. 

    Args:
        df (DataFrame): The DataFrame to be uploaded to S3.
    """

    logger = get_run_logger()

    s3_bucket_name_block = Secret.load("s3-bucket-name")
    s3_bucket_name = s3_bucket_name_block.get()

    aws_creds_block = Secret.load("aws-creds")
    aws_creds = aws_creds_block.get()

    s3 = boto3.client("s3", aws_access_key_id=aws_creds["access_key"], aws_secret_access_key=aws_creds["secret_key"])

    df_name = "inventory"

    if "order_id" in df.columns:
        df_name = "suppliers"
    elif "transaction_id" in df.columns:
        df_name = "transactions"

    path = f"tmp/{df_name}"

    logger.info(f"Writing {df_name}")
    df.write.mode("overwrite").parquet(path)

    # Wrangled around with JARs trying to do this natively, but it was frustrating to wrangle the "60s" invalid number error,
    # so I decided to just write the Parquet file locally first then upload the Parquet files to an S3 bucket.
    # If this were in the cloud and the JARs were automatically handled then I would've used the JAR approach.

    logger.info(f"Attempting to upload {df_name} Parquet files")
    for root, dirs, files in os.walk(path):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = f"data/{df_name}/{file}"
            try:
                s3.upload_file(local_path, s3_bucket_name, s3_key)
            except Exception as e:
                logger.error(f"Failed to upload {file} to S3 bucket")
                raise e
        
            time.sleep(5) 

    s3.close()
    