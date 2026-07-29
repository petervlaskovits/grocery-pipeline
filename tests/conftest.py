import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.13")
        .getOrCreate()
    )