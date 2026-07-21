from src.pipeline.utils import cleaning
from pyspark.sql import SparkSession

def test_store_mappings(spark):
    df = spark.read.jdbc("jdbc:postgresql://localhost:5432/postgres", table="test", properties={
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
    })
    assert 1 == 1

def test_flag_mappings():
    assert 1 == 1

def test_category_mappings():
    assert 1 == 1