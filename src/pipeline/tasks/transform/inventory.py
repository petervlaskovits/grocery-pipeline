from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, upper, create_map, lit, coalesce
from utils.cleaning import map_and_clean_column
from autocorrect import Speller

from prefect import task

store_location_map = {
    'WST PLAZA': 'WEST PLAZA',
    'WST MAIN ST': 'WEST MAIN ST',
    'WST CORNER': 'WEST CORNER',
    'SOTH PLAZA': 'SOUTH PLAZA',
    'SOTH MAIN ST': 'SOUTH MAIN ST',
    'SOTH CORNER': 'SOUTH CORNER',
    'NRTH PLAZA': 'NORTH PLAZA',
    'NRTH MAIN ST': 'NORTH MAIN ST',
    'EAS PLAZA': 'EAST PLAZA',
    'EAS MAIN ST': 'EAST MAIN ST',
    'EAS CORNER': 'EAST CORNER'
}

markdown_flag_map = {
    'N': 'FALSE',
    '0': 'FALSE',
    'NO': 'FALSE',
    'Y': 'TRUE',
    '1': 'TRUE',
    'YES': 'TRUE'
}

def clean_categories(df: DataFrame):
    cleaned = df.withColumn(
        'category', 
        upper(col('category'))
    )
    return cleaned

def clean_suppliers(df: DataFrame):
    cleaned = df.withColumn(
        'supplier_name',
        upper(col('supplier_name'))
    )
    return cleaned

@task
def transform_inventory(raw_df: DataFrame):
    return map_and_clean_column(raw_df, 'store_location', store_location_map)