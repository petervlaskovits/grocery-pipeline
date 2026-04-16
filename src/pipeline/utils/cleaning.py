from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, upper, create_map, lit, coalesce

from itertools import chain

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

flags_map = {
    'N': 'FALSE',
    '0': 'FALSE',
    'NO': 'FALSE',
    'Y': 'TRUE',
    '1': 'TRUE',
    'YES': 'TRUE'
}

category_map = {
    'ELECTROICS': 'ELECTRONICS',
    'DAIRRY': 'DAIRY',
    'BAKREY': 'BAKERY',
    'HOUSHOLD': 'HOUSEHOLD',
    'PESONAL CARE': 'PERSONAL CARE',
    'SNAKS': 'SNACKS',
    'BEVERAGSE': 'BEVERAGES',
    'MEA': 'MEAT',
    'LOTERY': 'LOTTERY',
    'PRODCE': 'PRODUCE',
    'TOBACO': 'TOBACCO',
    'FROZN': 'FROZEN'
}

def clean_suppliers(df: DataFrame):
    cleaned = df.withColumn(
        'supplier_name',
        upper(col('supplier_name'))
    )
    return cleaned

def map_and_clean_column(df: DataFrame, col_name: Column, map: dict):
    mapping = create_map(
        [lit(map_col) for map_col in chain(*map.items())]
    )

    cleaned = df.withColumn(
        col_name,
        coalesce(mapping[col(col_name)], upper(col(col_name)))
    )

    return cleaned