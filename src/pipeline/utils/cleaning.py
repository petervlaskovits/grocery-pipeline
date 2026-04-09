from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, upper, create_map, lit, coalesce

from itertools import chain

def map_and_clean_column(df: DataFrame, col_name: Column, map: dict):
    mapping = create_map(
        [lit(map_col) for map_col in chain(*map.items())]
    )

    cleaned = df.withColumn(
        col_name,
        coalesce(mapping[col(col_name)], upper(col(col_name)))
    )

    return cleaned