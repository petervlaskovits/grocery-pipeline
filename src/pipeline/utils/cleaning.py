from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, coalesce, upper

# All mappings to clean the store location column.
store_location_map = [
    ("WST PLAZA", "WEST PLAZA"),
    ("WST MAIN ST", "WEST MAIN ST"),
    ("WST CORNER", "WEST CORNER"),
    ("SOTH PLAZA", "SOUTH PLAZA"),
    ("SOTH MAIN ST", "SOUTH MAIN ST"),
    ("SOTH CORNER", "SOUTH CORNER"),
    ("NRTH PLAZA", "NORTH PLAZA"),
    ("NRTH MAIN ST", "NORTH MAIN ST"),
    ("EAS PLAZA", "EAST PLAZA"),
    ("EAS MAIN ST", "EAST MAIN ST"),
    ("EAS CORNER", "EAST CORNER"),
]

# All mappings to clean any flag column.
flags_map = [
    ("N", "FALSE"),
    ("n", "FALSE"),
    ("0", "FALSE"),
    ("NO", "FALSE"),
    ("No", "FALSE"),
    ("Y", "TRUE"),
    ("y", "TRUE"),
    ("1", "TRUE"),
    ("YES", "TRUE"),
    ("Yes", "TRUE")
]

# All mappings to clean the category column.
category_map = [
    ("ELECTROICS", "ELECTRONICS"),
    ("DAIRRY", "DAIRY"),
    ("BAKREY", "BAKERY"),
    ("HOUSHOLD", "HOUSEHOLD"),
    ("PESONAL CARE", "PERSONAL CARE"),
    ("SNAKS", "SNACKS"),
    ("BEVERAGSE", "BEVERAGES"),
    ("MEA", "MEAT"),
    ("LOTERY", "LOTTERY"),
    ("PRODCE", "PRODUCE"),
    ("TOBACO", "TOBACCO"),
    ("FROZN", "FROZEN"),
]

def create_lookup_df(lookup_map: list):
    spark = SparkSession.getActiveSession()
    lookup_df = spark.createDataFrame(lookup_map, ["dirty", "clean"])
    return lookup_df

def assign_lookup_df(original_df: DataFrame, lookup_map: list, column_name: str):
    lookup_df = create_lookup_df(lookup_map)
    cleaned_df = original_df.join(
        lookup_df, original_df[column_name] == lookup_df["dirty"],
        how='left'
    ).withColumn(
        column_name, 
        coalesce(col("clean"), col(column_name))
    ).drop("dirty", "clean")
    return cleaned_df

def uppercase_columns_for_mapping(original_df: DataFrame):
    if ('store_location' in original_df.columns) == True:
        return original_df.withColumns(
            {
                'supplier_name': upper(col('supplier_name')),
                'category':  upper(col('category')),
                'store_location': upper(col('store_location'))
            }
        )
    else:
        return original_df.withColumns(
            {
                'supplier_name': upper(col('supplier_name')),
                'category':  upper(col('category')),
                'store_id': upper(col('store_id'))
            }
        )