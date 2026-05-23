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
    ("NRTH CORNER", "NORTH CORNER"),
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
    ('no', "FALSE"),
    ("Y", "TRUE"),
    ("y", "TRUE"),
    ("1", "TRUE"),
    ("YES", "TRUE"),
    ("Yes", "TRUE"),
    ('yes', "TRUE"),
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

def create_lookup_df(lookup_map: list[tuple[str, str]]) -> DataFrame:
    """Creates a lookup DataFrame for Spark to apply mappings to messy values in a DataFrame's column containing the messy values.

    Args:
        lookup_map (list[tuple[str, str]]): The lookup map, a list of tuples containing messy data with a corresponding clean value (e.g. ("TOBACO", "TOBACCO"))

    Returns:
        DataFrame: The lookup DataFrame with dirty values and clean values for a column.
    """
    spark = SparkSession.getActiveSession()
    lookup_df = spark.createDataFrame(lookup_map, ["dirty", "clean"])
    return lookup_df

def map_lookup_to_df(original_df: DataFrame, lookup_map: list[tuple[str, str]], column_name: str) -> DataFrame:
    """Maps dirty values from the lookup DataFrame to a DataFrame by left-joining the original DataFrame to the lookup DataFrame, then replaces the column to be mapped to with the mapped values using coalesce.

    Args:
        original_df (DataFrame): The DataFrame to be transformed using the lookup DataFrame.
        lookup_map (list[tuple[str, str]]): The lookup map, a list of tuples containing messy data with a corresponding clean value (e.g. ("TOBACO", "TOBACCO"))
        column_name (str): The column to be mapped to and transformed inside the DataFrame.

    Returns:
        DataFrame: The cleaned DataFrame with all of the clean values mapped to the messy values.
    """
    lookup_df = create_lookup_df(lookup_map)
    cleaned_df = original_df.join(
        lookup_df, original_df[column_name] == lookup_df["dirty"],
        how='left'
    ).withColumn(
        column_name, 
        coalesce(col("clean"), col(column_name))
    ).drop("dirty", "clean")
    return cleaned_df

def uppercase_columns_for_mapping(original_df: DataFrame) -> DataFrame:
    """Converts several columns (supplier_name, category, store_location) to uppercase, making it easier to map clean values to messy values by making the selected columns consistently in uppercase. 

    Args:
        original_df (DataFrame): The DataFrame to be transformed

    Returns:
        DataFrame: The transformed DataFrame with all three columns converted to uppercase.
    """
    
    # One table (transactions) doesn't have store_location as a column (instead store_id), so we need to check if that column exists so that we can uppercase the store_location/store_id column  
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
                'category':  upper(col('category')),
                'store_id': upper(col('store_id'))
            }
        )