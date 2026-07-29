from pipeline.utils import cleaning
from pyspark.sql import SparkSession

def test_store_mappings(spark):
    df = spark.read.jdbc("jdbc:postgresql://localhost:5432/postgres", table="transactions", properties={
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
    })
    uppercased = cleaning.uppercase_columns_for_mapping(df)
    mapped = cleaning.map_lookup_to_df(original_df=uppercased, lookup_map=cleaning.store_location_map, column_name='store_id')

    distinct_col = mapped.select('store_id').distinct().collect()
    distinct_col_vals = [v[0] for v in distinct_col]
    distinct_col_vals.sort()

    all_values = ['NORTH MAIN ST', 'NORTH CORNER', 'EAST CORNER', 'SOUTH CORNER', 'NORTH PLAZA', 'EAST MAIN ST', 'WEST MAIN ST', 'WEST CORNER', 'EAST PLAZA', 'SOUTH MAIN ST', 'WEST PLAZA', 'SOUTH PLAZA']

    for value in all_values:
        assert value in distinct_col_vals

def test_flag_mappings(spark):
    df = spark.read.jdbc("jdbc:postgresql://localhost:5432/postgres", table="supplier_orders", properties={
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
    })

    uppercased = cleaning.uppercase_columns_for_mapping(df)
    mapped = cleaning.map_lookup_to_df(original_df=uppercased, lookup_map=cleaning.flags_map, column_name='invoice_matched_flag')
    distinct_col = mapped.select('invoice_matched_flag').distinct().collect()

    distinct_col_vals = [v[0] for v in distinct_col]
    
    all_values = [True, False]

    for value in all_values:
        assert value in distinct_col_vals

def test_category_mappings(spark):
    df = spark.read.jdbc("jdbc:postgresql://localhost:5432/postgres", table="inventory", properties={
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
    })

    uppercased = cleaning.uppercase_columns_for_mapping(df)
    mapped = cleaning.map_lookup_to_df(original_df=uppercased, lookup_map=cleaning.category_map, column_name='category')

    distinct_col = mapped.select('category').distinct().collect()

    distinct_col_vals = [v[0] for v in distinct_col]
    all_values = ['ELECTRONICS', 'DAIRY', 'BAKERY', 'HOUSEHOLD', 'PERSONAL CARE', 'SNACKS', 'BEVERAGES', 'MEAT', 'LOTTERY', 'PRODUCE', 'TOBACCO', 'FROZEN']

    for value in all_values:
        assert value in distinct_col_vals

