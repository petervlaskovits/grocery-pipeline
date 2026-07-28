from pipeline.tasks.transform import inventory, suppliers, transactions

def test_inventory_transformations(spark):
    df = spark.read.jdbc("jdbc:postgresql://localhost:5432/postgres", table="inventory", properties={
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
    })

    cleaned_df, validation_errors = inventory.clean_and_validate_inventory(raw_df=df)

    assert validation_errors == "{}"

def test_suppliers_transformations(spark):
    df = spark.read.jdbc("jdbc:postgresql://localhost:5432/postgres", table="supplier_orders", properties={
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
    })

    cleaned_df, validation_errors = suppliers.clean_and_validate_suppliers(raw_df=df)

    assert validation_errors == "{}"

def test_transactions_transformations(spark):
    df = spark.read.jdbc("jdbc:postgresql://localhost:5432/postgres", table="transactions", properties={
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
    })

    cleaned_df, validation_errors = transactions.clean_and_validate_transactions(raw_df=df)
    assert validation_errors == "{}"