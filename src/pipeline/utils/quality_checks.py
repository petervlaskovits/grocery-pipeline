import pandera.pyspark as pa
from pyspark.sql.types import DecimalType, DateType

shared_columns = {
    'sku': pa.Column(str),
    'product_name': pa.Column(str),
    'store_location': pa.Column(str),
    'supplier_name': pa.Column(str),
    'flag': pa.Column(str, pa.Check.isin(allowed_values=["TRUE", "FALSE"]))
}

inventory_schema = pa.DataFrameSchema(
    {
        'sku': pa.Column(str),
        'product_name': pa.Column(str),
        'category': pa.Column(str),
        'store_location': pa.Column(str),
        'supplier_name': pa.Column(str),
        'units_on_hand': pa.Column(int, nullable=True),
        'reorder_point': pa.Column(int),
        'days_on_shelf': pa.Column(int, nullable=True),
        'unit_cost_usd': pa.Column(DecimalType(10, 2)),
        'unit_retail_price_usd': pa.Column(DecimalType(10, 2)),
        'out_of_stock_days_30d': pa.Column(int),
        'expiry_date': pa.Column(DateType(), nullable=True),
        'markdown_flag': pa.Column(str, pa.Check.isin(allowed_values=["TRUE", "FALSE"]))
    }
)