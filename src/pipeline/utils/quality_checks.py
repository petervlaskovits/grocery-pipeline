import pandera.pyspark as pa

from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType, DateType, TimestampType

from prefect.logging import get_run_logger

from utils.cleaning import store_location_map, category_map

import json

shared_columns = {
    'sku': pa.Column(str, pa.Check.str_matches(pattern=r"SKU-[0-9]{4}")),
    'product_name': pa.Column(str),
    'category': pa.Column(str, pa.Check.isin(allowed_values=[pair[1] for pair in category_map])),
    'store_location': pa.Column(str, pa.Check.isin(allowed_values=[pair[1] for pair in store_location_map])),
    'supplier_name': pa.Column(str, pa.Check.str_matches(r"[A-Z\s]{12,22}")),
    'flag': pa.Column(str, pa.Check.isin(allowed_values=["TRUE", "FALSE"])),
    'unit_cost_usd': pa.Column(DecimalType(10, 2), checks=pa.Check.greater_than_or_equal_to(0)),
}

inventory_schema = pa.DataFrameSchema(
    {
        'sku': shared_columns['sku'],
        'product_name': shared_columns['product_name'],
        'category': shared_columns['category'],
        'store_location': shared_columns['store_location'],
        'supplier_name': shared_columns['supplier_name'],
        'units_on_hand': pa.Column(int, nullable=True, checks=pa.Check.greater_than_or_equal_to(0)),
        'reorder_point': pa.Column(int, checks=pa.Check.greater_than_or_equal_to(0)),
        'days_on_shelf': pa.Column(int, nullable=True, checks=pa.Check.greater_than_or_equal_to(0)),
        'last_restock_date': pa.Column(TimestampType(), nullable=True),
        'unit_cost_usd': shared_columns['unit_cost_usd'],
        'unit_retail_price_usd': pa.Column(DecimalType(10, 2), checks=pa.Check.greater_than_or_equal_to(0)),
        'out_of_stock_days_30d': pa.Column(int, checks=pa.Check.greater_than_or_equal_to(0)),
        'expiry_date': pa.Column(DateType(), nullable=True),
        'markdown_flag': shared_columns['flag']
    }
)

suppliers_schema = pa.DataFrameSchema({
    'order_id': pa.Column(str, pa.Check.str_matches(r"PO-[0-9]{5}")),
    'order_date': pa.Column(TimestampType(), nullable=True),
    'supplier_name': shared_columns['supplier_name'],
    'category': shared_columns['category'],
    'sku': shared_columns['sku'],
    'product_name': shared_columns['product_name'],
    'store_location': shared_columns['store_location'],
    'qty_ordered': pa.Column(int, pa.Check.greater_than(0)),
    'qty_received': pa.Column(int, pa.Check.greater_than_or_equal_to(0), nullable=True),
    'unit_cost_usd': shared_columns['unit_cost_usd'],
    'total_order_value_usd': shared_columns['unit_cost_usd'],
    'expected_delivery_date': pa.Column(DateType()),
    'actual_delivery_date': pa.Column(TimestampType(), nullable=True),
    'fill_rate_pct': pa.Column(DecimalType(5, 2), pa.Check.in_range(0, 100, True, True)),
    'invoice_matched_flag': shared_columns['flag']
})

transactions_schema = pa.DataFrameSchema({
    'transaction_id': pa.Column(str, pa.Check.str_matches(r"TXN-[0-9]{6}")),
    'customer_id': pa.Column(str, pa.Check.str_matches(r"(CUST-[0-9]{4})|(N\/A)")),
    'transaction_date': pa.Column(TimestampType(), nullable=True),
    'store_id': shared_columns['store_location'],
    'category': shared_columns['category'],
    'product_sku': shared_columns['sku'],
    'product_name': pa.Column(str),
    'quantity_sold': pa.Column(int),
    'unit_price_usd': pa.Column(DecimalType(12, 2)),
    'discount_rate': pa.Column(DecimalType(5, 4)),
    'total_sales_usd': pa.Column(DecimalType(12, 2)),
    'payment_method': pa.Column(str, pa.Check.isin(["CASH", "CREDIT CARD", "DEBIT CARD", "GIFT CARD", "MOBILE PAY"])),
    'cashier_id': pa.Column(str, pa.Check.str_matches(r"EMP-[0-9]{3}")),
    'shrinkage_usd': pa.Column(DecimalType(10, 2)),
    'return_flag': shared_columns['flag']
})

def apply_schema(schema: pa.DataFrameSchema, df: DataFrame) -> [DataFrame, str]:
    validated = schema.validate(df)
    validation_errors = json.dumps(dict(validated.pandera.errors), indent=4)

    return [validated, validation_errors]