"""Bronze layer schema definitions"""
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, 
    DecimalType, TimestampType, IntegerType
)


def get_card_transaction_schema():
    """Schema for card transactions"""
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("merchant_name", StringType(), False),
        StructField("merchant_category", StringType(), True),
        StructField("amount", DecimalType(15, 2), False),
        StructField("card_number", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("card_company", StringType(), True),
        StructField("created_at", TimestampType(), False)
    ])


def get_bank_transaction_schema():
    """Schema for bank transactions"""
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("description", StringType(), False),
        StructField("amount", DecimalType(15, 2), False),
        StructField("balance", DecimalType(15, 2), True),
        StructField("account_number", StringType(), True),
        StructField("created_at", TimestampType(), False)
    ])


def get_stock_transaction_schema():
    """Schema for stock transactions"""
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("transaction_date", DateType(), False),
        StructField("stock_code", StringType(), False),
        StructField("stock_name", StringType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("price", DecimalType(15, 2), False),
        StructField("total_amount", DecimalType(15, 2), False),
        StructField("created_at", TimestampType(), False)
    ])
