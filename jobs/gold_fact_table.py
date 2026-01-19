"""Gold layer fact table creation"""
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def build_fact_transactions(spark, silver_path, output_path='data/gold/fact_transactions'):
    """
    Build fact_transactions table with surrogate keys
    
    Args:
        spark: SparkSession
        silver_path: Path to silver transactions
        output_path: Output path for fact table
    """
    print(f"\n{'='*60}")
    print(f"🌟 Building fact_transactions")
    print(f"{'='*60}")
    
    # Read silver data
    print("Reading Silver data...")
    df_silver = spark.read.format("delta").load(silver_path)
    
    # Read dimension tables
    print("Reading dimension tables...")
    dim_date = spark.read.format("delta").load("data/gold/dim_date")
    dim_category = spark.read.format("delta").load("data/gold/dim_category")
    dim_merchant = spark.read.format("delta").load("data/gold/dim_merchant")
    
    # Join with dimensions to get surrogate keys (using Broadcast Joins for dimensions)
    print("Joining with dimensions...")
    
    # Join with dim_date
    fact_df = df_silver.join(
        F.broadcast(dim_date),
        df_silver.transaction_date == dim_date.full_date,
        "left"
    )
    
    # Join with dim_category
    fact_df = fact_df.join(
        F.broadcast(dim_category),
        df_silver.merchant_category == dim_category.category_name,
        "left"
    )
    
    # Join with dim_merchant
    fact_df = fact_df.join(
        F.broadcast(dim_merchant),
        (df_silver.normalized_merchant == dim_merchant.merchant_name) &
        (dim_merchant.is_current == True),
        "left"
    )
    
    # Select fact table columns including partitioning columns from dim_date
    fact_final = fact_df.select(
        F.monotonically_increasing_id().cast(IntegerType()).alias("transaction_key"),
        dim_date.date_key,
        dim_date.year,
        dim_date.month,
        dim_category.category_key,
        dim_merchant.merchant_key,
        df_silver.transaction_id,
        df_silver.amount,
        F.lit(1).alias("quantity"),
        F.current_timestamp().alias("created_at")
    )
    
    # Write to Delta Lake (partitioned by year, month)
    print(f"Writing to: {output_path} (partitioned by year, month)")
    fact_final.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save(output_path)
    
    print(f"✅ fact_transactions trigger initiated")
    print(f"{'='*60}\n")
    
    return output_path
