"""Gold layer fact table creation"""
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from datetime import datetime


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
    dim_card = spark.read.format("delta").load("data/gold/dim_card")
    
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
    
    # Join with dim_card
    fact_df = fact_df.join(
        F.broadcast(dim_card),
        df_silver.card_number == dim_card.card_number,
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
        dim_card.card_key,
        df_silver.transaction_id,
        df_silver.amount,
        F.lit(1).alias("quantity"),
        F.lit(datetime.now()).alias("created_at")
    ).localCheckpoint()
    
    # Write to Delta Lake (Incremental Merge)
    from delta.tables import DeltaTable
    
    if DeltaTable.isDeltaTable(spark, output_path):
        print(f"Performing Incremental MERGE into: {output_path}")
        dt_fact = DeltaTable.forPath(spark, output_path)
        
        dt_fact.alias("target").merge(
            fact_final.alias("source"),
            "target.transaction_id = source.transaction_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        print(f"Creating new fact_transactions table at: {output_path}")
        fact_final.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("year", "month") \
            .save(output_path)
    
    print(f"✅ fact_transactions update completed")
    print(f"{'='*60}\n")
    
    return output_path
