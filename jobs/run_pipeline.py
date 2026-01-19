"""Main pipeline orchestration"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.spark_session import create_spark_session, stop_spark_session
from jobs.bronze_ingestion import ingest_to_bronze
from jobs.silver_transformation import transform_to_silver
from jobs.gold_dimensions import (
    build_dim_date, 
    build_dim_category, 
    build_dim_merchant,
    build_all_dimensions
)
from jobs.gold_fact_table import build_fact_transactions
from jobs.quantum_pipeline import run_quantum_pipeline
from pyspark.sql import functions as F


def run_full_pipeline():
    """Execute complete ETL pipeline: Bronze → Silver → Gold"""
    
    print("\n" + "="*60)
    print("🚀 Personal Finance Data Platform - ETL Pipeline")
    print("="*60 + "\n")
    
    spark = None
    
    try:
        # Create Spark session
        print("Initializing Spark session...")
        spark = create_spark_session()
        print("✅ Spark session created\n")
        
        # Step 1: Bronze Layer Ingestion
        bronze_path = ingest_to_bronze(
            spark,
            source_path="data/raw/card_transactions.parquet",
            table_name="card_transactions"
        )
        
        import concurrent.futures

        # Step 2: Parallel execution (Silver and Date Dimension)
        print(f"\n🚀 Launching Parallel Stages (Silver Transformation & Date Dimension)...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_silver = executor.submit(
                transform_to_silver, 
                spark, 
                bronze_path=bronze_path, 
                silver_path="data/silver/transactions"
            )
            future_dim_date = executor.submit(build_dim_date, spark)
            
            # Wait for both to finish
            silver_path = future_silver.result()
            future_dim_date.result()

        # Step 3: Dependent Dimensions (Category, Merchant)
        print(f"\n🚀 Building dependent dimensions...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_cat = executor.submit(build_dim_category, spark, silver_path)
            future_merch = executor.submit(build_dim_merchant, spark, silver_path)
            
            future_cat.result()
            future_merch.result()
        
        # Step 4: Gold Layer - Build Fact Table
        build_fact_transactions(spark, silver_path)
        
        # Success summary
        print("\n" + "="*60)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("\nData Layers:")
        print(f"  🔶 Bronze: {bronze_path}")
        print(f"  🔷 Silver: {silver_path}")
        print(f"  🌟 Gold:")
        print(f"     - data/gold/dim_date")
        print(f"     - data/gold/dim_category")
        print(f"     - data/gold/dim_merchant")
        print(f"     - data/gold/fact_transactions")
        print("\n" + "="*60 + "\n")
        
    except Exception as e:
        print("\n" + "="*60)
        print("❌ PIPELINE FAILED")
        print("="*60)
        print(f"Error: {str(e)}")
        print("="*60 + "\n")
        raise
        
    finally:
        if spark:
            print("Stopping Spark session...")
            stop_spark_session(spark)
            print("✅ Spark session stopped\n")


def run_supersonic_pipeline():
    """Execute optimized 'One-Pass' ETL: Raw → In-Memory → Gold"""
    print("\n" + "="*60)
    print("🚀 SUPERSONIC One-Pass Pipeline (Under 60s Challenge)")
    print("="*60 + "\n")
    
    spark = create_spark_session("SupersonicFinance")
    try:
        # Step 1: Raw -> Bronze Logic (In-Memory)
        df_raw = spark.read.parquet("data/raw/card_transactions.parquet")
        df_bronze = df_raw.withColumn("ingestion_timestamp", F.current_timestamp()) \
                          .withColumn("year", F.year(F.col("transaction_date")).cast("int")) \
                          .withColumn("month", F.month(F.col("transaction_date")).cast("int"))
        
        # Step 2: Bronze -> Silver Logic (In-Memory)
        df_silver = df_bronze.dropDuplicates(["transaction_id"]) \
                             .filter((F.col("amount") > 0) & (F.col("transaction_date").isNotNull()))
        
        # Step 3: Silver -> Gold Fact Logic (In-Memory)
        # Pre-build dimensions (fast)
        from jobs.gold_dimensions import build_dim_date, build_dim_category, build_dim_merchant
        build_dim_date(spark) # Still write basic dims as they are small
        
        # Join-based dims from silver
        dim_category = df_silver.select("merchant_category").distinct() \
                                .withColumn("category_key", F.monotonically_increasing_id().cast("int"))
        dim_merchant = df_silver.select("merchant_name", "merchant_category").distinct() \
                                .withColumn("merchant_key", F.monotonically_increasing_id().cast("int"))
        
        # Step 4: Final Gold Join and Write (The Core Action)
        print("Finalizing Gold Fact table (Single DAG Trigger)...")
        dim_date = spark.read.format("delta").load("data/gold/dim_date")
        
        fact_df = df_silver.join(F.broadcast(dim_date), df_silver.transaction_date == dim_date.full_date, "left") \
                           .join(F.broadcast(dim_category), "merchant_category", "left") \
                           .join(F.broadcast(dim_merchant), ["merchant_name", "merchant_category"], "left")
        
        fact_final = fact_df.select(
            F.monotonically_increasing_id().cast("int").alias("transaction_key"),
            "date_key",
            dim_date.year,
            dim_date.month,
            "category_key",
            "merchant_key",
            "transaction_id",
            "amount",
            F.lit(1).alias("quantity"),
            F.current_timestamp().alias("created_at")
        )
        
        fact_final.write.format("delta").mode("overwrite").partitionBy("year", "month").save("data/gold/fact_transactions")
        
        print("\n✅ SUPERSONIC PIPELINE COMPLETED!")
        
    except Exception as e:
        print(f"❌ Supersonic failed: {e}")
        raise
    finally:
        stop_spark_session(spark)

if __name__ == "__main__":
    # Check for flags: --quantum, --supersonic, or default
    if "--quantum" in sys.argv:
        run_quantum_pipeline()
    elif "--supersonic" in sys.argv:
        run_supersonic_pipeline()
    else:
        run_full_pipeline()
