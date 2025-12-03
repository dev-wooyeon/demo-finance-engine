"""Main pipeline orchestration"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.spark_session import create_spark_session, stop_spark_session
from jobs.bronze_ingestion import ingest_to_bronze
from jobs.silver_transformation import transform_to_silver
from jobs.gold_dimensions import build_all_dimensions
from jobs.gold_fact_table import build_fact_transactions


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
        
        # Step 2: Silver Layer Transformation
        silver_path = transform_to_silver(
            spark,
            bronze_path=bronze_path,
            silver_path="data/silver/transactions"
        )
        
        # Step 3: Gold Layer - Build Dimensions
        build_all_dimensions(spark, silver_path)
        
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


if __name__ == "__main__":
    run_full_pipeline()
