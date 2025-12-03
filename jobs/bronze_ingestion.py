"""Bronze layer ingestion job"""
from pyspark.sql.functions import current_timestamp, input_file_name
from pathlib import Path


def ingest_to_bronze(spark, source_path, table_name, bronze_base_path="data/bronze"):
    """
    Ingest Parquet files to Bronze layer (Delta Lake)
    
    Args:
        spark: SparkSession
        source_path: Path to source Parquet file
        table_name: Name of the bronze table
        bronze_base_path: Base path for bronze layer
    """
    print(f"\n{'='*60}")
    print(f"🔶 Bronze Ingestion: {table_name}")
    print(f"{'='*60}")
    
    # Read source data
    print(f"Reading from: {source_path}")
    df = spark.read.parquet(source_path)
    
    print(f"Records read: {df.count():,}")
    
    # Add metadata columns
    df_with_meta = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name())
    
    # Create bronze path
    bronze_path = f"{bronze_base_path}/{table_name}"
    Path(bronze_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Write to Delta Lake (append mode)
    print(f"Writing to: {bronze_path}")
    df_with_meta.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(bronze_path)
    
    print(f"✅ Bronze ingestion complete: {table_name}")
    print(f"{'='*60}\n")
    
    return bronze_path
