"""Spark Session utility with Delta Lake support"""
from pyspark.sql import SparkSession


def create_spark_session(app_name="FinanceDataPlatform"):
    """
    Create Spark Session with Delta Lake configuration
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    # Import delta to configure Spark with Delta Lake
    from delta import configure_spark_with_delta_pip
    
    # Create builder
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        # Sub-60s "Supersonic" Challenge Tuning
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "16g")
        .config("spark.memory.fraction", "0.8")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "4g")
        .config("spark.sql.columnVector.offheap.enabled", "true")
        .config("spark.sql.shuffle.partitions", "64") # Reverted to 64 (sweet spot)
        .config("spark.sql.autoBroadcastJoinThreshold", "200mb")
        # Delta Tuning for raw speed
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "false") # Skip compaction to save time
        .master("local[*]")
    )
    
    # Configure Delta Lake before creating session
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark_session(spark):
    """Stop Spark session"""
    if spark:
        spark.stop()
