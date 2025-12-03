"""Gold layer dimension table creation"""
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType, StringType, BooleanType


def build_dim_date(spark, start_date='2023-01-01', end_date='2024-12-31', output_path='data/gold/dim_date'):
    """
    Build date dimension table
    
    Args:
        spark: SparkSession
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_path: Output path for dimension table
    """
    print(f"\n{'='*60}")
    print(f"🌟 Building dim_date")
    print(f"{'='*60}")
    
    # Generate date range
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    dates = []
    current = start
    while current <= end:
        dates.append({
            'date_key': int(current.strftime('%Y%m%d')),
            'full_date': current.date(),
            'year': current.year,
            'quarter': (current.month - 1) // 3 + 1,
            'month': current.month,
            'month_name': current.strftime('%B'),
            'day_of_month': current.day,
            'day_of_week': current.weekday(),
            'day_name': current.strftime('%A'),
            'is_weekend': current.weekday() >= 5,
            'is_holiday': False  # Simplified, can add holiday logic
        })
        current += timedelta(days=1)
    
    df = spark.createDataFrame(dates)
    
    print(f"Date range: {start_date} ~ {end_date}")
    print(f"Total dates: {df.count():,}")
    
    df.write.format("delta").mode("overwrite").save(output_path)
    print(f"✅ dim_date created: {output_path}")
    print(f"{'='*60}\n")


def build_dim_category(spark, silver_path, output_path='data/gold/dim_category'):
    """
    Build category dimension table from Silver data
    
    Args:
        spark: SparkSession
        silver_path: Path to silver transactions
        output_path: Output path for dimension table
    """
    print(f"\n{'='*60}")
    print(f"🌟 Building dim_category")
    print(f"{'='*60}")
    
    # Read silver data
    df_silver = spark.read.format("delta").load(silver_path)
    
    # Extract unique categories
    df_categories = df_silver.select("merchant_category").distinct()
    
    # Split category into levels
    df_dim = df_categories.withColumn(
        "level_1", F.split(F.col("merchant_category"), "-").getItem(0)
    ).withColumn(
        "level_2", F.split(F.col("merchant_category"), "-").getItem(1)
    ).withColumn(
        "category_key", F.monotonically_increasing_id().cast(IntegerType())
    ).select(
        "category_key",
        F.col("merchant_category").alias("category_name"),
        "level_1",
        "level_2"
    )
    
    print(f"Unique categories: {df_dim.count():,}")
    
    df_dim.write.format("delta").mode("overwrite").save(output_path)
    print(f"✅ dim_category created: {output_path}")
    print(f"{'='*60}\n")


def build_dim_merchant(spark, silver_path, output_path='data/gold/dim_merchant'):
    """
    Build merchant dimension table
    
    Args:
        spark: SparkSession
        silver_path: Path to silver transactions
        output_path: Output path for dimension table
    """
    print(f"\n{'='*60}")
    print(f"🌟 Building dim_merchant")
    print(f"{'='*60}")
    
    # Read silver data
    df_silver = spark.read.format("delta").load(silver_path)
    
    # Extract unique merchants
    df_merchants = df_silver.select(
        "normalized_merchant",
        "merchant_category"
    ).distinct()
    
    df_dim = df_merchants.withColumn(
        "merchant_key", F.monotonically_increasing_id().cast(IntegerType())
    ).withColumn(
        "is_current", F.lit(True)
    ).select(
        "merchant_key",
        F.col("normalized_merchant").alias("merchant_name"),
        "merchant_category",
        "is_current"
    )
    
    print(f"Unique merchants: {df_dim.count():,}")
    
    df_dim.write.format("delta").mode("overwrite").save(output_path)
    print(f"✅ dim_merchant created: {output_path}")
    print(f"{'='*60}\n")


def build_all_dimensions(spark, silver_path):
    """Build all dimension tables"""
    print("\n" + "="*60)
    print("Building All Dimension Tables")
    print("="*60 + "\n")
    
    build_dim_date(spark)
    build_dim_category(spark, silver_path)
    build_dim_merchant(spark, silver_path)
    
    print("\n" + "="*60)
    print("✅ All dimensions created successfully")
    print("="*60 + "\n")
