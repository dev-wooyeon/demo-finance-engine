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
    Build category dimension table from Silver data (SCD Type 1 - Idempotent)
    
    Args:
        spark: SparkSession
        silver_path: Path to silver transactions
        output_path: Output path for dimension table
    """
    from delta.tables import DeltaTable
    
    print(f"\n{'='*60}")
    print(f"🌟 Building dim_category (Idempotent)")
    print(f"{'='*60}")
    
    # Read silver data
    df_silver = spark.read.format("delta").load(silver_path)
    
    # Extract unique categories
    df_source = df_silver.select("merchant_category").distinct() \
        .withColumn("level_1", F.split(F.col("merchant_category"), "-").getItem(0)) \
        .withColumn("level_2", F.split(F.col("merchant_category"), "-").getItem(1)) \
        .withColumn("category_name", F.col("merchant_category")) \
        .withColumn("category_key", F.hash(F.col("merchant_category")).cast(IntegerType())) \
        .localCheckpoint() # Fix non-deterministic source for MERGE
    
    if DeltaTable.isDeltaTable(spark, output_path):
        print("Target dim_category exists. Performing MERGE...")
        dt_target = DeltaTable.forPath(spark, output_path)
        
        dt_target.alias("target").merge(
            df_source.alias("source"),
            "target.category_name = source.category_name"
        ).whenNotMatchedInsert(
            values={
                "category_key": "source.category_key",
                "category_name": "source.category_name",
                "level_1": "source.level_1",
                "level_2": "source.level_2"
            }
        ).execute()
    else:
        print("Creating new dim_category table...")
        df_dim = df_source.withColumn(
            "category_key", F.monotonically_increasing_id().cast(IntegerType())
        ).select(
            "category_key",
            "category_name",
            "level_1",
            "level_2"
        )
        df_dim.write.format("delta").mode("overwrite").save(output_path)
    
    print(f"✅ dim_category updated: {output_path}")
    print(f"{'='*60}\n")


def build_dim_merchant(spark, silver_path, output_path='data/gold/dim_merchant'):
    """
    Build merchant dimension table with SCD Type 2
    
    Args:
        spark: SparkSession
        silver_path: Path to silver transactions
        output_path: Output path for dimension table
    """
    from delta.tables import DeltaTable
    
    print(f"\n{'='*60}")
    print(f"🌟 Building dim_merchant (SCD Type 2)")
    print(f"{'='*60}")
    
    # Read silver data to get unique merchants
    df_silver = spark.read.format("delta").load(silver_path)
    
    # Extract unique merchants from current silver data
    df_source = df_silver.select(
        F.col("normalized_merchant").alias("merchant_name"),
        "merchant_category"
    ).distinct()
    
    # Get current date as literal for deterministic MERGE
    today = datetime.now().date()
    
    # Add SCD Type 2 metadata to source
    df_source = df_source.withColumn("is_current", F.lit(True)) \
                         .withColumn("effective_date", F.lit(today)) \
                         .withColumn("expiration_date", F.to_date(F.lit("9999-12-31")))
    
    # Check if target table exists
    if DeltaTable.isDeltaTable(spark, output_path):
        print("Target dim_merchant exists. Checking schema...")
        dt_target = DeltaTable.forPath(spark, output_path)
        target_cols = dt_target.toDF().columns
        
        # Ensure SCD columns exist in target
        if "effective_date" not in target_cols or "expiration_date" not in target_cols:
            print("Adding missing SCD columns to target table...")
            # We can't easily add columns to Delta table via Spark API directly without a write or ALTER TABLE
            # The simplest way for this demo is to recreate with current data if columns missing
            spark.read.format("delta").load(output_path) \
                .withColumn("effective_date", F.to_date(F.lit("2000-01-01"))) \
                .withColumn("expiration_date", F.to_date(F.lit("9999-12-31"))) \
                .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
            dt_target = DeltaTable.forPath(spark, output_path)
        
        print("Performing SCD Type 2 Merge...")
        
        # 1. Identity records that need updating (category changed)
        # Join source with current target records
        df_updates = df_source.alias("s").join(
            dt_target.toDF().alias("t"),
            (F.col("s.merchant_name") == F.col("t.merchant_name")) & (F.col("t.is_current") == True),
            "inner"
        ).filter(F.col("s.merchant_category") != F.col("t.merchant_category"))
        
        # 2. Expire old records
        # We handle this by merging: matches where category is different -> update old to expired
        # AND inserts for new records.
        # However, Delta MERGE SCD Type 2 typically requires a two-step process or a specific pattern.
        
        # Standard SCD Type 2 Pattern with Merge
        # Step A: Records to expire and New records
        # We need a unique key for the new record. 
        # Add merchant_key to source DF
        df_source_with_key = df_source.withColumn("merchant_key", F.hash(F.col("merchant_name"), F.col("merchant_category")).cast(IntegerType()))
        
        # Align columns for union
        # Part 1: Records that will cause expiration of existing records (category changed)
        # We use mergeKey=NULL to signal we want to UPDATE the matched target record
        df_to_expire = df_updates.selectExpr("NULL as mergeKey", "NULL as merchant_key", "s.*")
        
        # Part 2: New records (either entirely new or the "new" version of an updated record)
        # We use mergeKey=merchant_name to signal we want to INSERT if not matched by mergeKey=NULL
        df_to_insert = df_source_with_key.selectExpr("merchant_name as mergeKey", "merchant_key", "merchant_name", "merchant_category", "is_current", "effective_date", "expiration_date")
        
        staged_updates = df_to_expire.union(df_to_insert).localCheckpoint()
            
        dt_target.alias("target").merge(
            staged_updates.alias("staged"),
            "target.merchant_name = staged.mergeKey"
        ).whenMatchedUpdate(
            condition="target.is_current = true AND target.merchant_category <> staged.merchant_category",
            set={
                "is_current": "false",
                "expiration_date": "staged.effective_date"
            }
        ).whenNotMatchedInsert(
            values={
                "merchant_key": "staged.merchant_key",
                "merchant_name": "staged.merchant_name",
                "merchant_category": "staged.merchant_category",
                "is_current": "true",
                "effective_date": "staged.effective_date",
                "expiration_date": "staged.expiration_date"
            }
        ).execute()
        
    else:
        print("Creating new dim_merchant table...")
        df_dim = df_source.withColumn(
            "merchant_key", F.monotonically_increasing_id().cast(IntegerType())
        ).select(
            "merchant_key",
            "merchant_name",
            "merchant_category",
            "effective_date",
            "expiration_date",
            "is_current"
        )
        df_dim.write.format("delta").mode("overwrite").save(output_path)
    
    print(f"✅ dim_merchant updated: {output_path}")
    print(f"{'='*60}\n")


def build_dim_card(spark, silver_path, output_path='data/gold/dim_card'):
    """
    Build card dimension table (SCD Type 1 - Idempotent)
    
    Args:
        spark: SparkSession
        silver_path: Path to silver transactions
        output_path: Output path for dimension table
    """
    from delta.tables import DeltaTable
    
    print(f"\n{'='*60}")
    print(f"🌟 Building dim_card (Idempotent)")
    print(f"{'='*60}")
    
    # Read silver data
    df_silver = spark.read.format("delta").load(silver_path)
    
    # Extract unique cards
    df_source = df_silver.select(
        "card_number",
        "card_type",
        "card_company"
    ).distinct() \
     .withColumn("card_key", F.hash(F.col("card_number")).cast(IntegerType())) \
     .localCheckpoint() # Fix non-deterministic source for MERGE
    
    if DeltaTable.isDeltaTable(spark, output_path):
        print("Target dim_card exists. Performing MERGE...")
        dt_target = DeltaTable.forPath(spark, output_path)
        
        dt_target.alias("target").merge(
            df_source.alias("source"),
            "target.card_number = source.card_number"
        ).whenNotMatchedInsert(
            values={
                "card_key": "source.card_key",
                "card_number": "source.card_number",
                "card_type": "source.card_type",
                "card_company": "source.card_company"
            }
        ).execute()
    else:
        print("Creating new dim_card table...")
        df_dim = df_source.withColumn(
            "card_key", F.monotonically_increasing_id().cast(IntegerType())
        ).select(
            "card_key",
            "card_number",
            "card_type",
            "card_company"
        )
        df_dim.write.format("delta").mode("overwrite").save(output_path)
    
    print(f"✅ dim_card updated: {output_path}")
    print(f"{'='*60}\n")


def build_all_dimensions(spark, silver_path):
    """Build all dimension tables"""
    print("\n" + "="*60)
    print("Building All Dimension Tables")
    print("="*60 + "\n")
    
    build_dim_date(spark)
    build_dim_category(spark, silver_path)
    build_dim_merchant(spark, silver_path)
    build_dim_card(spark, silver_path)
    
    print("\n" + "="*60)
    print("✅ All dimensions created successfully")
    print("="*60 + "\n")
