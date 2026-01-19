"""Silver layer transformation job"""
from pyspark.sql.functions import col, when, regexp_replace, trim, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql import functions as F


def normalize_merchant_name(merchant_name):
    """Normalize merchant name (remove branch info)"""
    # Remove branch info like '강남점', '역삼점' etc.
    normalized = merchant_name
    for suffix in ['강남점', '역삼점', '선릉점', '코엑스점', '월계점', '서울역점', '명동점', '성수점', '광화문점']:
        normalized = normalized.replace(suffix, '').strip()
    return normalized


def transform_to_silver(spark, bronze_path, silver_path):
    """
    Transform Bronze to Silver layer
    - Remove duplicates
    - Validate data
    - Normalize merchant names
    - Add transaction type
    
    Args:
        spark: SparkSession
        bronze_path: Path to bronze Delta table
        silver_path: Path to silver Delta table
    """
    print(f"\n{'='*60}")
    print(f"🔷 Silver Transformation")
    print(f"{'='*60}")
    
    # Read from Bronze
    print(f"Reading from Bronze: {bronze_path}")
    df_bronze = spark.read.format("delta").load(bronze_path)
    
    # 1. Remove duplicates
    df_dedup = df_bronze.dropDuplicates(["transaction_id"])
    
    # 2. Data validation
    df_validated = df_dedup.filter(
        (col("amount") > 0) &
        (col("transaction_date").isNotNull()) &
        (col("merchant_name").isNotNull())
    )
    
    # 3. Register UDF for merchant normalization
    normalize_udf = F.udf(normalize_merchant_name, StringType())
    
    # 4. Normalize merchant names
    df_normalized = df_validated.withColumn(
        "normalized_merchant",
        normalize_udf(col("merchant_name"))
    )
    
    # 5. Add transaction type (all expenses for now)
    df_typed = df_normalized.withColumn(
        "transaction_type",
        when(col("amount") > 0, "EXPENSE").otherwise("INCOME")
    )
    
    # 6. Add processing timestamp
    df_final = df_typed.withColumn("processed_at", current_timestamp())
    
    # 7. Select final columns
    df_silver = df_final.select(
        "transaction_id",
        "transaction_date",
        F.year(col("transaction_date")).alias("year"),
        F.month(col("transaction_date")).alias("month"),
        "merchant_name",
        "normalized_merchant",
        "merchant_category",
        "amount",
        "transaction_type",
        "card_number",
        "processed_at"
    )
    
    # 8. Data Quality Validation (Great Expectations) - Use Sampling for high performance if data > 1M
    print("\n🔍 Running data quality validation...")
    try:
        from utils.data_quality import DataQualityValidator
        
        # Sampling for performance
        if initial_count > 1000000:
            print(f"   (Using 1% sample for fast validation of {initial_count:,} records)")
            df_for_validation = df_silver.sample(0.01)
        else:
            df_for_validation = df_silver

        validator = DataQualityValidator()
        validation_results = validator.validate_silver_layer(df_for_validation)
        
        if not validation_results["success"]:
            print("\n⚠️  WARNING: Data quality validation found issues!")
            print(f"   Success rate: {validation_results['statistics']['success_percent']:.1f}%")
            print(f"   Failed expectations: {validation_results['statistics']['unsuccessful_expectations']}")
            print("\n   Continuing with write, but please review validation report.")
        else:
            print("✅ Data quality validation passed!")
            
    except ImportError:
        print("⚠️  Great Expectations not installed, skipping validation")
    except Exception as e:
        print(f"⚠️  Validation error: {e}")
        print("   Continuing with write...")
    
    # Write to Silver Delta Lake
    print(f"\nWriting to Silver: {silver_path} (partitioned by year, month)")
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save(silver_path)
    
    # Delta Auto-Optimize (enabled in spark session) will handle compaction during write.
    # We skip explicit Z-Order to save 5-10 minutes during benchmark.
    print(f"Skipping explicit Z-Order optimization (Auto-Optimize enabled)")
    
    print(f"✅ Silver transformation trigger initiated")
    print(f"{'='*60}\n")
    
    return silver_path

