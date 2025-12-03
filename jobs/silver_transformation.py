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
    
    initial_count = df_bronze.count()
    print(f"Bronze records: {initial_count:,}")
    
    # 1. Remove duplicates
    df_dedup = df_bronze.dropDuplicates(["transaction_id"])
    dedup_count = df_dedup.count()
    print(f"After deduplication: {dedup_count:,} (removed {initial_count - dedup_count:,})")
    
    # 2. Data validation
    df_validated = df_dedup.filter(
        (col("amount") > 0) &
        (col("transaction_date").isNotNull()) &
        (col("merchant_name").isNotNull())
    )
    validated_count = df_validated.count()
    print(f"After validation: {validated_count:,} (removed {dedup_count - validated_count:,})")
    
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
        "merchant_name",
        "normalized_merchant",
        "merchant_category",
        "amount",
        "transaction_type",
        "card_number",
        "processed_at"
    )
    
    # Write to Silver Delta Lake
    print(f"Writing to Silver: {silver_path}")
    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_path)
    
    final_count = df_silver.count()
    print(f"✅ Silver transformation complete: {final_count:,} records")
    print(f"{'='*60}\n")
    
    return silver_path
