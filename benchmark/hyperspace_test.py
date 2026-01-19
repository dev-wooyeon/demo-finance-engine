import duckdb
import time
import os
from pathlib import Path

def run_hyperspace_benchmark(source_path="data/raw/card_transactions.parquet", output_path="data/quantum/fact_hyperspace.parquet"):
    print("\n" + "="*60)
    print("🚀 HYPERSPACE PIPELINE (Extreme Optimization)")
    print("="*60)
    
    db = duckdb.connect(':memory:')
    db.execute("SET memory_limit='28GB'")
    db.execute("SET threads=12")
    db.execute("SET preserve_insertion_order=false")
    
    start_time = time.time()
    
    # 1. READ & DEDUPLICATE (Using GROUP BY instead of Window for speed)
    print("Reading & Hash-Deduplicating (100M records)...")
    db.execute("""
        CREATE TABLE silver AS 
        SELECT 
            transaction_id,
            any_value(transaction_date) as transaction_date,
            any_value(merchant_name) as merchant_name,
            any_value(merchant_category) as merchant_category,
            any_value(amount) as amount,
            year(any_value(transaction_date)) as year,
            month(any_value(transaction_date)) as month
        FROM read_parquet(?)
        WHERE amount > 0
        GROUP BY transaction_id
    """, [source_path])
    
    print("Building Dimensions...")
    db.execute("CREATE TABLE dim_cat AS SELECT merchant_category, row_number() OVER () as key FROM (SELECT DISTINCT merchant_category FROM silver)")
    db.execute("CREATE TABLE dim_merch AS SELECT merchant_name, merchant_category, row_number() OVER () as key FROM (SELECT DISTINCT merchant_name, merchant_category FROM silver)")
    
    print("Final Join & High-Speed Write...")
    db.execute(f"""
        COPY (
            SELECT 
                s.transaction_id,
                s.year,
                s.month,
                c.key as category_key,
                m.key as merchant_key,
                s.amount
            FROM silver s
            JOIN dim_cat c ON s.merchant_category = c.merchant_category
            JOIN dim_merch m ON s.merchant_name = m.merchant_name AND s.merchant_category = m.merchant_category
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
    """)
    
    end_time = time.time()
    print(f"\n✅ HYPERSPACE COMPLETED: {end_time - start_time:.2f} seconds")
    print("="*60 + "\n")

if __name__ == "__main__":
    if os.path.exists("data/quantum/fact_hyperspace.parquet"):
        os.remove("data/quantum/fact_hyperspace.parquet")
    run_hyperspace_benchmark()
