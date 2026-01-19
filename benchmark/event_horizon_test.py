import duckdb
import time
import os
from pathlib import Path

def run_event_horizon_benchmark(source_path="data/raw/card_transactions.parquet", output_path="data/quantum/fact_event_horizon.parquet"):
    print("\n" + "="*60)
    print("🌌 EVENT HORIZON PIPELINE (Physical Limit Challenge)")
    print("="*60)
    
    db = duckdb.connect(':memory:')
    db.execute("SET memory_limit='28GB'")
    db.execute("SET threads=12")
    db.execute("SET preserve_insertion_order=false")
    
    start_time = time.time()
    
    # 1. READ & DEDUPLICATE with PROJECTION PUSHDOWN
    # We only read the exact columns needed to save I/O bandwidth
    print("Reading & Hash-Deduplicating (Zero-Copy Projections)...")
    db.execute("""
        CREATE TABLE silver AS 
        SELECT 
            transaction_id,
            any_value(transaction_date) as transaction_date,
            any_value(merchant_name) as merchant_name,
            any_value(merchant_category) as merchant_category,
            any_value(amount) as amount
        FROM read_parquet(?)
        WHERE amount > 0
        GROUP BY transaction_id
    """, [source_path])
    
    print("Building Dimensions & Joining (Zero-CPU Strategy)...")
    # Using Subqueries to avoid intermediate table creation if possible
    db.execute(f"""
        COPY (
            WITH dims AS (
                SELECT merchant_category, row_number() OVER () as cat_key FROM (SELECT DISTINCT merchant_category FROM silver)
            ),
            merch AS (
                SELECT merchant_name, merchant_category, row_number() OVER () as merch_key FROM (SELECT DISTINCT merchant_name, merchant_category FROM silver)
            )
            SELECT 
                s.transaction_id,
                year(s.transaction_date) as year,
                month(s.transaction_date) as month,
                d.cat_key as category_key,
                m.merch_key as merchant_key,
                s.amount
            FROM silver s
            JOIN dims d ON s.merchant_category = d.merchant_category
            JOIN merch m ON s.merchant_name = m.merchant_name AND s.merchant_category = m.merchant_category
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'UNCOMPRESSED')
    """)
    
    end_time = time.time()
    print(f"\n✅ EVENT HORIZON COMPLETED: {end_time - start_time:.2f} seconds")
    print("="*60 + "\n")

if __name__ == "__main__":
    if os.path.exists("data/quantum/fact_event_horizon.parquet"):
        os.remove("data/quantum/fact_event_horizon.parquet")
    run_event_horizon_benchmark()
