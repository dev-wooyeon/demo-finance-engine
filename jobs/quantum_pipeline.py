import duckdb
import os
import time
from pathlib import Path

def run_quantum_pipeline(source_path="data/raw/card_transactions.parquet", output_base="data/quantum"):
    """
    Supersonic Quantum Pipeline using DuckDB
    - Bypasses JVM overhead
    - Native C++ Vectorized Execution
    - Targets Sub-60s for 100M records
    """
    print("\n" + "="*60)
    print("⚛️  QUANTUM PIPELINE (DuckDB Native Engine)")
    print("="*60)
    
    # 1. Setup
    Path(output_base).mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(':memory:')
    
    # Tuning for 32GB RAM & 12 Cores
    db.execute("SET memory_limit='28GB'")
    db.execute("SET threads=12")
    db.execute("SET preserve_insertion_order=false")
    
    print(f"Reading from: {source_path}")
    
    # 2. Bronze Stage & Silver Stage (Hash-based Deduplication)
    # Using GROUP BY + any_value instead of window functions for 2x speedup
    print("Executing Hyperspace Transformation (Hash-Deduplication 100M records)...")
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
    
    # 3. Gold Stage (Dimensions)
    print("Building Dimensions...")
    db.execute("""
        CREATE TABLE dim_category AS 
        SELECT merchant_category as category_name, 
               row_number() OVER () as category_key 
        FROM (SELECT DISTINCT merchant_category FROM silver)
    """)
    
    db.execute("""
        CREATE TABLE dim_merchant AS 
        SELECT merchant_name, merchant_category, 
               row_number() OVER () as merchant_key 
        FROM (SELECT DISTINCT merchant_name, merchant_category FROM silver)
    """)
    
    # 4. Final Gold Fact Join & Write
    gold_path = f"{output_base}/fact_transactions.parquet"
    print(f"Writing Gold Fact Table (Hyperspace Mode)...")
    
    db.execute(f"""
        COPY (
            SELECT 
                s.transaction_id,
                s.year,
                s.month,
                c.category_key,
                m.merchant_key,
                s.amount
            FROM silver s
            JOIN dim_category c ON s.merchant_category = c.category_name
            JOIN dim_merchant m ON s.merchant_name = m.merchant_name 
                                     AND s.merchant_category = m.merchant_category
        ) TO '{gold_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')
    """)
    
    print(f"\n✅ QUANTUM PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_quantum_pipeline()
