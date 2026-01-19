import duckdb
import time
import os

def check_hardware_floor():
    db = duckdb.connect(':memory:')
    db.execute("SET memory_limit='24GB'")
    db.execute("SET threads=12")
    
    source = "data/raw/card_transactions.parquet"
    target = "data/quantum/floor_test.parquet"
    
    if os.path.exists(target):
        os.remove(target)
        
    print("🚀 Starting Identity Limit Test (Read 100M -> Write 100M)...")
    start = time.time()
    
    db.execute(f"COPY (SELECT * FROM read_parquet('{source}')) TO '{target}' (FORMAT PARQUET)")
    
    end = time.time()
    print(f"🏁 Identity Limit: {end - start:.2f} seconds")

if __name__ == "__main__":
    check_hardware_floor()
