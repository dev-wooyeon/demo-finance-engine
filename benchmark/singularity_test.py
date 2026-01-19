import polars as pl
import time
import os
from pathlib import Path

def run_singularity_benchmark(source_path="data/raw/card_transactions.parquet", output_path="data/quantum/fact_singularity.parquet"):
    print("\n" + "="*60)
    print("✨ SINGULARITY PIPELINE (Polars Rust Engine)")
    print("="*60)
    
    start_time = time.time()
    
    # 1. Lazy Scan (Zero-Copy Metadata)
    print("Initializing Lazy DAG (100M records)...")
    lf = pl.scan_parquet(source_path)
    
    # 2. Silver Transformation (Deduplication & Cleaning)
    # Note: unique() in Polars is extremely fast
    lf_silver = (
        lf
        .unique(subset=["transaction_id"], maintain_order=False)
        .filter(pl.col("amount") > 0)
        .with_columns([
            pl.col("transaction_date").dt.year().alias("year").cast(pl.Int32),
            pl.col("transaction_date").dt.month().alias("month").cast(pl.Int32)
        ])
    )
    
    # Since we need to join for dims, it's better to materialize silver *if* it fits in RAM
    # 100M rows with ~8-10 columns fits in ~16-24GB
    print("Materializing Silver layer (Extreme Parallel Deduplication)...")
    df_silver = lf_silver.collect()
    
    # 3. Gold Stage (Dimensions)
    print("Building Dimensions...")
    dim_cat = (
        df_silver.select("merchant_category").unique()
        .with_columns(pl.Series("category_key", range(1, 1 + df_silver.select("merchant_category").unique().height)))
    )
    
    dim_merch = (
        df_silver.select(["merchant_name", "merchant_category"]).unique()
        .with_columns(pl.Series("merchant_key", range(1, 1 + df_silver.select(["merchant_name", "merchant_category"]).unique().height)))
    )
    
    # 4. Final Join & Write
    print("Executing Final Join & Writing Parquet...")
    fact_final = (
        df_silver
        .join(dim_cat, on="merchant_category")
        .join(dim_merch, on=["merchant_name", "merchant_category"])
        .select([
            "transaction_id",
            "year",
            "month",
            "category_key",
            "merchant_key",
            "amount"
        ])
    )
    
    # Use Snappy for fast compression or even uncompressed for raw speed
    fact_final.write_parquet(output_path, compression="snappy")
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n✅ SINGULARITY PIPELINE COMPLETED: {duration:.2f} seconds")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_singularity_benchmark()
