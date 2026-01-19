"""
Performance Benchmark Tool for Finance Data Platform
Measures execution time and resource usage for each pipeline stage.
"""
import time
import psutil
import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from jobs.run_pipeline import run_full_pipeline
from data_generator.transaction_generator import TransactionGenerator
from pathlib import Path

class BenchmarkRunner:
    def __init__(self, output_file="benchmark/results.md"):
        self.results = []
        self.output_file = output_file
        self.start_time = None

    def measure_stage(self, stage_name, func, *args, **kwargs):
        print(f"\n⏱️  Starting Stage: {stage_name}...")
        start_ts = time.time()
        process = psutil.Process(os.getpid())
        start_mem = process.memory_info().rss / 1024 / 1024  # MB
        
        try:
            func(*args, **kwargs)
            status = "✅ Success"
        except Exception as e:
            print(f"❌ Failed: {e}")
            status = "❌ Failed"
            raise e
        
        end_ts = time.time()
        end_mem = process.memory_info().rss / 1024 / 1024  # MB
        duration = end_ts - start_ts
        
        metrics = {
            "stage": stage_name,
            "duration_sec": round(duration, 2),
            "memory_used_mb": round(end_mem - start_mem, 2),
            "peak_memory_mb": round(end_mem, 2),
            "status": status
        }
        self.results.append(metrics)
        print(f"   Done in {metrics['duration_sec']}s | Mem: {metrics['peak_memory_mb']}MB")
        return metrics

    def save_report(self, title, record_count, optimized=False):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode = "⚡ Optimized" if optimized else "🐢 Baseline"
        
        with open(self.output_file, "a") as f:
            f.write(f"\n## 📊 Benchmark Report: {title}\n")
            f.write(f"- **Date**: {timestamp}\n")
            f.write(f"- **Records**: {record_count:,}\n")
            f.write(f"- **Mode**: {mode}\n\n")
            
            f.write("| Stage | Duration (s) | Memory (MB) | Status |\n")
            f.write("|-------|-------------:|------------:|:------:|\n")
            
            total_duration = 0
            for r in self.results:
                f.write(f"| {r['stage']} | {r['duration_sec']} | {r['peak_memory_mb']} | {r['status']} |\n")
                total_duration += r['duration_sec']
            
            f.write(f"| **Total** | **{round(total_duration, 2)}** | - | - |\n")
            f.write("\n---\n")
        
        print(f"\n📝 Report saved to {self.output_file}")

def generate_data_wrapper(records, output_path):
    """Wrapper function for data generation"""
    generator = TransactionGenerator()
    df = generator.generate_card_transactions(num_records=records)
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    generator.save_to_parquet(df, Path(output_path))
    print(f"✅ Generated {len(df):,} records")

def run_baseline_test(records=100000):
    runner = BenchmarkRunner()
    
    print(f"🚀 Starting Baseline Benchmark with {records:,} records...")
    
    # 1. Data Generation (Single Thread)
    runner.measure_stage(
        "Data Generation (Single)", 
        generate_data_wrapper, 
        records, 
        "data/raw/card_transactions.parquet"
    )
    
    # 2. Pipeline Execution
    # run_full_pipeline creates its own spark session
    runner.measure_stage(
        "ETL Pipeline",
        run_full_pipeline
    )
    
    runner.save_report("Baseline Test", records, optimized=False)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=100000)
    args = parser.parse_args()
    
    run_baseline_test(args.records)
