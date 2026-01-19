"""
Simple Pipeline Benchmark - Only measures ETL pipeline performance
Assumes data is already generated in data/raw/
"""
import time
import psutil
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from jobs.run_pipeline import run_full_pipeline, run_supersonic_pipeline, run_quantum_pipeline


class SimpleBenchmark:
    def __init__(self):
        self.results = {}
        
    def run(self):
        print("\n" + "="*60)
        print("🚀 Starting Pipeline Benchmark")
        print("="*60 + "\n")
        
        # Get initial metrics
        process = psutil.Process(os.getpid())
        start_mem = process.memory_info().rss / 1024 / 1024  # MB
        start_time = time.time()
        
        # Run pipeline
        try:
            if "--quantum" in sys.argv:
                run_quantum_pipeline()
            elif "--supersonic" in sys.argv:
                run_supersonic_pipeline()
            else:
                run_full_pipeline()
            status = "✅ Success"
        except Exception as e:
            print(f"❌ Pipeline failed: {e}")
            status = "❌ Failed"
            raise
        
        # Calculate metrics
        end_time = time.time()
        end_mem = process.memory_info().rss / 1024 / 1024  # MB
        duration = end_time - start_time
        
        self.results = {
            "duration_sec": round(duration, 2),
            "start_memory_mb": round(start_mem, 2),
            "end_memory_mb": round(end_mem, 2),
            "memory_increase_mb": round(end_mem - start_mem, 2),
            "status": status
        }
        
        # Print results
        print("\n" + "="*60)
        print("📊 Benchmark Results")
        print("="*60)
        print(f"Status: {self.results['status']}")
        print(f"Duration: {self.results['duration_sec']} seconds")
        print(f"Start Memory: {self.results['start_memory_mb']} MB")
        print(f"End Memory: {self.results['end_memory_mb']} MB")
        print(f"Memory Increase: {self.results['memory_increase_mb']} MB")
        print("="*60 + "\n")
        
        # Save to file
        self.save_report()
        
    def save_report(self):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        output_file = "benchmark/results.md"
        
        # Create benchmark directory if it doesn't exist
        Path("benchmark").mkdir(exist_ok=True)
        
        with open(output_file, "a") as f:
            f.write(f"\n## 📊 Pipeline Benchmark - {timestamp}\n\n")
            f.write(f"- **Duration**: {self.results['duration_sec']} seconds\n")
            f.write(f"- **Memory Start**: {self.results['start_memory_mb']} MB\n")
            f.write(f"- **Memory End**: {self.results['end_memory_mb']} MB\n")
            f.write(f"- **Memory Increase**: {self.results['memory_increase_mb']} MB\n")
            f.write(f"- **Status**: {self.results['status']}\n")
            f.write("\n---\n")
        
        print(f"📝 Report saved to {output_file}\n")


if __name__ == "__main__":
    benchmark = SimpleBenchmark()
    benchmark.run()
