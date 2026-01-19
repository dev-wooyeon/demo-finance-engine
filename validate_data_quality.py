#!/usr/bin/env python3
"""
Standalone Data Quality Validation Script

Run this script to validate all layers of the pipeline:
    uv run python validate_data_quality.py

This will:
1. Load data from Bronze, Silver, and Gold layers
2. Run Great Expectations validations
3. Generate detailed reports
4. Exit with status code 0 (success) or 1 (failure)
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from utils.spark_session import create_spark_session, stop_spark_session
from utils.data_quality import validate_pipeline_data


def main():
    """Run data quality validation on all layers"""
    
    print("\n" + "="*70)
    print("🔍 DATA QUALITY VALIDATION - Personal Finance Data Platform")
    print("="*70)
    
    spark = None
    
    try:
        # Create Spark session
        print("\nInitializing Spark session...")
        spark = create_spark_session("DataQualityValidation")
        print("✅ Spark session created\n")
        
        # Define paths
        bronze_path = "data/bronze/card_transactions"
        silver_path = "data/silver/transactions"
        gold_path = "data/gold"
        
        # Validate all layers
        all_passed = validate_pipeline_data(
            spark=spark,
            bronze_path=bronze_path,
            silver_path=silver_path,
            gold_path=gold_path
        )
        
        # Exit with appropriate status code
        if all_passed:
            print("\n✅ Data quality validation completed successfully!")
            print("   All expectations met across all layers.\n")
            sys.exit(0)
        else:
            print("\n⚠️  Data quality validation completed with failures!")
            print("   Check the reports in 'data_quality_reports/' for details.\n")
            sys.exit(1)
            
    except FileNotFoundError as e:
        print(f"\n❌ Error: Data files not found - {e}")
        print("   Please run the pipeline first: uv run python jobs/run_pipeline.py\n")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Validation failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        if spark:
            print("Stopping Spark session...")
            stop_spark_session(spark)
            print("✅ Spark session stopped\n")


if __name__ == "__main__":
    main()
