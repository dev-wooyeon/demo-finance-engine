"""Data quality validation using Great Expectations

This module provides data quality validation for all layers of the Medallion Architecture.
Uses a simplified approach with Pandas DataFrames for compatibility with Great Expectations 1.x

Usage:
    from utils.data_quality import DataQualityValidator
    
    validator = DataQualityValidator()
    results = validator.validate_silver_layer(df)
    
    if not results['success']:
        print(f"Validation failed: {results}")
"""

import great_expectations as gx
from typing import Dict, Any
import json
from pathlib import Path
from datetime import datetime


class DataQualityValidator:
    """Validate data quality for each layer of the Medallion Architecture"""
    
    def __init__(self, output_dir: str = "data_quality_reports"):
        """
        Initialize validator
        
        Args:
            output_dir: Directory to save validation reports
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def validate_bronze_layer(self, df) -> Dict[str, Any]:
        """
        Validate Bronze layer data
        
        Args:
            df: Spark DataFrame to validate
            
        Returns:
            Dict with validation results
        """
        print("\n🔍 Validating Bronze Layer...")
        
        results = {
            "layer": "bronze",
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "checks": [],
            "failed_checks": []
        }
        
        # Convert to Pandas for validation (sample for large datasets)
        row_count = df.count()
        sample_size = min(10000, row_count)
        pdf = df.limit(sample_size).toPandas()
        
        # Check 1: Required columns exist
        required_cols = ["transaction_id", "transaction_date", "merchant_name", "amount", "card_number"]
        for col in required_cols:
            if col in pdf.columns:
                results["checks"].append(f"✓ Column '{col}' exists")
            else:
                results["success"] = False
                results["failed_checks"].append(f"✗ Column '{col}' missing")
        
        # Check 2: No null values in critical columns
        for col in ["transaction_id", "transaction_date", "merchant_name", "amount"]:
            if col in pdf.columns:
                null_count = pdf[col].isnull().sum()
                if null_count == 0:
                    results["checks"].append(f"✓ No nulls in '{col}'")
                else:
                    results["success"] = False
                    results["failed_checks"].append(f"✗ {null_count} nulls in '{col}'")
        
        # Check 3: Unique transaction IDs
        if "transaction_id" in pdf.columns:
            dup_count = pdf["transaction_id"].duplicated().sum()
            if dup_count == 0:
                results["checks"].append(f"✓ All transaction_ids unique")
            else:
                results["success"] = False
                results["failed_checks"].append(f"✗ {dup_count} duplicate transaction_ids")
        
        # Check 4: Amount range
        if "amount" in pdf.columns:
            invalid_amounts = ((pdf["amount"] < 1) | (pdf["amount"] > 10000000)).sum()
            if invalid_amounts == 0:
                results["checks"].append(f"✓ All amounts in valid range (1-10M)")
            else:
                results["success"] = False
                results["failed_checks"].append(f"✗ {invalid_amounts} amounts out of range")
        
        # Print and save results
        self._print_summary(results)
        self._save_report(results, "bronze")
        
        return results
    
    def validate_silver_layer(self, df) -> Dict[str, Any]:
        """
        Validate Silver layer data
        
        Args:
            df: Spark DataFrame to validate
            
        Returns:
            Dict with validation results
        """
        print("\n🔍 Validating Silver Layer...")
        
        results = {
            "layer": "silver",
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "checks": [],
            "failed_checks": []
        }
        
        # Convert to Pandas for validation
        row_count = df.count()
        sample_size = min(10000, row_count)
        pdf = df.limit(sample_size).toPandas()
        
        # Check 1: Required columns exist
        required_cols = ["transaction_id", "normalized_merchant", "transaction_type", "processed_at"]
        for col in required_cols:
            if col in pdf.columns:
                results["checks"].append(f"✓ Column '{col}' exists")
            else:
                results["success"] = False
                results["failed_checks"].append(f"✗ Column '{col}' missing")
        
        # Check 2: Transaction type values
        if "transaction_type" in pdf.columns:
            valid_types = ["INCOME", "EXPENSE"]
            invalid_types = ~pdf["transaction_type"].isin(valid_types)
            invalid_count = invalid_types.sum()
            if invalid_count == 0:
                results["checks"].append(f"✓ All transaction_types valid")
            else:
                results["success"] = False
                results["failed_checks"].append(f"✗ {invalid_count} invalid transaction_types")
        
        # Check 3: No nulls in critical columns
        for col in ["transaction_id", "normalized_merchant", "amount"]:
            if col in pdf.columns:
                null_count = pdf[col].isnull().sum()
                if null_count == 0:
                    results["checks"].append(f"✓ No nulls in '{col}'")
                else:
                    results["success"] = False
                    results["failed_checks"].append(f"✗ {null_count} nulls in '{col}'")
        
        # Check 4: Unique transaction IDs
        if "transaction_id" in pdf.columns:
            dup_count = pdf["transaction_id"].duplicated().sum()
            if dup_count == 0:
                results["checks"].append(f"✓ All transaction_ids unique")
            else:
                results["success"] = False
                results["failed_checks"].append(f"✗ {dup_count} duplicate transaction_ids")
        
        # Check 5: Amount range
        if "amount" in pdf.columns:
            invalid_amounts = ((pdf["amount"] < 1) | (pdf["amount"] > 10000000)).sum()
            if invalid_amounts == 0:
                results["checks"].append(f"✓ All amounts in valid range")
            else:
                results["success"] = False
                results["failed_checks"].append(f"✗ {invalid_amounts} amounts out of range")
        
        # Print and save results
        self._print_summary(results)
        self._save_report(results, "silver")
        
        return results
    
    def validate_gold_fact_table(self, df) -> Dict[str, Any]:
        """
        Validate Gold fact table
        
        Args:
            df: Spark DataFrame to validate
            
        Returns:
            Dict with validation results
        """
        print("\n🔍 Validating Gold Fact Table...")
        
        results = {
            "layer": "gold_fact",
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "checks": [],
            "failed_checks": []
        }
        
        # Convert to Pandas for validation
        row_count = df.count()
        sample_size = min(10000, row_count)
        pdf = df.limit(sample_size).toPandas()
        
        # Check 1: Required columns exist
        required_cols = ["transaction_key", "date_key", "merchant_key", "category_key", "amount"]
        for col in required_cols:
            if col in pdf.columns:
                results["checks"].append(f"✓ Column '{col}' exists")
            else:
                results["success"] = False
                results["failed_checks"].append(f"✗ Column '{col}' missing")
        
        # Check 2: No nulls in foreign keys
        for col in ["date_key", "merchant_key", "category_key"]:
            if col in pdf.columns:
                null_count = pdf[col].isnull().sum()
                if null_count == 0:
                    results["checks"].append(f"✓ No nulls in '{col}'")
                else:
                    results["success"] = False
                    results["failed_checks"].append(f"✗ {null_count} nulls in '{col}'")
        
        # Check 3: Unique transaction keys
        if "transaction_key" in pdf.columns:
            dup_count = pdf["transaction_key"].duplicated().sum()
            if dup_count == 0:
                results["checks"].append(f"✓ All transaction_keys unique")
            else:
                results["success"] = False
                results["failed_checks"].append(f"✗ {dup_count} duplicate transaction_keys")
        
        # Check 4: Date key format (YYYYMMDD)
        if "date_key" in pdf.columns:
            invalid_dates = ((pdf["date_key"] < 20230101) | (pdf["date_key"] > 20251231)).sum()
            if invalid_dates == 0:
                results["checks"].append(f"✓ All date_keys in valid range")
            else:
                results["success"] = False
                results["failed_checks"].append(f"✗ {invalid_dates} invalid date_keys")
        
        # Print and save results
        self._print_summary(results)
        self._save_report(results, "gold_fact")
        
        return results
    
    def validate_gold_dimension(self, df, dimension_name: str) -> Dict[str, Any]:
        """
        Validate Gold dimension table
        
        Args:
            df: Spark DataFrame to validate
            dimension_name: Name of dimension (date, category, merchant)
            
        Returns:
            Dict with validation results
        """
        print(f"\n🔍 Validating Gold Dimension: {dimension_name}...")
        
        results = {
            "layer": f"gold_dim_{dimension_name}",
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "checks": [],
            "failed_checks": []
        }
        
        # Convert to Pandas for validation
        pdf = df.toPandas()
        
        if dimension_name == "date":
            # Date dimension validations
            required_cols = ["date_key", "full_date", "year", "month"]
            for col in required_cols:
                if col in pdf.columns:
                    results["checks"].append(f"✓ Column '{col}' exists")
                else:
                    results["success"] = False
                    results["failed_checks"].append(f"✗ Column '{col}' missing")
            
            if "date_key" in pdf.columns:
                dup_count = pdf["date_key"].duplicated().sum()
                if dup_count == 0:
                    results["checks"].append(f"✓ All date_keys unique")
                else:
                    results["success"] = False
                    results["failed_checks"].append(f"✗ {dup_count} duplicate date_keys")
            
        elif dimension_name == "category":
            # Category dimension validations
            required_cols = ["category_key", "category_name", "level_1"]
            for col in required_cols:
                if col in pdf.columns:
                    results["checks"].append(f"✓ Column '{col}' exists")
                else:
                    results["success"] = False
                    results["failed_checks"].append(f"✗ Column '{col}' missing")
            
            if "category_key" in pdf.columns:
                dup_count = pdf["category_key"].duplicated().sum()
                if dup_count == 0:
                    results["checks"].append(f"✓ All category_keys unique")
                else:
                    results["success"] = False
                    results["failed_checks"].append(f"✗ {dup_count} duplicate category_keys")
            
        elif dimension_name == "merchant":
            # Merchant dimension validations
            required_cols = ["merchant_key", "merchant_name", "normalized_name"]
            for col in required_cols:
                if col in pdf.columns:
                    results["checks"].append(f"✓ Column '{col}' exists")
                else:
                    results["success"] = False
                    results["failed_checks"].append(f"✗ Column '{col}' missing")
            
            if "merchant_key" in pdf.columns:
                dup_count = pdf["merchant_key"].duplicated().sum()
                if dup_count == 0:
                    results["checks"].append(f"✓ All merchant_keys unique")
                else:
                    results["success"] = False
                    results["failed_checks"].append(f"✗ {dup_count} duplicate merchant_keys")
        
        # Print and save results
        self._print_summary(results)
        self._save_report(results, f"gold_dim_{dimension_name}")
        
        return results
    
    def _print_summary(self, results: Dict[str, Any]):
        """Print validation summary"""
        if results["success"]:
            print(f"✅ {results['layer'].upper()} validation PASSED")
        else:
            print(f"❌ {results['layer'].upper()} validation FAILED")
        
        print(f"   Total checks: {len(results['checks']) + len(results['failed_checks'])}")
        print(f"   Passed: {len(results['checks'])}")
        print(f"   Failed: {len(results['failed_checks'])}")
        
        if results["failed_checks"]:
            print(f"\n   Failed Checks:")
            for i, failure in enumerate(results["failed_checks"][:5], 1):
                print(f"   {i}. {failure}")
            
            if len(results["failed_checks"]) > 5:
                print(f"   ... and {len(results['failed_checks']) - 5} more")
    
    def _save_report(self, results: Dict[str, Any], layer_name: str):
        """Save validation report to JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.output_dir / f"{layer_name}_validation_{timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"   📄 Report saved: {report_file}")


def validate_pipeline_data(spark, bronze_path: str, silver_path: str, gold_path: str):
    """
    Validate all layers of the pipeline
    
    Args:
        spark: SparkSession
        bronze_path: Path to bronze Delta table
        silver_path: Path to silver Delta table
        gold_path: Path to gold Delta tables
    """
    validator = DataQualityValidator()
    
    print("\n" + "="*60)
    print("🔍 DATA QUALITY VALIDATION")
    print("="*60)
    
    all_passed = True
    
    # Validate Bronze
    try:
        df_bronze = spark.read.format("delta").load(bronze_path)
        bronze_results = validator.validate_bronze_layer(df_bronze)
        if not bronze_results["success"]:
            all_passed = False
    except Exception as e:
        print(f"❌ Bronze validation error: {e}")
        all_passed = False
    
    # Validate Silver
    try:
        df_silver = spark.read.format("delta").load(silver_path)
        silver_results = validator.validate_silver_layer(df_silver)
        if not silver_results["success"]:
            all_passed = False
    except Exception as e:
        print(f"❌ Silver validation error: {e}")
        all_passed = False
    
    # Validate Gold Dimensions
    for dim_name in ["date", "category", "merchant"]:
        try:
            dim_path = f"{gold_path}/dim_{dim_name}"
            df_dim = spark.read.format("delta").load(dim_path)
            dim_results = validator.validate_gold_dimension(df_dim, dim_name)
            if not dim_results["success"]:
                all_passed = False
        except Exception as e:
            print(f"❌ Dimension {dim_name} validation error: {e}")
            all_passed = False
    
    # Validate Gold Fact
    try:
        fact_path = f"{gold_path}/fact_transactions"
        df_fact = spark.read.format("delta").load(fact_path)
        fact_results = validator.validate_gold_fact_table(df_fact)
        if not fact_results["success"]:
            all_passed = False
    except Exception as e:
        print(f"❌ Fact table validation error: {e}")
        all_passed = False
    
    print("\n" + "="*60)
    if all_passed:
        print("✅ ALL VALIDATIONS PASSED")
    else:
        print("❌ SOME VALIDATIONS FAILED - Check reports for details")
    print("="*60 + "\n")
    
    return all_passed
