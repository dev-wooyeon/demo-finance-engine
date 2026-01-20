"""
Simple analysis script for quick data exploration
Run with: uv run python analytics/quick_analysis.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.spark_session import create_spark_session


def print_section(title):
    """Print section header"""
    print("\n" + "="*60)
    print(f"📊 {title}")
    print("="*60)


def main():
    """Run quick analysis on Gold Layer data"""
    
    print("\n🚀 Starting Quick Analysis...")
    spark = create_spark_session("QuickAnalysis")
    
    try:
        # Load Delta tables and register as temp views
        print("Loading Gold Layer tables...")
        
        fact_df = spark.read.format("delta").load("data/gold/fact_transactions")
        dim_date = spark.read.format("delta").load("data/gold/dim_date")
        dim_category = spark.read.format("delta").load("data/gold/dim_category")
        dim_merchant = spark.read.format("delta").load("data/gold/dim_merchant")
        dim_card = spark.read.format("delta").load("data/gold/dim_card")
        
        fact_df.createOrReplaceTempView("fact_transactions")
        dim_date.createOrReplaceTempView("dim_date")
        dim_category.createOrReplaceTempView("dim_category")
        dim_merchant.createOrReplaceTempView("dim_merchant")
        dim_card.createOrReplaceTempView("dim_card")
        
        print(f"✅ Loaded {fact_df.count():,} transactions\n")
        
        # 1. Monthly Spending
        print_section("Monthly Spending Summary")
        monthly = spark.sql("""
            SELECT 
                d.year,
                d.month_name,
                COUNT(*) as txn_count,
                ROUND(SUM(f.amount), 0) as total_amount,
                ROUND(AVG(f.amount), 0) as avg_amount
            FROM fact_transactions f
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.year, d.month, d.month_name
            ORDER BY d.year, d.month
        """)
        monthly.show(24, truncate=False)
        
        # 2. Category Breakdown
        print_section("Top Categories by Spending")
        categories = spark.sql("""
            SELECT 
                c.level_1 as category,
                COUNT(*) as txn_count,
                ROUND(SUM(f.amount), 0) as total_amount,
                ROUND(AVG(f.amount), 0) as avg_amount
            FROM fact_transactions f
            JOIN dim_category c ON f.category_key = c.category_key
            GROUP BY c.level_1
            ORDER BY total_amount DESC
        """)
        categories.show(truncate=False)
        
        # 3. Top Merchants
        print_section("Top 15 Merchants by Spending")
        merchants = spark.sql("""
            SELECT 
                m.merchant_name,
                c.level_1 as category,
                COUNT(*) as visits,
                ROUND(SUM(f.amount), 0) as total_spent
            FROM fact_transactions f
            JOIN dim_merchant m ON f.merchant_key = m.merchant_key
            JOIN dim_category c ON f.category_key = c.category_key
            WHERE m.is_current = true
            GROUP BY m.merchant_name, c.level_1
            ORDER BY total_spent DESC
            LIMIT 15
        """)
        merchants.show(15, truncate=False)
        
        # 4. Weekend vs Weekday
        print_section("Weekend vs Weekday Spending")
        weekend = spark.sql("""
            SELECT 
                CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
                COUNT(*) as txn_count,
                ROUND(SUM(f.amount), 0) as total_amount,
                ROUND(AVG(f.amount), 0) as avg_amount
            FROM fact_transactions f
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.is_weekend
        """)
        weekend.show(truncate=False)
        
        # 5. Card Spending Analysis
        print_section("Spending by Card Company & Type")
        card_analysis = spark.sql("""
            SELECT 
                c.card_company,
                c.card_type,
                COUNT(*) as txn_count,
                ROUND(SUM(f.amount), 0) as total_amount,
                ROUND(AVG(f.amount), 0) as avg_amount
            FROM fact_transactions f
            JOIN dim_card c ON f.card_key = c.card_key
            GROUP BY c.card_company, c.card_type
            ORDER BY total_amount DESC
        """)
        card_analysis.show(truncate=False)
        
        # 5. Overall Statistics
        print_section("Overall Statistics")
        stats = spark.sql("""
            SELECT 
                COUNT(*) as total_transactions,
                ROUND(SUM(amount), 0) as total_spent,
                ROUND(AVG(amount), 0) as avg_transaction,
                ROUND(MIN(amount), 0) as min_transaction,
                ROUND(MAX(amount), 0) as max_transaction
            FROM fact_transactions
        """)
        stats.show(truncate=False)
        
        print("\n" + "="*60)
        print("✅ Analysis Complete!")
        print("="*60 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
