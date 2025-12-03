"""
Personal Finance Data Analysis
Jupyter Notebook for exploring Gold Layer Star Schema data
"""

# %%
# Setup and Imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Set plotting style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.family'] = 'AppleGothic'  # Korean font support

print("✅ Imports complete")

# %%
# Initialize Spark Session
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("FinanceAnalysis")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark session created")

# %%
# Load Gold Layer Tables
fact_df = spark.read.format("delta").load("../data/gold/fact_transactions")
dim_date = spark.read.format("delta").load("../data/gold/dim_date")
dim_category = spark.read.format("delta").load("../data/gold/dim_category")
dim_merchant = spark.read.format("delta").load("../data/gold/dim_merchant")

print(f"📊 Data Loaded:")
print(f"  - Fact Transactions: {fact_df.count():,} records")
print(f"  - Dim Date: {dim_date.count():,} records")
print(f"  - Dim Category: {dim_category.count():,} records")
print(f"  - Dim Merchant: {dim_merchant.count():,} records")

# %%
# 1. Monthly Spending Analysis
monthly_spending = spark.sql("""
    SELECT 
        d.year,
        d.month,
        d.month_name,
        COUNT(*) as transaction_count,
        SUM(f.amount) as total_amount,
        AVG(f.amount) as avg_amount,
        MIN(f.amount) as min_amount,
        MAX(f.amount) as max_amount
    FROM delta.`../data/gold/fact_transactions` f
    JOIN delta.`../data/gold/dim_date` d ON f.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month
""")

monthly_df = monthly_spending.toPandas()
print("\n📅 Monthly Spending Summary:")
print(monthly_df.to_string(index=False))

# %%
# Visualize Monthly Spending
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Total spending by month
monthly_df['year_month'] = monthly_df['year'].astype(str) + '-' + monthly_df['month'].astype(str).str.zfill(2)
ax1.bar(range(len(monthly_df)), monthly_df['total_amount'] / 1000000)
ax1.set_xlabel('Month')
ax1.set_ylabel('Total Spending (Million KRW)')
ax1.set_title('Monthly Total Spending')
ax1.set_xticks(range(len(monthly_df)))
ax1.set_xticklabels(monthly_df['year_month'], rotation=45, ha='right')
ax1.grid(axis='y', alpha=0.3)

# Transaction count by month
ax2.plot(range(len(monthly_df)), monthly_df['transaction_count'], marker='o', linewidth=2)
ax2.set_xlabel('Month')
ax2.set_ylabel('Transaction Count')
ax2.set_title('Monthly Transaction Count')
ax2.set_xticks(range(len(monthly_df)))
ax2.set_xticklabels(monthly_df['year_month'], rotation=45, ha='right')
ax2.grid(alpha=0.3)

plt.tight_layout()
plt.savefig('../notebooks/monthly_spending.png', dpi=150, bbox_inches='tight')
plt.show()

print("✅ Monthly spending chart saved")

# %%
# 2. Category Breakdown Analysis
category_spending = spark.sql("""
    SELECT 
        c.level_1 as main_category,
        c.level_2 as sub_category,
        COUNT(*) as transaction_count,
        SUM(f.amount) as total_amount,
        AVG(f.amount) as avg_amount
    FROM delta.`../data/gold/fact_transactions` f
    JOIN delta.`../data/gold/dim_category` c ON f.category_key = c.category_key
    GROUP BY c.level_1, c.level_2
    ORDER BY total_amount DESC
""")

category_df = category_spending.toPandas()
print("\n🏷️ Category Spending Summary:")
print(category_df.to_string(index=False))

# %%
# Visualize Category Spending
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

# Main category pie chart
main_category_total = category_df.groupby('main_category')['total_amount'].sum().sort_values(ascending=False)
colors = sns.color_palette('husl', len(main_category_total))
ax1.pie(main_category_total.values, labels=main_category_total.index, autopct='%1.1f%%', 
        colors=colors, startangle=90)
ax1.set_title('Spending by Main Category')

# Sub-category bar chart (top 15)
top_subcategories = category_df.nlargest(15, 'total_amount')
ax2.barh(range(len(top_subcategories)), top_subcategories['total_amount'] / 1000000)
ax2.set_yticks(range(len(top_subcategories)))
ax2.set_yticklabels(top_subcategories['main_category'] + ' - ' + top_subcategories['sub_category'])
ax2.set_xlabel('Total Spending (Million KRW)')
ax2.set_title('Top 15 Sub-Categories by Spending')
ax2.invert_yaxis()
ax2.grid(axis='x', alpha=0.3)

plt.tight_layout()
plt.savefig('../notebooks/category_breakdown.png', dpi=150, bbox_inches='tight')
plt.show()

print("✅ Category breakdown chart saved")

# %%
# 3. Top Merchants Analysis
top_merchants = spark.sql("""
    SELECT 
        m.merchant_name,
        c.level_1 as category,
        COUNT(*) as visit_count,
        SUM(f.amount) as total_spent,
        AVG(f.amount) as avg_transaction
    FROM delta.`../data/gold/fact_transactions` f
    JOIN delta.`../data/gold/dim_merchant` m ON f.merchant_key = m.merchant_key
    JOIN delta.`../data/gold/dim_category` c ON f.category_key = c.category_key
    WHERE m.is_current = true
    GROUP BY m.merchant_name, c.level_1
    ORDER BY total_spent DESC
    LIMIT 20
""")

merchant_df = top_merchants.toPandas()
print("\n🏪 Top 20 Merchants by Spending:")
print(merchant_df.to_string(index=False))

# %%
# Visualize Top Merchants
fig, ax = plt.subplots(figsize=(14, 8))

colors_map = {cat: color for cat, color in zip(merchant_df['category'].unique(), 
                                                sns.color_palette('Set2', len(merchant_df['category'].unique())))}
colors = [colors_map[cat] for cat in merchant_df['category']]

bars = ax.barh(range(len(merchant_df)), merchant_df['total_spent'] / 1000000, color=colors)
ax.set_yticks(range(len(merchant_df)))
ax.set_yticklabels(merchant_df['merchant_name'])
ax.set_xlabel('Total Spending (Million KRW)')
ax.set_title('Top 20 Merchants by Total Spending')
ax.invert_yaxis()
ax.grid(axis='x', alpha=0.3)

# Add legend
from matplotlib.patches import Patch
legend_elements = [Patch(facecolor=colors_map[cat], label=cat) 
                   for cat in merchant_df['category'].unique()]
ax.legend(handles=legend_elements, loc='lower right')

plt.tight_layout()
plt.savefig('../notebooks/top_merchants.png', dpi=150, bbox_inches='tight')
plt.show()

print("✅ Top merchants chart saved")

# %%
# 4. Weekend vs Weekday Analysis
weekend_analysis = spark.sql("""
    SELECT 
        d.is_weekend,
        CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
        COUNT(*) as transaction_count,
        SUM(f.amount) as total_amount,
        AVG(f.amount) as avg_amount
    FROM delta.`../data/gold/fact_transactions` f
    JOIN delta.`../data/gold/dim_date` d ON f.date_key = d.date_key
    GROUP BY d.is_weekend
    ORDER BY d.is_weekend
""")

weekend_df = weekend_analysis.toPandas()
print("\n📆 Weekend vs Weekday Spending:")
print(weekend_df.to_string(index=False))

# %%
# Visualize Weekend vs Weekday
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Transaction count comparison
ax1.bar(weekend_df['day_type'], weekend_df['transaction_count'], color=['#3498db', '#e74c3c'])
ax1.set_ylabel('Transaction Count')
ax1.set_title('Transaction Count: Weekday vs Weekend')
ax1.grid(axis='y', alpha=0.3)

# Average amount comparison
ax2.bar(weekend_df['day_type'], weekend_df['avg_amount'], color=['#3498db', '#e74c3c'])
ax2.set_ylabel('Average Amount (KRW)')
ax2.set_title('Average Transaction Amount: Weekday vs Weekend')
ax2.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('../notebooks/weekend_analysis.png', dpi=150, bbox_inches='tight')
plt.show()

print("✅ Weekend analysis chart saved")

# %%
# 5. Summary Statistics
total_stats = spark.sql("""
    SELECT 
        COUNT(*) as total_transactions,
        SUM(amount) as total_spent,
        AVG(amount) as avg_transaction,
        MIN(amount) as min_transaction,
        MAX(amount) as max_transaction,
        STDDEV(amount) as std_transaction
    FROM delta.`../data/gold/fact_transactions`
""")

stats_df = total_stats.toPandas()
print("\n📊 Overall Statistics:")
print(f"  Total Transactions: {stats_df['total_transactions'][0]:,}")
print(f"  Total Spent: ₩{stats_df['total_spent'][0]:,.0f}")
print(f"  Average Transaction: ₩{stats_df['avg_transaction'][0]:,.0f}")
print(f"  Min Transaction: ₩{stats_df['min_transaction'][0]:,.0f}")
print(f"  Max Transaction: ₩{stats_df['max_transaction'][0]:,.0f}")
print(f"  Std Dev: ₩{stats_df['std_transaction'][0]:,.0f}")

# %%
# 6. Daily Spending Trend
daily_trend = spark.sql("""
    SELECT 
        d.full_date,
        d.day_name,
        COUNT(*) as transaction_count,
        SUM(f.amount) as total_amount
    FROM delta.`../data/gold/fact_transactions` f
    JOIN delta.`../data/gold/dim_date` d ON f.date_key = d.date_key
    GROUP BY d.full_date, d.day_name
    ORDER BY d.full_date
""")

daily_df = daily_trend.toPandas()
daily_df['full_date'] = pd.to_datetime(daily_df['full_date'])

# Plot daily trend
fig, ax = plt.subplots(figsize=(16, 6))
ax.plot(daily_df['full_date'], daily_df['total_amount'] / 1000, linewidth=1, alpha=0.7)
ax.set_xlabel('Date')
ax.set_ylabel('Daily Spending (Thousand KRW)')
ax.set_title('Daily Spending Trend')
ax.grid(alpha=0.3)

# Add 7-day moving average
daily_df['ma_7'] = daily_df['total_amount'].rolling(window=7).mean()
ax.plot(daily_df['full_date'], daily_df['ma_7'] / 1000, linewidth=2, color='red', label='7-day MA')
ax.legend()

plt.tight_layout()
plt.savefig('../notebooks/daily_trend.png', dpi=150, bbox_inches='tight')
plt.show()

print("✅ Daily trend chart saved")

# %%
print("\n" + "="*60)
print("✅ Analysis Complete!")
print("="*60)
print("\nGenerated Charts:")
print("  - monthly_spending.png")
print("  - category_breakdown.png")
print("  - top_merchants.png")
print("  - weekend_analysis.png")
print("  - daily_trend.png")
print("\nAll charts saved to: notebooks/")
print("="*60)

# %%
# Stop Spark session
spark.stop()
print("\n✅ Spark session stopped")
