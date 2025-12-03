"""Transaction data generator using Faker"""
from faker import Faker
import pandas as pd
from datetime import datetime, timedelta
import random
from merchant_catalog import get_merchant_catalog


class TransactionGenerator:
    """Generate realistic transaction data"""
    
    def __init__(self, seed=42):
        self.fake = Faker('ko_KR')
        Faker.seed(seed)
        random.seed(seed)
        self.merchants = get_merchant_catalog()
    
    def generate_card_transactions(
        self, 
        num_records=100000,
        start_date='2023-01-01',
        end_date='2024-12-31'
    ):
        """
        Generate card transaction data
        
        Args:
            num_records: Number of transactions to generate
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            pandas.DataFrame: Generated transaction data
        """
        print(f"Generating {num_records:,} card transactions...")
        
        data = []
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        for i in range(num_records):
            if i % 10000 == 0:
                print(f"  Progress: {i:,}/{num_records:,}")
            
            merchant = random.choice(self.merchants)
            txn_date = self.fake.date_between(start_date=start, end_date=end)
            
            # Generate amount within merchant's typical range
            amount = random.randint(
                merchant.get('min_amount', 1000),
                merchant.get('max_amount', 50000)
            )
            
            data.append({
                'transaction_id': self.fake.uuid4(),
                'transaction_date': txn_date,
                'merchant_name': merchant['name'],
                'merchant_category': f"{merchant['category']}-{merchant['subcategory']}",
                'amount': amount,
                'card_number': f"****{random.randint(1000, 9999)}",
                'created_at': datetime.now()
            })
        
        print(f"✅ Generated {num_records:,} transactions")
        return pd.DataFrame(data)
    
    def save_to_parquet(self, df, output_path):
        """Save DataFrame to Parquet with Spark-compatible timestamp format"""
        # Convert timestamp columns to millisecond precision for Spark compatibility
        df['created_at'] = pd.to_datetime(df['created_at']).dt.floor('ms')
        
        df.to_parquet(
            output_path, 
            index=False, 
            engine='pyarrow',
            use_deprecated_int96_timestamps=True  # Spark compatibility
        )
        print(f"✅ Saved to {output_path}")
