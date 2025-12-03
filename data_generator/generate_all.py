"""Generate all sample data"""
import argparse
from pathlib import Path
from transaction_generator import TransactionGenerator


def main():
    parser = argparse.ArgumentParser(description='Generate sample transaction data')
    parser.add_argument('--records', type=int, default=100000, 
                       help='Number of records to generate (default: 100000)')
    parser.add_argument('--output-dir', type=str, default='data/raw',
                       help='Output directory (default: data/raw)')
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate data
    generator = TransactionGenerator()
    
    print(f"\n{'='*60}")
    print(f"Generating {args.records:,} card transactions")
    print(f"{'='*60}\n")
    
    df = generator.generate_card_transactions(num_records=args.records)
    
    # Save to Parquet
    output_path = output_dir / 'card_transactions.parquet'
    generator.save_to_parquet(df, output_path)
    
    print(f"\n{'='*60}")
    print(f"✅ Data generation complete!")
    print(f"{'='*60}")
    print(f"Output: {output_path}")
    print(f"Records: {len(df):,}")
    print(f"Date range: {df['transaction_date'].min()} ~ {df['transaction_date'].max()}")
    print(f"Total amount: ₩{df['amount'].sum():,.0f}")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
