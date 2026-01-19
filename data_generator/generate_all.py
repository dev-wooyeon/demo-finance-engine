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
    
    output_path = output_dir / 'card_transactions.parquet'
    
    if args.records > 1000000:
        # Use batch generation for large datasets
        generator.generate_and_save_in_batches(
            output_path=output_path,
            num_records=args.records,
            batch_size=1000000
        )
    else:
        # Use standard generation for small datasets
        df = generator.generate_card_transactions(num_records=args.records)
        generator.save_to_parquet(df, output_path)
    
    # Verify file existence and basic info
    if output_path.exists():
        file_size = output_path.stat().st_size / (1024 * 1024)
        print(f"\n{'='*60}")
        print(f"✅ Data generation complete!")
        print(f"{'='*60}")
        print(f"Output: {output_path}")
        print(f"File size: {file_size:.2f} MB")
        print(f"{'='*60}\n")
    else:
        print(f"❌ Error: Failed to generate {output_path}")


if __name__ == '__main__':
    main()
