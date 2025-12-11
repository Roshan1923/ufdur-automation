#!/usr/bin/env python3
"""
UFDUR Quarter Combiner
======================
Combines multiple quarterly UFDUR CSV files into one master file.
"""

import pandas as pd
import os
from pathlib import Path


def combine_quarters(data_dir="data/processed", output_file="data/processed/UFDUR_Master.csv"):
    """Combine all quarterly CSV files into one master file."""
    
    print("=" * 60)
    print("UFDUR Quarter Combiner")
    print("=" * 60)
    print()
    
    data_path = Path(data_dir)
    
    if not data_path.exists():
        print(f"Error: '{data_dir}' folder not found!")
        return None
    
    # Find all combined CSV files
    combined_files = list(data_path.glob("*_combined.csv"))
    
    # Exclude the master file itself
    combined_files = [f for f in combined_files if "Master" not in f.name]
    
    if not combined_files:
        print(f"Error: No *_combined.csv files found in {data_dir}!")
        return None
    
    print(f"Found {len(combined_files)} quarterly files:")
    for f in sorted(combined_files):
        print(f"  - {f.name}")
    print()
    
    # Read and combine all files
    all_dfs = []
    for file in sorted(combined_files):
        print(f"Reading {file.name}...")
        df = pd.read_csv(file)
        print(f"  ✓ {len(df):,} rows")
        all_dfs.append(df)
    
    print()
    print("Combining all quarters...")
    master_df = pd.concat(all_dfs, ignore_index=True)
    
    # Ensure output directory exists
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save master file
    master_df.to_csv(output_file, index=False)
    
    print()
    print("=" * 60)
    print("Combination complete!")
    print("=" * 60)
    print()
    print(f"Saved: {output_file}")
    print(f"Total rows: {len(master_df):,}")
    print()
    
    # Summary by quarter
    print("Summary by Quarter:")
    master_df['ThirtyDayEquiv'] = pd.to_numeric(master_df['ThirtyDayEquiv'], errors='coerce')
    quarter_summary = master_df.groupby('Qtr').agg({
        'ThirtyDayEquiv': 'sum',
        'ProductName': 'count'
    }).rename(columns={'ProductName': 'Records'})
    
    for qtr in sorted(quarter_summary.index):
        records = quarter_summary.loc[qtr, 'Records']
        total = quarter_summary.loc[qtr, 'ThirtyDayEquiv']
        print(f"  {qtr}: {records:,} records, {total:,.2f} 30-Day Equiv")
    
    return master_df


if __name__ == "__main__":
    combine_quarters()
