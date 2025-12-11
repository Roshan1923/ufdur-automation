#!/usr/bin/env python3
"""
UFDUR Pipeline Runner
=====================
Main script that runs the complete pipeline:
1. Check health.mil for new UFDUR files
2. Download new files
3. Extract data from Excel to CSV
4. Combine all quarters into master file
"""

import os
import sys
from pathlib import Path

# Add scripts directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from check_and_download import check_and_download
from extract_ufdur import extract_ufdur
from combine_quarters import combine_quarters


def run_pipeline():
    """Run the complete UFDUR data pipeline."""
    
    print()
    print("=" * 60)
    print("       UFDUR AUTOMATED PIPELINE")
    print("=" * 60)
    print()
    
    # Step 1: Check for new files and download
    print("STEP 1: Checking for new UFDUR files...")
    print("-" * 60)
    new_files = check_and_download()
    print()
    
    # Step 2: Extract data from any new Excel files
    if new_files:
        print("STEP 2: Extracting data from new files...")
        print("-" * 60)
        
        for file_info in new_files:
            input_path = file_info['path']
            quarter = file_info['quarter']
            output_prefix = f"data/processed/{quarter}"
            
            print(f"\nProcessing {quarter}...")
            try:
                extract_ufdur(input_path, output_prefix)
                print(f"  ✓ {quarter} extracted successfully")
            except Exception as e:
                print(f"  ✗ Error extracting {quarter}: {e}")
        
        print()
        
        # Step 3: Combine all quarters
        print("STEP 3: Combining all quarters...")
        print("-" * 60)
        try:
            combine_quarters()
            print("  ✓ Master file updated")
        except Exception as e:
            print(f"  ✗ Error combining quarters: {e}")
    else:
        print("STEP 2: No new files to extract")
        print("-" * 60)
        print("  Skipping extraction - no new data")
        print()
        
        # Still combine in case we need to regenerate master
        existing_files = list(Path("data/processed").glob("*_combined.csv"))
        existing_files = [f for f in existing_files if "Master" not in f.name]
        
        if existing_files:
            print("STEP 3: Regenerating master file...")
            print("-" * 60)
            try:
                combine_quarters()
            except Exception as e:
                print(f"  ✗ Error: {e}")
        else:
            print("STEP 3: No data files found")
            print("-" * 60)
            print("  No quarterly files to combine")
    
    print()
    print("=" * 60)
    print("       PIPELINE COMPLETE")
    print("=" * 60)
    print()
    
    # Summary
    master_file = Path("data/processed/UFDUR_Master.csv")
    if master_file.exists():
        size_mb = master_file.stat().st_size / (1024 * 1024)
        print(f"Master file: {master_file}")
        print(f"Size: {size_mb:.1f} MB")
    else:
        print("Master file not yet created (no data downloaded)")


if __name__ == "__main__":
    run_pipeline()
