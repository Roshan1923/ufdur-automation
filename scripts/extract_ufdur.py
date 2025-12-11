#!/usr/bin/env python3
"""
UFDUR Pivot Cache Extractor v2
==============================
Extracts raw data from DoD Uniform Formulary Drug Utilization Report (UFDUR) Excel files.
Auto-detects which pivot cache contains main data vs outliers.
"""

import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import sys
import os
from pathlib import Path


def get_cache_fields(xlsx_path, cache_num):
    """Get field names from a pivot cache definition."""
    ns = {'ns': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    with zipfile.ZipFile(xlsx_path, 'r') as z:
        cache_def_path = f'xl/pivotCache/pivotCacheDefinition{cache_num}.xml'
        if cache_def_path not in z.namelist():
            return None
        cache_def = z.read(cache_def_path)
        def_root = ET.fromstring(cache_def)
        cache_fields = def_root.findall('.//ns:cacheField', ns)
        return [f.get('name') for f in cache_fields]


def detect_cache_types(xlsx_path):
    """
    Detect which cache is main UFDUR and which is outliers.
    Main UFDUR has 'ClaimState' column, Outliers has 'TotalOrphanQTY'.
    """
    cache1_fields = get_cache_fields(xlsx_path, 1)
    cache2_fields = get_cache_fields(xlsx_path, 2)
    
    if cache1_fields and 'ClaimState' in cache1_fields:
        return (1, 2)
    elif cache2_fields and 'ClaimState' in cache2_fields:
        return (2, 1)
    else:
        print("  Warning: Could not auto-detect cache types, assuming cache 1 is main")
        return (1, 2)


def extract_pivot_cache(xlsx_path, cache_num):
    """Extract raw data from an Excel pivot cache."""
    
    ns = {'ns': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    
    with zipfile.ZipFile(xlsx_path, 'r') as z:
        cache_def_path = f'xl/pivotCache/pivotCacheDefinition{cache_num}.xml'
        cache_rec_path = f'xl/pivotCache/pivotCacheRecords{cache_num}.xml'
        
        if cache_def_path not in z.namelist():
            raise FileNotFoundError(f"Pivot cache {cache_num} not found in {xlsx_path}")
        
        cache_def = z.read(cache_def_path)
        def_root = ET.fromstring(cache_def)
        
        cache_fields = def_root.findall('.//ns:cacheField', ns)
        field_names = [f.get('name') for f in cache_fields]
        
        shared_items = {}
        for i, field in enumerate(cache_fields):
            items = field.find('ns:sharedItems', ns)
            if items is not None:
                values = []
                for item in items:
                    tag = item.tag.split('}')[1] if '}' in item.tag else item.tag
                    if tag == 's':
                        values.append(item.get('v'))
                    elif tag == 'n':
                        values.append(float(item.get('v')))
                    elif tag == 'm':
                        values.append(None)
                    elif tag == 'b':
                        values.append(item.get('v') == '1')
                    else:
                        values.append(item.get('v'))
                shared_items[i] = values
        
        cache_records = z.read(cache_rec_path)
        records_root = ET.fromstring(cache_records)
        
        data = []
        for record in records_root.findall('.//ns:r', ns):
            row = []
            field_idx = 0
            for item in record:
                tag = item.tag.split('}')[1] if '}' in item.tag else item.tag
                
                if tag == 'x':
                    idx = int(item.get('v'))
                    if field_idx in shared_items and idx < len(shared_items[field_idx]):
                        row.append(shared_items[field_idx][idx])
                    else:
                        row.append(None)
                elif tag == 'n':
                    row.append(float(item.get('v')))
                elif tag == 's':
                    row.append(item.get('v'))
                elif tag == 'm':
                    row.append(None)
                else:
                    row.append(item.get('v'))
                
                field_idx += 1
            
            data.append(row)
        
        df = pd.DataFrame(data, columns=field_names)
        return df


def standardize_columns(df, is_outliers=False):
    """Standardize column names to match across all files."""
    rename_map = {
        'TotalOrphanQTY': 'TotalQty',
        'TotalOrphanDS': 'TotalDS',
        'TotalOrphanRxCnt': 'TotalRxCnt',
        'TotalOrphanThirtyDayEquiv': 'ThirtyDayEquiv'
    }
    df = df.rename(columns=rename_map)
    
    if 'ClaimState' not in df.columns:
        df['ClaimState'] = 'AGGREGATED'
    if 'MTF' not in df.columns:
        df['MTF'] = ''
    if 'MTFbranchofservice' not in df.columns:
        df['MTFbranchofservice'] = ''
    
    return df


def extract_ufdur(input_file, output_prefix):
    """Main extraction function."""
    
    print("=" * 60)
    print("UFDUR Pivot Cache Extractor v2")
    print("=" * 60)
    print(f"Input file: {input_file}")
    print(f"Output prefix: {output_prefix}")
    print()
    
    output_dir = os.path.dirname(output_prefix)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print("Detecting cache structure...")
    main_cache, outliers_cache = detect_cache_types(input_file)
    print(f"  Main UFDUR: cache {main_cache}")
    print(f"  Outliers: cache {outliers_cache}")
    print()
    
    print("Extracting main UFDUR data...")
    try:
        df_main = extract_pivot_cache(input_file, cache_num=main_cache)
        df_main = standardize_columns(df_main, is_outliers=False)
        df_main['Source'] = 'MAIN'
        print(f"  ✓ Extracted {len(df_main):,} records")
    except Exception as e:
        print(f"  ✗ Error: {e}")
        df_main = None
    
    print("Extracting outliers data...")
    try:
        df_outliers = extract_pivot_cache(input_file, cache_num=outliers_cache)
        df_outliers = standardize_columns(df_outliers, is_outliers=True)
        df_outliers['Source'] = 'OUTLIERS'
        print(f"  ✓ Extracted {len(df_outliers):,} records")
    except Exception as e:
        print(f"  ✗ Error: {e}")
        df_outliers = None
    
    print()
    
    all_columns = ['Qtr', 'ClaimState', 'DOD_UF_CLASS', 'DSF', 'ProductName', 
                   'SpecialtyDrugFlag', 'point_of_service', 'MTF', 'MTFbranchofservice',
                   'TotalQty', 'TotalDS', 'TotalRxCnt', 'ThirtyDayEquiv', 'Source']
    
    if df_main is not None:
        for col in all_columns:
            if col not in df_main.columns:
                df_main[col] = ''
        df_main = df_main[all_columns]
    
    if df_outliers is not None:
        for col in all_columns:
            if col not in df_outliers.columns:
                df_outliers[col] = ''
        df_outliers = df_outliers[all_columns]
    
    if df_main is not None and df_outliers is not None:
        df_combined = pd.concat([df_main, df_outliers], ignore_index=True)
        combined_output = f"{output_prefix}_combined.csv"
        df_combined.to_csv(combined_output, index=False)
        print(f"Saved: {combined_output} ({len(df_combined):,} rows)")
        return df_combined
    
    return None


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_ufdur.py <input_file.xlsx> [output_prefix]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_prefix = sys.argv[2] if len(sys.argv) >= 3 else Path(input_file).stem.split('_')[0]
    extract_ufdur(input_file, output_prefix)
