#!/usr/bin/env python3
"""
UFDUR Auto-Downloader
=====================
Checks health.mil for new UFDUR files and downloads them.
"""

import requests
import re
import os
import json
import urllib3
from pathlib import Path
from datetime import datetime

# Disable SSL warnings (needed for health.mil)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DATA_DIR = "data"
RAW_DIR = f"{DATA_DIR}/raw"
PROCESSED_DIR = f"{DATA_DIR}/processed"
TRACKER_FILE = f"{DATA_DIR}/downloaded_files.json"
UFDUR_PAGE_URL = "https://health.mil/Military-Health-Topics/Access-Cost-Quality-and-Safety/Pharmacy-Operations/DOD-PT-Committee"


def setup_directories():
    Path(RAW_DIR).mkdir(parents=True, exist_ok=True)
    Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)


def load_tracker():
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE, 'r') as f:
            return json.load(f)
    return {"downloaded": []}


def save_tracker(tracker):
    with open(TRACKER_FILE, 'w') as f:
        json.dump(tracker, f, indent=2)


def find_ufdur_links(html_content):
    pattern = r'href=["\']([^"\']*UFDUR[^"\']*\.xlsx)["\']'
    matches = re.findall(pattern, html_content, re.IGNORECASE)
    
    results = []
    for url in matches:
        if url.startswith('/'):
            url = f"https://health.mil{url}"
        elif not url.startswith('http'):
            url = f"https://health.mil/{url}"
        
        filename = url.split('/')[-1]
        quarter_match = re.search(r'FY\d{2}Q\d', filename, re.IGNORECASE)
        quarter = quarter_match.group(0).upper() if quarter_match else None
        
        if quarter:
            results.append({
                'filename': filename,
                'url': url,
                'quarter': quarter
            })
    
    return results


def download_file(url, output_path):
    print(f"  Downloading: {url}")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    try:
        response = requests.get(url, headers=headers, timeout=60, stream=True, verify=False)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"  Saved to: {output_path}")
        return True
    except Exception as e:
        print(f"  Error downloading: {e}")
        return False


def check_and_download():
    print("=" * 60)
    print("UFDUR Auto-Downloader")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    setup_directories()
    tracker = load_tracker()
    
    print("Fetching UFDUR page...")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    try:
        response = requests.get(UFDUR_PAGE_URL, headers=headers, timeout=30, verify=False)
        response.raise_for_status()
        html_content = response.text
        print("  Page fetched successfully")
    except Exception as e:
        print(f"  Error fetching page: {e}")
        return []
    
    print("Searching for UFDUR files...")
    ufdur_files = find_ufdur_links(html_content)
    
    if not ufdur_files:
        print("  No UFDUR files found on page.")
        return []
    
    print(f"  Found {len(ufdur_files)} UFDUR file(s)")
    for f in ufdur_files:
        print(f"    - {f['quarter']}: {f['filename']}")
    
    new_files = []
    for file_info in ufdur_files:
        filename = file_info['filename']
        
        if filename in tracker['downloaded']:
            print(f"  Already downloaded: {filename}")
            continue
        
        print(f"  New file found: {filename}")
        output_path = os.path.join(RAW_DIR, filename)
        
        if download_file(file_info['url'], output_path):
            tracker['downloaded'].append(filename)
            new_files.append({
                'filename': filename,
                'path': output_path,
                'quarter': file_info['quarter']
            })
    
    save_tracker(tracker)
    print(f"Download complete! New files: {len(new_files)}")
    
    return new_files


if __name__ == "__main__":
    check_and_download()
