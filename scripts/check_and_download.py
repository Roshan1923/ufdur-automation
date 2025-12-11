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
from pathlib import Path
from datetime import datetime


# Configuration
UFDUR_PAGE_URL = "https://health.mil/Military-Health-Topics/Access-Cost-Quality-and-Safety/Pharmacy-Operations/DOD-PT-Committee"
DATA_DIR = "data"
RAW_DIR = f"{DATA_DIR}/raw"
PROCESSED_DIR = f"{DATA_DIR}/processed"
TRACKER_FILE = f"{DATA_DIR}/downloaded_files.json"


def setup_directories():
    """Create necessary directories if they don't exist."""
    Path(RAW_DIR).mkdir(parents=True, exist_ok=True)
    Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)


def load_tracker():
    """Load list of already downloaded files."""
    if os.path.exists(TRACKER_FILE):
        with open(TRACKER_FILE, 'r') as f:
            return json.load(f)
    return {"downloaded": []}


def save_tracker(tracker):
    """Save list of downloaded files."""
    with open(TRACKER_FILE, 'w') as f:
        json.dump(tracker, f, indent=2)


def find_ufdur_links(html_content):
    """
    Find UFDUR Excel file links in the page HTML.
    Returns list of (filename, url) tuples.
    """
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
    """Download a file from URL to local path."""
    print(f"  Downloading: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=60, stream=True)
        response.raise_for_status()
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"  ✓ Saved to: {output_path}")
        return True
        
    except Exception as e:
        print(f"  ✗ Error downloading: {e}")
        return False


def check_and_download():
    """Main function to check for new files and download them."""
    
    print("=" * 60)
    print("UFDUR Auto-Downloader")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print()
    
    setup_directories()
    tracker = load_tracker()
    
    print("Fetc
