#!/usr/bin/env python3
"""
Download DOGE Medicaid Provider Spending Data

This script downloads the Medicaid Provider Spending dataset released by DOGE (Department
of Government Efficiency) from the HHS Open Data portal.

Data source: https://opendata.hhs.gov/datasets/medicaid-provider-spending/
Data contains: Provider-level Medicaid spending from T-MSIS (2018-2024)

Fields in dataset:
- BILLING_PROVIDER_NPI_NUM: Billing provider NPI number
- SERVICING_PROVIDER_NPI_NUM: Servicing provider NPI number
- HCPCS_CODE: Healthcare procedure code
- CLAIM_FROM_MONTH: Month of claim
- TOTAL_UNIQUE_BENEFICIARIES: Count of unique beneficiaries
- TOTAL_CLAIMS: Number of claims
- TOTAL_PAID: Total amount paid (USD)
"""

import os
import sys
import hashlib
import requests
from pathlib import Path
from tqdm import tqdm

# Base URL for the dataset
BASE_URL = "https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09"

# Available files with their approximate sizes
FILES = {
    "parquet": {
        "filename": "medicaid-provider-spending.parquet",
        "size": "2.9 GB",
        "description": "Columnar format - best for analytics (recommended)"
    },
    "zip": {
        "filename": "medicaid-provider-spending.csv.zip",
        "size": "3.6 GB",
        "description": "Compressed CSV - good balance of size and compatibility"
    },
    "csv": {
        "filename": "medicaid-provider-spending.csv",
        "size": "11.1 GB",
        "description": "Raw CSV - largest file, universal compatibility"
    }
}


def get_file_size(url: str) -> int:
    """Get the file size from the server via HEAD request."""
    response = requests.head(url, allow_redirects=True)
    return int(response.headers.get('content-length', 0))


def download_file(url: str, dest_path: Path, chunk_size: int = 8192) -> bool:
    """
    Download a file with progress bar and resume support.

    Args:
        url: URL to download from
        dest_path: Local path to save file
        chunk_size: Size of chunks to download

    Returns:
        True if download successful, False otherwise
    """
    # Check if partial download exists
    downloaded_size = 0
    if dest_path.exists():
        downloaded_size = dest_path.stat().st_size

    # Get total file size
    total_size = get_file_size(url)

    if downloaded_size >= total_size and total_size > 0:
        print(f"File already fully downloaded: {dest_path}")
        return True

    # Set up headers for resume
    headers = {}
    if downloaded_size > 0:
        headers['Range'] = f'bytes={downloaded_size}-'
        print(f"Resuming download from {downloaded_size / (1024**3):.2f} GB")

    # Start download
    mode = 'ab' if downloaded_size > 0 else 'wb'

    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()

        # Handle resume response
        if response.status_code == 206:  # Partial content
            remaining_size = total_size - downloaded_size
        else:
            remaining_size = total_size
            downloaded_size = 0
            mode = 'wb'

        with open(dest_path, mode) as f:
            with tqdm(
                total=total_size,
                initial=downloaded_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                desc=dest_path.name
            ) as pbar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

        print(f"Download complete: {dest_path}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"Download error: {e}")
        return False


def calculate_sha256(file_path: Path, chunk_size: int = 8192) -> str:
    """Calculate SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        with tqdm(
            total=file_path.stat().st_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            desc="Calculating SHA-256"
        ) as pbar:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                sha256_hash.update(chunk)
                pbar.update(len(chunk))
    return sha256_hash.hexdigest()


def list_available_formats():
    """Display available download formats."""
    print("\nAvailable download formats:")
    print("-" * 60)
    for fmt, info in FILES.items():
        print(f"  {fmt:8} | {info['size']:>8} | {info['description']}")
    print("-" * 60)


def main():
    """Main function to download Medicaid data."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Download DOGE Medicaid Provider Spending Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python download_medicaid_data.py                    # Download parquet (recommended)
  python download_medicaid_data.py --format zip       # Download compressed CSV
  python download_medicaid_data.py --format csv       # Download full CSV
  python download_medicaid_data.py --list             # List available formats
  python download_medicaid_data.py --output ./data    # Specify output directory
        """
    )

    parser.add_argument(
        '--format', '-f',
        choices=['parquet', 'zip', 'csv'],
        default='parquet',
        help='File format to download (default: parquet)'
    )

    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=Path('.'),
        help='Output directory (default: current directory)'
    )

    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List available formats and exit'
    )

    parser.add_argument(
        '--verify',
        action='store_true',
        help='Calculate SHA-256 checksum after download'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Download all formats'
    )

    args = parser.parse_args()

    if args.list:
        list_available_formats()
        return

    # Create output directory
    args.output.mkdir(parents=True, exist_ok=True)

    # Determine which formats to download
    formats_to_download = list(FILES.keys()) if args.all else [args.format]

    print("=" * 60)
    print("DOGE Medicaid Provider Spending Data Downloader")
    print("=" * 60)
    print(f"\nData source: https://opendata.hhs.gov/datasets/medicaid-provider-spending/")
    print(f"Output directory: {args.output.absolute()}")

    for fmt in formats_to_download:
        file_info = FILES[fmt]
        filename = file_info['filename']
        url = f"{BASE_URL}/{filename}"
        dest_path = args.output / filename

        print(f"\n{'=' * 60}")
        print(f"Downloading: {filename}")
        print(f"Size: {file_info['size']}")
        print(f"URL: {url}")
        print(f"{'=' * 60}\n")

        success = download_file(url, dest_path)

        if success and args.verify:
            print("\nVerifying file integrity...")
            checksum = calculate_sha256(dest_path)
            print(f"SHA-256: {checksum}")

        if not success:
            print(f"Failed to download {filename}")
            sys.exit(1)

    print("\n" + "=" * 60)
    print("Download complete!")
    print("=" * 60)

    # Print some helpful tips
    print("""
Next steps:
  - For Parquet files, use pandas or pyarrow:
      import pandas as pd
      df = pd.read_parquet('medicaid-provider-spending.parquet')

  - For ZIP files, extract first:
      import zipfile
      with zipfile.ZipFile('medicaid-provider-spending.csv.zip', 'r') as z:
          z.extractall('.')

  - For CSV files:
      import pandas as pd
      df = pd.read_csv('medicaid-provider-spending.csv')
""")


if __name__ == "__main__":
    main()
