#!/usr/bin/env python3
"""
Read and inspect the DOGE Medicaid Provider Spending parquet file.
"""

import os
import pandas as pd

# File path
PARQUET_FILE = "medicaid-provider-spending.parquet"

# Get file size
file_size_bytes = os.path.getsize(PARQUET_FILE)
file_size_gb = file_size_bytes / (1024 ** 3)

print("=" * 60)
print("DOGE Medicaid Provider Spending Data")
print("=" * 60)

print(f"\nFile: {PARQUET_FILE}")
print(f"File size: {file_size_gb:.2f} GB ({file_size_bytes:,} bytes)")

# Read the parquet file
print("\nReading parquet file...")
df = pd.read_parquet(PARQUET_FILE)

print(f"\nTotal rows: {len(df):,}")
print(f"Total columns: {len(df.columns)}")

print("\n" + "=" * 60)
print("Column Headers")
print("=" * 60)
for i, col in enumerate(df.columns, 1):
    print(f"  {i}. {col}")

print("\n" + "=" * 60)
print("Column Data Types")
print("=" * 60)
for col in df.columns:
    print(f"  {col}: {df[col].dtype}")

print("\n" + "=" * 60)
print("First 5 Rows")
print("=" * 60)
print(df.head())

print("\n" + "=" * 60)
print("Basic Statistics")
print("=" * 60)
print(df.describe())

# Export column names to CSV
print("\n" + "=" * 60)
print("Exporting Column Names to CSV")
print("=" * 60)

column_info = pd.DataFrame({
    'column_number': range(1, len(df.columns) + 1),
    'column_name': df.columns,
    'data_type': [str(df[col].dtype) for col in df.columns]
})

output_file = "column_names.csv"
column_info.to_csv(output_file, index=False)
print(f"Column names exported to: {output_file}")
