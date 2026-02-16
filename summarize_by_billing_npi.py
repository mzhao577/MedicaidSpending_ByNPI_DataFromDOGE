#!/usr/bin/env python3
"""
Summarize Medicaid Provider Spending data by Billing NPI.
Outputs: billing_npi, total_claims, total_paid (one line per NPI)
"""

import pandas as pd

PARQUET_FILE = "medicaid-provider-spending.parquet"
OUTPUT_FILE = "billing_npi_summary.csv"

print("Reading parquet file...")
df = pd.read_parquet(PARQUET_FILE)

print("Aggregating by Billing NPI...")
summary = df.groupby('BILLING_PROVIDER_NPI_NUM').agg({
    'TOTAL_CLAIMS': 'sum',
    'TOTAL_PAID': 'sum'
}).reset_index()

# Rename columns for clarity
summary.columns = ['billing_npi', 'total_claims', 'total_paid']

# Sort by total_paid descending
summary = summary.sort_values('total_paid', ascending=False)

# Save to CSV
summary.to_csv(OUTPUT_FILE, index=False)

print(f"\nSummary saved to: {OUTPUT_FILE}")
print(f"Total unique Billing NPIs: {len(summary):,}")
print(f"Total claims: {summary['total_claims'].sum():,}")
print(f"Total paid: ${summary['total_paid'].sum():,.2f}")

print("\n" + "=" * 60)
print("Top 10 Billing NPIs by Total Paid")
print("=" * 60)
print(summary.head(10).to_string(index=False))
