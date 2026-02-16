#!/usr/bin/env python3
"""
Monthly summary for the top 1000 Billing NPIs by total paid.
Outputs:
  1. monthly_summary_top1000.csv - billing_npi, month, total_claims, total_paid
  2. monthly_trend_top1000.csv  - billing_npi, year, month_num, total_claims, total_paid (for trend plots)
"""

import pandas as pd

PARQUET_FILE = "medicaid-provider-spending.parquet"
OUTPUT_FILE = "monthly_summary_top1000.csv"
TREND_FILE = "monthly_trend_top1000.csv"
TOP_N = 1000

print("Reading parquet file...")
df = pd.read_parquet(PARQUET_FILE)

# Step 1: Get top 1000 billing NPIs by total paid
print(f"Identifying top {TOP_N} Billing NPIs by total paid...")
npi_totals = df.groupby('BILLING_PROVIDER_NPI_NUM')['TOTAL_PAID'].sum()
top_npis = npi_totals.nlargest(TOP_N).index.tolist()

# Create a rank mapping for top NPIs
npi_ranks = {npi: rank + 1 for rank, npi in enumerate(top_npis)}

print(f"Top {TOP_N} NPIs account for ${npi_totals[top_npis].sum():,.2f} in total payments")

# Step 2: Filter data to only include top NPIs
print("Filtering data for top NPIs...")
df_top = df[df['BILLING_PROVIDER_NPI_NUM'].isin(top_npis)]

# Step 3: Aggregate by billing NPI and month
print("Aggregating by Billing NPI and Month...")
monthly_summary = df_top.groupby(['BILLING_PROVIDER_NPI_NUM', 'CLAIM_FROM_MONTH']).agg({
    'TOTAL_CLAIMS': 'sum',
    'TOTAL_PAID': 'sum'
}).reset_index()

# Rename columns
monthly_summary.columns = ['billing_npi', 'month', 'total_claims', 'total_paid']

# Sort by billing_npi and month
monthly_summary = monthly_summary.sort_values(['billing_npi', 'month'])

# Save to CSV
monthly_summary.to_csv(OUTPUT_FILE, index=False)

print(f"\nMonthly summary saved to: {OUTPUT_FILE}")
print(f"Total rows: {len(monthly_summary):,}")
print(f"Unique Billing NPIs: {monthly_summary['billing_npi'].nunique():,}")
print(f"Date range: {monthly_summary['month'].min()} to {monthly_summary['month'].max()}")
print(f"Total claims: {monthly_summary['total_claims'].sum():,}")
print(f"Total paid: ${monthly_summary['total_paid'].sum():,.2f}")

# Step 4: Create trend file with year and month_num columns
print("\nCreating trend file for yearly comparison...")
trend_df = monthly_summary.copy()
trend_df['year'] = trend_df['month'].str[:4].astype(int)
trend_df['month_num'] = trend_df['month'].str[5:7].astype(int)
trend_df['rank'] = trend_df['billing_npi'].map(npi_ranks)
trend_df['npi_total_paid'] = trend_df['billing_npi'].map(npi_totals)

# Reorder columns
trend_df = trend_df[['rank', 'billing_npi', 'npi_total_paid', 'year', 'month_num', 'total_claims', 'total_paid']]
trend_df = trend_df.sort_values(['rank', 'year', 'month_num'])

# Save trend file
trend_df.to_csv(TREND_FILE, index=False)

print(f"Trend file saved to: {TREND_FILE}")

print("\n" + "=" * 70)
print("Sample Output - Monthly Summary (first 20 rows)")
print("=" * 70)
print(monthly_summary.head(20).to_string(index=False))

print("\n" + "=" * 70)
print("Sample Output - Trend File (first 20 rows)")
print("=" * 70)
print(trend_df.head(20).to_string(index=False))
