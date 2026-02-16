#!/usr/bin/env python3
"""
Look up business names for NPIs using the CMS NPI Registry API.
https://npiregistry.cms.hhs.gov/api-page
"""

import pandas as pd
import requests
import time
from tqdm import tqdm

INPUT_FILE = "top1000_npi.csv"
OUTPUT_FILE = "top1000_npi_with_names.csv"
API_URL = "https://npiregistry.cms.hhs.gov/api/"

def lookup_npi(npi):
    """
    Look up a single NPI using the CMS API.
    Returns dict with provider info or None if not found.
    """
    params = {
        "number": npi,
        "version": "2.1"
    }

    try:
        response = requests.get(API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("result_count", 0) == 0:
            return None

        result = data["results"][0]
        basic = result.get("basic", {})

        # Determine if organization or individual (enumeration_type is at top level)
        enumeration_type = result.get("enumeration_type", "")

        if enumeration_type == "NPI-2":  # Organization
            name = basic.get("organization_name", "")
            provider_type = "Organization"
        else:  # Individual (NPI-1)
            first = basic.get("first_name", "")
            last = basic.get("last_name", "")
            credential = basic.get("credential", "")
            name = f"{first} {last}".strip()
            if credential:
                name += f", {credential}"
            provider_type = "Individual"

        # Get address
        addresses = result.get("addresses", [])
        practice_address = ""
        city = ""
        state = ""
        zip_code = ""

        for addr in addresses:
            if addr.get("address_purpose") == "LOCATION":
                practice_address = addr.get("address_1", "")
                city = addr.get("city", "")
                state = addr.get("state", "")
                zip_code = addr.get("postal_code", "")[:5] if addr.get("postal_code") else ""
                break

        # Get primary taxonomy (specialty)
        taxonomies = result.get("taxonomies", [])
        specialty = ""
        for tax in taxonomies:
            if tax.get("primary", False):
                specialty = tax.get("desc", "")
                break
        if not specialty and taxonomies:
            specialty = taxonomies[0].get("desc", "")

        return {
            "name": name,
            "provider_type": provider_type,
            "specialty": specialty,
            "address": practice_address,
            "city": city,
            "state": state,
            "zip": zip_code
        }

    except requests.exceptions.RequestException as e:
        print(f"Error looking up NPI {npi}: {e}")
        return None

def main():
    # Load NPIs
    print(f"Loading NPIs from {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)

    print(f"Looking up {len(df)} NPIs from CMS Registry...")
    print("This may take a few minutes...\n")

    # Look up each NPI
    results = []
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Looking up NPIs"):
        npi = row['billing_npi']
        info = lookup_npi(npi)

        if info:
            results.append({
                "rank": row['rank'],
                "billing_npi": npi,
                "name": info['name'],
                "provider_type": info['provider_type'],
                "specialty": info['specialty'],
                "city": info['city'],
                "state": info['state'],
                "zip": info['zip'],
                "total_claims": row['total_claims'],
                "total_paid": row['total_paid']
            })
        else:
            results.append({
                "rank": row['rank'],
                "billing_npi": npi,
                "name": "NOT FOUND",
                "provider_type": "",
                "specialty": "",
                "city": "",
                "state": "",
                "zip": "",
                "total_claims": row['total_claims'],
                "total_paid": row['total_paid']
            })

        # Small delay to avoid overwhelming the API
        time.sleep(0.1)

    # Create output dataframe
    result_df = pd.DataFrame(results)

    # Save to CSV
    result_df.to_csv(OUTPUT_FILE, index=False)

    print(f"\n{'=' * 70}")
    print(f"Results saved to: {OUTPUT_FILE}")
    print(f"Total NPIs: {len(result_df)}")
    print(f"Found: {len(result_df[result_df['name'] != 'NOT FOUND'])}")
    print(f"Not found: {len(result_df[result_df['name'] == 'NOT FOUND'])}")

    # Show breakdown by provider type
    print(f"\nProvider Type Breakdown:")
    print(result_df['provider_type'].value_counts().to_string())

    # Show top states
    print(f"\nTop 10 States:")
    print(result_df['state'].value_counts().head(10).to_string())

    print(f"\n{'=' * 70}")
    print("Top 20 NPIs with Names:")
    print("=" * 70)
    print(result_df.head(20)[['rank', 'billing_npi', 'name', 'state', 'total_paid']].to_string(index=False))

if __name__ == "__main__":
    main()
