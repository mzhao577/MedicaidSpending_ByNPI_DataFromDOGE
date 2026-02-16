#!/usr/bin/env python3
"""
Streamlit Web App: Monthly trend visualization for top 1000 Billing NPIs.
Shows 7 yearly trends (2018-2024) on the same plot with different colors.

To run locally: streamlit run streamlit_monthly_trends.py
To deploy: Push to GitHub and connect to Streamlit Cloud
"""

import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

# Page config
st.set_page_config(
    page_title="Medicaid Provider Spending Trends",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("📊 Medicaid Provider Monthly Spending Trends")
st.markdown("Interactive visualization of top 1000 Billing NPIs (2018-2024)")

# Colors for each year
year_colors = {
    2018: '#1f77b4',  # blue
    2019: '#ff7f0e',  # orange
    2020: '#2ca02c',  # green
    2021: '#d62728',  # red
    2022: '#9467bd',  # purple
    2023: '#8c564b',  # brown
    2024: '#e377c2',  # pink
}

# Markers for each year
year_markers = {
    2018: 'o',   # circle
    2019: 's',   # square
    2020: '^',   # triangle up
    2021: 'D',   # diamond
    2022: 'v',   # triangle down
    2023: 'p',   # pentagon
    2024: '*',   # star
}


@st.cache_data
def load_data():
    """Load and preprocess data (cached for performance)."""
    df = pd.read_csv('monthly_summary_top1000.csv')
    npi_names_df = pd.read_csv('top1000_npi_with_names.csv')

    # Parse month into year and month number
    df['year'] = df['month'].str[:4].astype(int)
    df['month_num'] = df['month'].str[5:7].astype(int)

    # Create NPI names dictionary
    npi_names = dict(zip(npi_names_df['billing_npi'], npi_names_df['name']))

    # Get list of unique billing NPIs sorted by total paid
    npi_totals = df.groupby('billing_npi')['total_paid'].sum().sort_values(ascending=False)
    npi_list = npi_totals.index.tolist()

    return df, npi_names, npi_totals, npi_list


def plot_npi_trends(df, npi, npi_name, npi_total, rank, total_npis):
    """Create matplotlib plot for a specific NPI."""
    npi_data = df[df['billing_npi'] == npi]

    fig, ax = plt.subplots(figsize=(12, 7))

    # Plot each year as a separate line
    for year in sorted(year_colors.keys()):
        year_data = npi_data[npi_data['year'] == year].sort_values('month_num')
        if not year_data.empty:
            ax.plot(
                year_data['month_num'],
                year_data['total_paid'] / 1e6,  # Convert to millions
                marker=year_markers[year],
                color=year_colors[year],
                label=str(year),
                linewidth=2,
                markersize=8
            )

    # Formatting
    ax.set_xlabel('Month', fontsize=12)
    ax.set_ylabel('Total Paid (Millions $)', fontsize=12)
    ax.set_title(
        f'{npi_name}\n'
        f'NPI: {npi} | Rank: {rank}/{total_npis} | '
        f'Total Paid (2018-2024): ${npi_total:,.0f}',
        fontsize=11
    )
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
    ax.legend(title='Year', loc='upper right')
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)

    plt.tight_layout()
    return fig


# Load data
try:
    df, npi_names, npi_totals, npi_list = load_data()
    total_npis = len(npi_list)

    # Sidebar controls
    st.sidebar.header("Navigation")

    # Slider for NPI selection
    rank = st.sidebar.slider(
        "Select NPI Rank",
        min_value=1,
        max_value=total_npis,
        value=1,
        help="Rank 1 = highest total spending"
    )

    # Navigation buttons
    st.sidebar.markdown("### Navigate")

    # Previous / Next (single step)
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("◀ Previous"):
            new_rank = max(1, rank - 1)
            st.session_state.rank = new_rank
            st.rerun()
    with col2:
        if st.button("Next ▶"):
            new_rank = min(total_npis, rank + 1)
            st.session_state.rank = new_rank
            st.rerun()

    # -10 / +10 (jump by 10)
    col3, col4 = st.sidebar.columns(2)
    with col3:
        if st.button("◀◀ -10"):
            new_rank = max(1, rank - 10)
            st.session_state.rank = new_rank
            st.rerun()
    with col4:
        if st.button("+10 ▶▶"):
            new_rank = min(total_npis, rank + 10)
            st.session_state.rank = new_rank
            st.rerun()

    # First / Last
    col5, col6 = st.sidebar.columns(2)
    with col5:
        if st.button("⏮ First"):
            st.session_state.rank = 1
            st.rerun()
    with col6:
        if st.button("Last ⏭"):
            st.session_state.rank = total_npis
            st.rerun()

    # Get current NPI info
    current_idx = rank - 1
    npi = npi_list[current_idx]
    npi_name = npi_names.get(npi, "Unknown")
    npi_total = npi_totals[npi]

    # Display info cards
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Provider", npi_name[:30] + "..." if len(npi_name) > 30 else npi_name)
    with col2:
        st.metric("NPI", npi)
    with col3:
        st.metric("Total Paid (2018-2024)", f"${npi_total:,.0f}")

    # Plot
    fig = plot_npi_trends(df, npi, npi_name, npi_total, rank, total_npis)
    st.pyplot(fig)
    plt.close(fig)

    # Footer
    st.markdown("---")
    st.markdown("**Data Source:** DOGE Medicaid Provider Spending Dataset")

except FileNotFoundError as e:
    st.error(f"Data file not found: {e}")
    st.info("Please ensure 'monthly_summary_top1000.csv' and 'top1000_npi_with_names.csv' are in the same directory.")
