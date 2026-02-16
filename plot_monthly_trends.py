#!/usr/bin/env python3
"""
Interactive monthly trend visualization for top 1000 Billing NPIs.
Shows 7 yearly trends (2018-2024) on the same plot with different colors.
Navigation: Next, Previous, Skip (jump 10), End
"""

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.widgets import Button
import numpy as np

# Load data
print("Loading data...")
df = pd.read_csv('monthly_summary_top1000.csv')

# Load NPI names
print("Loading NPI names...")
npi_names_df = pd.read_csv('top1000_npi_with_names.csv')
npi_names = dict(zip(npi_names_df['billing_npi'], npi_names_df['name']))

# Parse month into year and month number
df['year'] = df['month'].str[:4].astype(int)
df['month_num'] = df['month'].str[5:7].astype(int)

# Get list of unique billing NPIs sorted by total paid
npi_totals = df.groupby('billing_npi')['total_paid'].sum().sort_values(ascending=False)
npi_list = npi_totals.index.tolist()

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

class TrendViewer:
    def __init__(self, df, npi_list, npi_totals, npi_names):
        self.df = df
        self.npi_list = npi_list
        self.npi_totals = npi_totals
        self.npi_names = npi_names
        self.current_idx = 0
        self.total_npis = len(npi_list)

        # Create figure and axes
        self.fig, self.ax = plt.subplots(figsize=(12, 7))
        plt.subplots_adjust(bottom=0.2)

        # Create buttons
        ax_prev = plt.axes([0.2, 0.05, 0.12, 0.05])
        ax_next = plt.axes([0.35, 0.05, 0.12, 0.05])
        ax_skip = plt.axes([0.50, 0.05, 0.12, 0.05])
        ax_end = plt.axes([0.65, 0.05, 0.12, 0.05])

        self.btn_prev = Button(ax_prev, 'Previous')
        self.btn_next = Button(ax_next, 'Next')
        self.btn_skip = Button(ax_skip, 'Skip 10')
        self.btn_end = Button(ax_end, 'End')

        self.btn_prev.on_clicked(self.prev_npi)
        self.btn_next.on_clicked(self.next_npi)
        self.btn_skip.on_clicked(self.skip_npi)
        self.btn_end.on_clicked(self.end_viewer)

        # Plot first NPI
        self.plot_current()

    def plot_current(self):
        self.ax.clear()

        npi = self.npi_list[self.current_idx]
        npi_data = self.df[self.df['billing_npi'] == npi]
        total_paid = self.npi_totals[npi]
        npi_name = self.npi_names.get(npi, "Unknown")

        # Plot each year as a separate line
        for year in sorted(year_colors.keys()):
            year_data = npi_data[npi_data['year'] == year].sort_values('month_num')
            if not year_data.empty:
                self.ax.plot(
                    year_data['month_num'],
                    year_data['total_paid'] / 1e6,  # Convert to millions
                    marker=year_markers[year],
                    color=year_colors[year],
                    label=str(year),
                    linewidth=2,
                    markersize=8
                )

        # Formatting
        self.ax.set_xlabel('Month', fontsize=12)
        self.ax.set_ylabel('Total Paid (Millions $)', fontsize=12)
        self.ax.set_title(
            f'{npi_name}\n'
            f'NPI: {npi} | Rank: {self.current_idx + 1}/{self.total_npis} | '
            f'Total Paid (2018-2024): ${total_paid:,.0f}',
            fontsize=11
        )
        self.ax.set_xticks(range(1, 13))
        self.ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
        self.ax.legend(title='Year', loc='upper right')
        self.ax.grid(True, alpha=0.3)

        # Set y-axis to start from 0
        self.ax.set_ylim(bottom=0)

        self.fig.canvas.draw_idle()

    def next_npi(self, event):
        if self.current_idx < self.total_npis - 1:
            self.current_idx += 1
            self.plot_current()

    def prev_npi(self, event):
        if self.current_idx > 0:
            self.current_idx -= 1
            self.plot_current()

    def skip_npi(self, event):
        self.current_idx = min(self.current_idx + 10, self.total_npis - 1)
        self.plot_current()

    def end_viewer(self, event):
        plt.close(self.fig)

# Create and show viewer
print(f"Loaded {len(npi_list)} NPIs")
print("Starting interactive viewer...")
print("\nControls:")
print("  - Previous: Go to previous NPI")
print("  - Next: Go to next NPI")
print("  - Skip 10: Jump forward 10 NPIs")
print("  - End: Close the viewer")

viewer = TrendViewer(df, npi_list, npi_totals, npi_names)
plt.show()
