# Medicaid Provider Spending Trends

Interactive visualization of Medicaid provider spending data (2018-2024) for the top 1000 billing NPIs.

## Live Demo

**Streamlit App:** [Coming soon - add your Streamlit Cloud URL here]

**Google Colab:** [Coming soon - add your Colab link here]

## Features

- Interactive monthly trend charts for top 1000 billing providers
- 7-year comparison (2018-2024) with color-coded lines
- Search by NPI number
- Navigation by spending rank
- Total paid amounts and provider names

## Quick Start

### Option 1: Streamlit (Web App)

```bash
pip install -r requirements.txt
streamlit run streamlit_monthly_trends.py
```

### Option 2: Google Colab

1. Open `plot_monthly_trends_colab.ipynb` in Google Colab
2. Upload the CSV data files when prompted
3. Run all cells

### Option 3: Local Python Script

```bash
pip install pandas matplotlib
python plot_monthly_trends.py
```

## Files

| File | Description |
|------|-------------|
| `streamlit_monthly_trends.py` | Streamlit web application |
| `plot_monthly_trends_colab.ipynb` | Google Colab notebook |
| `plot_monthly_trends.py` | Original matplotlib script |
| `monthly_summary_top1000.csv` | Monthly spending data |
| `top1000_npi_with_names.csv` | NPI to provider name mapping |

## Data Source

DOGE Medicaid Provider Spending Dataset

## Deployment

To deploy on Streamlit Cloud:

1. Fork this repository
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub account
4. Select this repository and `streamlit_monthly_trends.py`
5. Click Deploy
