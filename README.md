# US Flight Delays ETL Pipeline

**Simple ETL process for US flight delay data + basic arrival delay inspection**

## Overview

This repository contains a lightweight **ETL pipeline** that:

- Extracts raw US flight performance data (typically monthly/annual CSV files from BTS)
- Performs necessary cleaning and transformation steps
- Loads the processed data into a usable format (e.g. cleaned CSV, Parquet, or in-memory for quick inspection)

The notebook also includes **minimal EDA** — mainly displaying the resulting dataframe and one or two simple visualizations to understand the distribution and patterns of **arrival delays**.

No advanced modeling, feature engineering, or extensive statistical analysis is performed.

## Repository Structure

```
US_Flight_Delays/
├── src/                    # (optional) helper functions, ETL logic, constants, etc.
├── EDA.ipynb               # Main notebook: ETL steps + basic arrival delay inspection & viz
├── requirements.txt        # Python dependencies
├── .gitignore
└── README.md
```

**Note:** Raw data files are **not** included (they are large). Download them yourself from the sources below.

## Data Source

Data comes from the **Bureau of Transportation Statistics (BTS)** — U.S. Department of Transportation:

- Main portal: https://www.transtats.bts.gov/ONTIME/
- Download page: https://www.transtats.bts.gov/DL_SelectFields.aspx
- Popular mirrored version on Kaggle: https://www.kaggle.com/datasets/usdot/flight-delays

Common relevant columns:

- ORIGIN, DEST, DEP_DELAY, ARR_DELAY
- CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY
- CANCELLED, DIVERTED, DISTANCE, etc.

## Getting Started

### 1. Prerequisites

- Python 3.8+
- Jupyter Notebook / JupyterLab (or VS Code with Jupyter extension)

### 2. Install dependencies

```bash
# Create & activate virtual environment (recommended)
python -m venv venv
source venv/bin/activate      # Linux/macOS
# venv\Scripts\activate       # Windows


pip install -r requirements.txt


# Typical requirements.txt:
pandas>=1.5.0
numpy>=1.23.0
matplotlib>=3.6.0
seaborn>=0.12.0             # optional — for nicer plots
pyarrow                     # optional — if saving to Parquet
jupyter

```

### 3. Prepare data

Download one or more monthly/yearly CSV files from BTS or Kaggle
Place them in a folder (e.g. data/raw/)
Update the file path(s) in EDA.ipynb

### 4. Run the notebook

```bash
jupyter notebook EDA.ipynb
```

### The notebook walks through:

Reading & combining raw files (if multiple)
Handling missing values, data types, filtering cancelled/diverted flights, etc.
Creating any derived columns if needed
Saving cleaned data
Quick .head(), .info(), .describe()
One or two plots focused on ARR_DELAY (histogram, boxplot by carrier/airport/month, etc.)


### Future Ideas (optional)

Move ETL logic into modular .py scripts in src/
Add incremental loading / date partitioning
Save output to database (SQLite/PostgreSQL)
Compare delay distributions across years

