# Python ETL Pipeline
## Data Engineering Portfolio

## What this project does
A Python ETL pipeline that:
- Extracts raw sales data from CSV
- Transforms and cleans the data
- Loads clean data to CSV and Parquet format

## Tech Stack
- Python 3.13
- Pandas
- NumPy
- PyArrow

## Pipeline Steps
1. **Extract** → Read raw_sales.csv
2. **Transform** →
   - Remove duplicates
   - Handle missing values
   - Fix data types
   - Add commission, tax, category columns
3. **Load** →
   - clean_sales.csv
   - clean_sales.parquet
   - sales_summary.csv

## How to Run
```bash
pip install -r requirements.txt
python3 etl.py
```

## Input Data
Raw messy CSV with 8 rows containing:
- Missing values
- Duplicate records
- Invalid amounts
- Wrong data types

## Output
- 4 clean rows after transformation
- Summary report by region