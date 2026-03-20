# Python ETL Pipeline

## What this project does
A Python ETL pipeline that:
- Extracts raw sales data from CSV
- Transforms and cleans the data
- Loads clean data to CSV and Parquet format

## Tech Stack
- Python 3.14
- Pandas
- NumPy
- PyArrow

## Pipeline Steps
1. Extract - Read raw_sales.csv
2. Transform - Remove duplicates, handle nulls, fix data types
3. Load - clean_sales.csv, clean_sales.parquet, sales_summary.csv

## How to Run
pip install -r requirements.txt
python3 etl.py

## Input vs Output
- Input: 8 raw messy rows
- Output: 4 clean validated rows
