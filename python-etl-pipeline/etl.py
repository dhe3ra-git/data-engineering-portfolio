import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime

# ─────────────────────────────────────────
# SETUP LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# STEP 1 — EXTRACT
# ─────────────────────────────────────────
def extract(filepath):
    logger.info(f"Extracting data from {filepath}")
    try:
        df = pd.read_csv(filepath)
        logger.info(f"Extracted {len(df)} rows successfully")
        return df
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        raise

# ─────────────────────────────────────────
# STEP 2 — TRANSFORM
# ─────────────────────────────────────────
def transform(df):
    logger.info("Starting transformation...")
    initial_count = len(df)

    # 1. Remove duplicates
    df = df.drop_duplicates()
    logger.info(f"Removed {initial_count - len(df)} duplicate rows")

    # 2. Drop rows with missing critical columns
    df = df.dropna(subset=['sale_id', 'region', 'amount', 'sale_date'])
    logger.info(f"Rows after dropping nulls: {len(df)}")

    # 3. Fix data types
    df['sale_id'] = df['sale_id'].astype(int)
    df['amount']  = pd.to_numeric(df['amount'], errors='coerce')
    df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')

    # 4. Drop rows where amount became null after conversion
    df = df.dropna(subset=['amount', 'sale_date'])

    # 5. Remove invalid amounts
    df = df[df['amount'] > 0]
    logger.info(f"Rows after removing invalid amounts: {len(df)}")

    # 6. Add new columns
    df['commission']    = df['amount'] * 5 / 100
    df['tax']           = df['amount'] * 18 / 100
    df['final_amount']  = df['amount'] + df['tax']
    df['year']          = df['sale_date'].dt.year
    df['month']         = df['sale_date'].dt.month
    df['category']      = df['amount'].apply(
        lambda x: 'High' if x > 50000 else ('Medium' if x >= 20000 else 'Low')
    )

    # 7. Data quality check
    assert df['amount'].isnull().sum() == 0,   "Nulls found in amount!"
    assert df['region'].isnull().sum() == 0,   "Nulls found in region!"
    assert (df['amount'] > 0).all(),           "Negative amounts found!"

    logger.info(f"Transformation complete! Final rows: {len(df)}")
    return df

# ─────────────────────────────────────────
# STEP 3 — LOAD
# ─────────────────────────────────────────
def load(df, output_path):
    logger.info(f"Loading data to {output_path}")

    # Save cleaned CSV
    df.to_csv(f"{output_path}/clean_sales.csv", index=False)

    # Save as Parquet — used in AWS!
    df.to_parquet(f"{output_path}/clean_sales.parquet", index=False)

    # Save summary report
    summary = df.groupby('region').agg(
        total_sales=('amount', 'sum'),
        avg_sales=('amount', 'mean'),
        total_commission=('commission', 'sum'),
        count=('sale_id', 'count')
    ).reset_index()

    summary.to_csv(f"{output_path}/sales_summary.csv", index=False)
    logger.info(f"Data loaded successfully! {len(df)} rows written")

# ─────────────────────────────────────────
# STEP 4 — RUN PIPELINE
# ─────────────────────────────────────────
def run_pipeline(input_file, output_path):
    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info("ETL PIPELINE STARTED")
    logger.info("=" * 50)

    try:
        # Extract
        raw_df = extract(input_file)

        # Transform
        clean_df = transform(raw_df)

        # Load
        os.makedirs(output_path, exist_ok=True)
        load(clean_df, output_path)

        end_time = datetime.now()
        duration = (end_time - start_time).seconds

        logger.info("=" * 50)
        logger.info(f"ETL PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info(f"Duration   : {duration} seconds")
        logger.info(f"Input rows : {len(raw_df)}")
        logger.info(f"Output rows: {len(clean_df)}")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"ETL PIPELINE FAILED: {e}")
        raise

# ─────────────────────────────────────────
# RUN IT!
# ─────────────────────────────────────────
if __name__ == '__main__':
    run_pipeline('raw_sales.csv', 'output')