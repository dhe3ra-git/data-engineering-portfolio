import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark import StorageLevel

# ─────────────────────────────────────────
# SETUP LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
BRONZE_PATH = "s3a://sales-data-lake/bronze"
SILVER_PATH = "s3a://sales-data-lake-processed/silver"

# For LocalStack
S3_ENDPOINT = "http://localhost:4566"

# ─────────────────────────────────────────
# CREATE SPARK SESSION
# ─────────────────────────────────────────
def create_spark_session():

    spark = SparkSession.builder \
        .appName("Bronze_to_Silver_ETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.hadoop.fs.s3.impl.disable.cache", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "50") \
        .config("spark.hadoop.fs.s3a.threads.max", "50") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A") \
        .config("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A") \
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ─────────────────────────────────────────
# EXTRACT — Read all CSV files from Bronze
# ─────────────────────────────────────────
def extract(spark):
    logger.info("=" * 50)
    logger.info("EXTRACT — Reading from S3 Bronze Layer")
    logger.info("=" * 50)

    # Read all tables
    sales_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3a://sales-data-lake/bronze/sales/")
    logger.info(f"Sales rows: {sales_df.count()}")

    customers_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3a://sales-data-lake/bronze/customers/")
    logger.info(f"Customers rows: {customers_df.count()}")

    products_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3a://sales-data-lake/bronze/products/")
    logger.info(f"Products rows: {products_df.count()}")

    stores_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3a://sales-data-lake/bronze/stores/")
    logger.info(f"Stores rows: {stores_df.count()}")

    regions_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3a://sales-data-lake/bronze/regions/")
    logger.info(f"Regions rows: {regions_df.count()}")

    promotions_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3a://sales-data-lake/bronze/promotions/")
    logger.info(f"Promotions rows: {promotions_df.count()}")

    returns_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3a://sales-data-lake/bronze/returns/")
    logger.info(f"Returns rows: {returns_df.count()}")

    date_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"s3a://sales-data-lake/bronze/date_dim/")
    logger.info(f"Date rows: {date_df.count()}")

    return sales_df, customers_df, products_df, \
           stores_df, regions_df, promotions_df, \
           returns_df, date_df

# ─────────────────────────────────────────
# DATA QUALITY CHECKS
# ─────────────────────────────────────────
def check_data_quality(df, table_name, critical_cols):
    logger.info(f"Running data quality checks on {table_name}...")
    errors = []

    # Check nulls
    for col in critical_cols:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            errors.append(f"{col} has {null_count} nulls!")

    # Check duplicates
    id_col = critical_cols[0]
    dup_count = df.count() - df.dropDuplicates([id_col]).count()
    if dup_count > 0:
        errors.append(f"{table_name} has {dup_count} duplicates!")

    if errors:
        for e in errors:
            logger.warning(f"Data Quality Warning: {e}")
    else:
        logger.info(f"{table_name} passed all quality checks! ✅")

    return len(errors) == 0

# ─────────────────────────────────────────
# TRANSFORM — Clean each table
# ─────────────────────────────────────────
def transform(spark, sales_df, customers_df, products_df,
              stores_df, regions_df, promotions_df,
              returns_df, date_df):

    logger.info("=" * 50)
    logger.info("TRANSFORM — Cleaning and transforming data")
    logger.info("=" * 50)

    # ── Clean Sales ──
    logger.info("Cleaning sales_transactions...")
    check_data_quality(sales_df, "sales",
        ["sale_id", "customer_id", "product_id", "final_amount"])

    clean_sales = sales_df \
        .dropDuplicates(["sale_id"]) \
        .dropna(subset=["sale_id", "customer_id",
                        "product_id", "final_amount"]) \
        .filter(F.col("final_amount") > 0) \
        .filter(F.col("sale_status") == "Completed") \
        .withColumn("sale_date", F.to_date("sale_date")) \
        .withColumn("year", F.year("sale_date")) \
        .withColumn("month", F.month("sale_date")) \
        .withColumn("day", F.dayofmonth("sale_date")) \
        .withColumn("quarter",
            F.when(F.col("month").between(1,3), "Q1")
             .when(F.col("month").between(4,6), "Q2")
             .when(F.col("month").between(7,9), "Q3")
             .otherwise("Q4")) \
        .withColumn("profit",
            F.col("final_amount") - F.col("tax_amount") -
            F.col("discount_amount"))
    logger.info(f"Clean sales rows: {clean_sales.count()}")

    # ── Clean Customers ──
    logger.info("Cleaning customers...")
    check_data_quality(customers_df, "customers",
        ["customer_id", "first_name", "email"])

    clean_customers = customers_df \
        .dropDuplicates(["customer_id"]) \
        .dropna(subset=["customer_id", "email"]) \
        .filter(F.col("is_active") == "Y") \
        .withColumn("full_name",
            F.concat(F.col("first_name"),
                     F.lit(" "),
                     F.col("last_name"))) \
        .withColumn("registration_date",
            F.to_date("registration_date")) \
        .select("customer_id", "full_name", "email",
                "phone", "gender", "age", "city",
                "state", "region_id", "segment",
                "registration_date")
    logger.info(f"Clean customers rows: {clean_customers.count()}")

    # ── Clean Products ──
    logger.info("Cleaning products...")
    check_data_quality(products_df, "products",
        ["product_id", "product_name", "unit_price"])

    clean_products = products_df \
        .dropDuplicates(["product_id"]) \
        .dropna(subset=["product_id", "unit_price"]) \
        .filter(F.col("unit_price") > 0) \
        .withColumn("margin_pct",
            F.round(
                (F.col("unit_price") - F.col("cost_price")) /
                F.col("unit_price") * 100, 2))
    logger.info(f"Clean products rows: {clean_products.count()}")

    # ── Clean Stores ──
    logger.info("Cleaning stores...")
    clean_stores = stores_df \
        .dropDuplicates(["store_id"]) \
        .dropna(subset=["store_id", "store_name"])
    logger.info(f"Clean stores rows: {clean_stores.count()}")

    # ── Clean Returns ──
    logger.info("Cleaning returns...")
    clean_returns = returns_df \
        .dropDuplicates(["return_id"]) \
        .dropna(subset=["return_id", "sale_id"]) \
        .filter(F.col("return_status") == "Approved") \
        .withColumn("return_date", F.to_date("return_date"))
    logger.info(f"Clean returns rows: {clean_returns.count()}")

    # ─────────────────────────────────────
    # JOIN all tables — Enriched Sales
    # ─────────────────────────────────────
    logger.info("Joining all tables...")

    # Cache sales before multiple joins
    clean_sales.persist(StorageLevel.MEMORY_AND_DISK)

    enriched_sales = clean_sales \
        .join(broadcast(clean_customers),
              on="customer_id", how="left") \
        .join(broadcast(clean_products),
              on="product_id", how="left") \
        .join(broadcast(clean_stores),
              on="store_id", how="left") \
        .join(broadcast(regions_df),
              on="region_id", how="left") \
        .join(broadcast(promotions_df),
              on="promo_id", how="left") \
        .select(
            # Sale info
            "sale_id", "sale_date", "year",
            "month", "day", "quarter",
            # Customer info
            "customer_id", "full_name",
            "segment", "city", "state",
            # Product info
            "product_id", "product_name",
            "category", "sub_category", "brand",
            # Store info
            "store_id", "store_name", "store_type",
            # Region info
            "region_id", "region_name", "zone",
            # Financial info
            "quantity", "unit_price",
            "discount_amount", "tax_amount",
            "final_amount", "profit",
            # Promo info
            "promo_name", "discount_percent",
            # Payment info
            "payment_method", "sale_status"
        )

    logger.info(f"Enriched sales rows: {enriched_sales.count()}")
    clean_sales.unpersist()

    return enriched_sales, clean_customers, \
           clean_products, clean_stores, \
           clean_returns, regions_df, \
           promotions_df, date_df

# ─────────────────────────────────────────
# LOAD — Write to S3 Silver Layer
# ─────────────────────────────────────────
def load(enriched_sales, clean_customers,
         clean_products, clean_stores,
         clean_returns, regions_df,
         promotions_df, date_df):

    logger.info("=" * 50)
    logger.info("LOAD — Writing to S3 Silver Layer")
    logger.info("=" * 50)

    # Write enriched sales — partitioned!
    logger.info("Writing enriched sales...")
    enriched_sales.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .partitionBy("year", "month") \
        .parquet(f"s3a://sales-data-lake-processed/silver/sales/")
    logger.info("Enriched sales written! ✅")

    # Write dimension tables
    logger.info("Writing dimension tables...")

    clean_customers.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3a://sales-data-lake-processed/silver/customers/")

    clean_products.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3a://sales-data-lake-processed/silver/products/")

    clean_stores.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3a://sales-data-lake-processed/silver/stores/")

    clean_returns.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3a://sales-data-lake-processed/silver/returns/")

    regions_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3a://sales-data-lake-processed/silver/regions/")

    promotions_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3a://sales-data-lake-processed/silver/promotions/")

    date_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(f"s3a://sales-data-lake-processed/silver/date_dim/")

    logger.info("All dimension tables written! ✅")

# ─────────────────────────────────────────
# RUN PIPELINE
# ─────────────────────────────────────────
def run_pipeline():
    from datetime import datetime
    start_time = datetime.now()

    logger.info("=" * 50)
    logger.info("BRONZE TO SILVER PIPELINE STARTED")
    logger.info("=" * 50)

    try:
        # Create SparkSession
        spark = create_spark_session()

        # Extract
        sales_df, customers_df, products_df, \
        stores_df, regions_df, promotions_df, \
        returns_df, date_df = extract(spark)

        # Transform
        enriched_sales, clean_customers, \
        clean_products, clean_stores, \
        clean_returns, regions_df, \
        promotions_df, date_df = transform(
            spark, sales_df, customers_df,
            products_df, stores_df, regions_df,
            promotions_df, returns_df, date_df)

        # Load
        load(enriched_sales, clean_customers,
             clean_products, clean_stores,
             clean_returns, regions_df,
             promotions_df, date_df)

        end_time = datetime.now()
        duration = (end_time - start_time).seconds

        logger.info("=" * 50)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY! 🎉")
        logger.info(f"Duration: {duration} seconds")
        logger.info("=" * 50)

        spark.stop()

    except Exception as e:
        logger.error(f"PIPELINE FAILED: {str(e)}")
        raise e

if __name__ == "__main__":
    run_pipeline()