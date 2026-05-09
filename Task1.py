"""
E-Commerce Data Pipeline - Task 01: Data Ingestion & Schema Enforcement
Loads CSVs with explicit schemas natively, trapping casting failures in a rejected DataFrame.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.sql.functions import col
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session():
    return SparkSession.builder \
        .appName("E-Commerce Data Pipeline - Task 01") \
        .getOrCreate()

# ============================================================================
# 1. EXPLICIT SCHEMA DEFINITIONS
# Note: Dates are kept as StringType here because Task 02 specifically asks 
# for "date normalisation". If cast as DateType now, Spark will reject one of 
# the formats entirely. Numerics are explicitly cast to Double/Integer.
# ============================================================================

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DoubleType(), True), # Explicit casting
    StructField("discount_pct", DoubleType(), True),     # Explicit casting
    StructField("_corrupt_record", StringType(), True) # Magic column for rejected rows
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_tier", StringType(), True),
    StructField("email", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),    # Explicit casting
    StructField("unit_price", DoubleType(), True),   # Explicit casting
    StructField("category", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

RETURNS_SCHEMA = StructType([
    StructField("return_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("return_date", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("refund_amount", DoubleType(), True), # Explicit casting
    StructField("_corrupt_record", StringType(), True)
])


# ============================================================================
# 2. DISTRIBUTED INGESTION FUNCTION
# ============================================================================

def ingest_and_split_data(spark, file_path, schema, table_name):
    """
    Reads a CSV using an explicit schema in PERMISSIVE mode. 
    Splits the results into valid and rejected DataFrames based on casting failures.
    """
    logger.info(f"Ingesting {table_name} from {file_path}")
    
    # Read natively using Spark's CSV reader
    raw_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(schema) \
        .load(file_path)

    # Cache to avoid re-computing the read when performing counts/filters
    raw_df.cache()

    # Split the data natively
    valid_df = raw_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
    rejected_df = raw_df.filter(col("_corrupt_record").isNotNull())

    # Logging counts
    total_count = raw_df.count()
    valid_count = valid_df.count()
    rejected_count = rejected_df.count()
    
    logger.info(f"{table_name.upper()} STATS -> Total: {total_count} | Valid: {valid_count} | Rejected: {rejected_count}")

    return valid_df, rejected_df


# ============================================================================
# 3. MAIN EXECUTION
# ============================================================================

def main():
    spark = get_spark_session()
    base_path =  "/mnt/c/Users/USER/Downloads/data/data"

    # Process Orders
    orders_valid, orders_rejected = ingest_and_split_data(
        spark, f"{base_path}/orders.csv", ORDERS_SCHEMA, "Orders"
    )

    # Process Customers
    customers_valid, customers_rejected = ingest_and_split_data(
        spark, f"{base_path}/customers.csv", CUSTOMERS_SCHEMA, "Customers"
    )

    # Process Order Items
    items_valid, items_rejected = ingest_and_split_data(
        spark, f"{base_path}/order_items.csv", ORDER_ITEMS_SCHEMA, "Order Items"
    )

    # Process Returns
    returns_valid, returns_rejected = ingest_and_split_data(
        spark, f"{base_path}/returns.csv", RETURNS_SCHEMA, "Returns"
    )

    logger.info("Task 01 Complete! DataFrames are ready for Task 02.")
    
    # Return the clean dictionaries for the next Task
    return {
        "orders": orders_valid,
        "customers": customers_valid,
        "order_items": items_valid,
        "returns": returns_valid
    }

if __name__ == "__main__":
    task1_outputs = main()