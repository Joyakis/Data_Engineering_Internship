"""
E-Commerce Data Pipeline - Tasks 01 & 02
Data Ingestion, Schema Enforcement, and Data Cleaning
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

# ============================================================================
# INITIALIZE LOGGER & SPARK
# ============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session():
    return SparkSession.builder \
        .appName("E-Commerce Data Pipeline") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

# ============================================================================
# TASK 01: EXPLICIT SCHEMA DEFINITIONS
# ============================================================================
ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DoubleType(), True), 
    StructField("discount", DoubleType(), True),     
    StructField("_corrupt_record", StringType(), True) 
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
    StructField("quantity", IntegerType(), True),    
    StructField("unit_price", DoubleType(), True),   
    StructField("category", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

RETURNS_SCHEMA = StructType([
    StructField("return_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("return_date", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("refund_amount", DoubleType(), True), 
    StructField("_corrupt_record", StringType(), True)
])

def ingest_and_split_data(spark, file_path, schema, table_name):
    logger.info(f"Ingesting {table_name}...")
    raw_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .schema(schema) \
        .load(file_path)

    raw_df.cache()
    valid_df = raw_df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
    
    logger.info(f"{table_name.upper()} INGESTED -> Valid Rows: {valid_df.count()}")
    return valid_df

# ============================================================================
# TASK 02: CLEANING FUNCTIONS (UPDATED WITH REGEX DATE PARSING)
# ============================================================================
def clean_orders(df):
    logger.info("Cleaning Orders...")
    
    # 1. Deduplicate
    df_clean = df.dropDuplicates()
    
    # 2. Date Normalisation using Regex (Bypasses strict ANSI exceptions)
    df_clean = df_clean.withColumn(
        "order_date",
        F.when(F.col("order_date").rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(F.col("order_date"), "yyyy-MM-dd"))
         .when(F.col("order_date").rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(F.col("order_date"), "dd/MM/yyyy"))
         .otherwise(F.lit(None))
    )
    
    # 3. NULL handling
    df_clean = df_clean.dropna(subset=["order_id", "customer_id"])
    
    # 4. Flag negative amounts (Do not drop)
    df_clean = df_clean.withColumn(
        "is_negative_amount",
        F.when(F.col("total_amount") < 0, True).otherwise(False)
    )
    
    return df_clean

def clean_customers(df):
    logger.info("Cleaning Customers...")
    df_clean = df.dropDuplicates()
    
    # Date Normalisation using Regex
    df_clean = df_clean.withColumn(
        "signup_date",
        F.when(F.col("signup_date").rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(F.col("signup_date"), "yyyy-MM-dd"))
         .when(F.col("signup_date").rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(F.col("signup_date"), "dd/MM/yyyy"))
         .otherwise(F.lit(None))
    )
    
    # 5. Casing normalisation
    df_clean = df_clean.withColumn("customer_tier", F.lower(F.col("customer_tier")))
    return df_clean

def clean_order_items(df):
    logger.info("Cleaning Order Items...")
    return df.dropDuplicates()

def clean_returns(df):
    logger.info("Cleaning Returns...")
    df_clean = df.dropDuplicates()
    
    # Date Normalisation using Regex
    df_clean = df_clean.withColumn(
        "return_date",
        F.when(F.col("return_date").rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(F.col("return_date"), "yyyy-MM-dd"))
         .when(F.col("return_date").rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(F.col("return_date"), "dd/MM/yyyy"))
         .otherwise(F.lit(None))
    )
    return df_clean
# ============================================================================
# MAIN PIPELINE EXECUTION
# ============================================================================
def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR") # Hides the messy Spark warnings
    
    base_path = "/mnt/c/Users/USER/Downloads/data/data" # Your WSL path
    
    # --- TASK 01: INGESTION ---
    logger.info("--- STARTING TASK 01: INGESTION ---")
    orders_raw = ingest_and_split_data(spark, f"{base_path}/orders.csv", ORDERS_SCHEMA, "Orders")
    customers_raw = ingest_and_split_data(spark, f"{base_path}/customers.csv", CUSTOMERS_SCHEMA, "Customers")
    items_raw = ingest_and_split_data(spark, f"{base_path}/order_items.csv", ORDER_ITEMS_SCHEMA, "Order Items")
    returns_raw = ingest_and_split_data(spark, f"{base_path}/returns.csv", RETURNS_SCHEMA, "Returns")
    
    # --- TASK 02: CLEANING ---
    logger.info("--- STARTING TASK 02: CLEANING ---")
    orders_clean = clean_orders(orders_raw)
    customers_clean = clean_customers(customers_raw)
    items_clean = clean_order_items(items_raw)
    returns_clean = clean_returns(returns_raw)
    
    # FORCE PRINT OUTPUTS TO TERMINAL
    print("\n" + "="*50)
    print( "SUCCESS!")
    print("="*50)
    
    print("\n---> SAMPLE OF CLEANED ORDERS:")
    orders_clean.show(5, truncate=False)
    
    print("\n---> SAMPLE OF CLEANED CUSTOMERS:")
    customers_clean.show(5, truncate=False)

    return {
        "orders": orders_clean,
        "customers": customers_clean,
        "order_items": items_clean,
        "returns": returns_clean
    }

# THIS IS THE TRIGGER THAT RUNS THE WHOLE SCRIPT
if __name__ == "__main__":
    clean_dataframes = main()