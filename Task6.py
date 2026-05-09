"""
E-Commerce Data Pipeline - Task 06: Output & Partitioning
Writes master dataset to Parquet (partitioned) and summary tables to CSV.
Idempotent writes using mode('overwrite').
"""

import Task2
import Task3
import Task4
import Task5
from pyspark.sql import functions as F
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def write_master_to_parquet(df, base_path):
    """Writes the master DataFrame to Parquet, partitioned by year and month."""
    output_path = f"{base_path}/output/master_enriched_data"
    logger.info("Preparing master data for Parquet partitioning...")
    
    # Extract the year and month from order_date for partitioning
    df_partitioned = df.withColumn("order_year", F.year("order_date")) \
                       .withColumn("order_month", F.month("order_date"))
    
    logger.info(f"Writing partitioned Parquet files to: {output_path}")
    
    # JUSTIFICATION: mode("overwrite") guarantees idempotency. 
    # Running this script 100 times will result in the exact same output state.
    df_partitioned.write \
        .mode("overwrite") \
        .partitionBy("order_year", "order_month") \
        .parquet(output_path)

def write_summary_to_csv(df, folder_name, base_path):
    """Writes an aggregated summary DataFrame to a single CSV file."""
    output_path = f"{base_path}/output/summaries/{folder_name}"
    logger.info(f"Writing summary CSV to: {output_path}")
    
    # JUSTIFICATION: Using coalesce(1) because summary tables are small. 
    # This prevents Spark from writing 200 tiny CSV files for a 10-row table.
    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

def main():
    logger.info("="*50)
    logger.info("STARTING TASK 06: FINAL OUTPUT & PARTITIONING")
    logger.info("="*50)
    
    base_path = "/mnt/c/Users/USER/Downloads/data"
    
    # 1. Grab base DataFrames
    clean_dfs = Task2.main()
    returns_clean = clean_dfs["returns"]
    enriched_df = Task3.main()
    
    # 2. Generate Master DataFrame (Task 5 logic)
    master_df = Task5.process_returns_and_flags(enriched_df, returns_clean)
    
    # 3. Generate Summary Tables (Task 4 & 5 logic)
    logger.info("Generating summary tables...")
    category_shares = Task4.get_category_revenue_share(master_df)
    rate_by_category, rate_by_tier = Task5.calculate_return_rates(master_df)
    top_refunded = Task5.get_top_refunded_customers(master_df)
    
    # 4. Write Master Data to Parquet (Partitioned)
    write_master_to_parquet(master_df, base_path)
    
    # 5. Write Summary Tables to CSV
    write_summary_to_csv(category_shares, "category_revenue_shares", base_path)
    write_summary_to_csv(rate_by_category, "return_rates_by_category", base_path)
    write_summary_to_csv(rate_by_tier, "return_rates_by_tier", base_path)
    write_summary_to_csv(top_refunded, "top_10_refunded_customers", base_path)
    
    logger.info("\n" + "="*50)
    logger.info("🚀 PIPELINE COMPLETE! ALL DATA WRITTEN SUCCESSFULLY.")
    logger.info("="*50)

if __name__ == "__main__":
    main()