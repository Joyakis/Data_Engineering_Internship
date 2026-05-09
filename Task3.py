"""
E-Commerce Data Pipeline - Task 03: Joins & Enrichment
Isolates orphaned records, joins tables, and calculates net_amount.
"""

import Task2
from pyspark.sql import functions as F
import logging
from pyspark.sql.functions import broadcast

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def isolate_orphaned_items(items_df, orders_df, output_path):
    """Finds order items that don't belong to any valid order and saves them."""
    logger.info("Isolating orphaned order_items using LEFT ANTI join...")
    
    # JUSTIFICATION: A Left Anti Join returns all rows from the left table (items) 
    # that do NOT have a match in the right table (orders). It is the most 
    # efficient way to find orphaned/ghost records.
    orphaned_items = items_df.join(orders_df, on="order_id", how="left_anti")
    
    orphaned_count = orphaned_items.count()
    logger.info(f"Found {orphaned_count} orphaned order_items. Writing to file...")
    
    # Write to a separate output file (using CSV for easy review)
    # Using coalesce(1) to output a single file instead of multiple partitions for this small dataset
    if orphaned_count > 0:
        orphaned_items.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        logger.info(f"Orphaned items saved to: {output_path}")
        
    return orphaned_items


def enrich_pipeline(orders_df, customers_df, items_df):
    """Joins the core tables and derives the net_amount."""
    logger.info("Enriching data through joins...")
    
    # 1. Join Orders to Order Items
    # JUSTIFICATION: Inner Join. A valid transaction requires both an order header 
    # and line items. If an order has no items, or an item has no order, drop it from the main flow.
    enriched_df = orders_df.join(items_df, on="order_id", how="inner")
    
    # 2. Join Enriched Orders to Customers
    # JUSTIFICATION (B2 BONUS): We wrap customers_df in broadcast() because it is 
    # a smaller dimension table. Broadcasting it to all worker nodes eliminates 
    # expensive network shuffles when joining against the much larger fact table.
    enriched_df = enriched_df.join(broadcast(customers_df), on="customer_id", how="left")
    
    # 3. Derive net_amount = total_amount * (1 - discount_pct / 100)
    logger.info("Calculating derived column: net_amount...")
    
    # Note: Using coalesce on discount to ensure NULL discounts are treated as 0%
    enriched_df = enriched_df.withColumn(
        "net_amount",
        F.round(
            F.col("total_amount") * (1 - (F.coalesce(F.col("discount"), F.lit(0.0)) / 100)), 
            2
        )
    )
    
    return enriched_df


def main():
    logger.info("="*50)
    logger.info("STARTING TASK 03: JOINS & ENRICHMENT")
    logger.info("="*50)
    
    # 1. Import cleaned data from Task 02 silently
    clean_dfs = Task2.main()
    orders_clean = clean_dfs["orders"]
    customers_clean = clean_dfs["customers"]
    items_clean = clean_order_items = clean_dfs["order_items"]
    
    base_path = "/mnt/c/Users/USER/Downloads/data"
    orphaned_output_path = f"{base_path}/output/orphaned_items"
    
    # 2. Isolate and save orphaned items
    orphaned_items = isolate_orphaned_items(items_clean, orders_clean, orphaned_output_path)
    
    # 3. Join the data and calculate net_amount
    final_enriched_df = enrich_pipeline(orders_clean, customers_clean, items_clean)
    
    # 4. Show the results
    logger.info("\n---> SAMPLE OF ENRICHED DATA (Notice the new net_amount column):")
    
    # Selecting specific columns so the terminal output isn't a messy wrap-around text
    final_enriched_df.select(
        "order_id", "customer_id", "item_id", "total_amount", "discount", "net_amount", "customer_tier"
    ).show(5)
    
    return final_enriched_df

if __name__ == "__main__":
    enriched_data = main()