"""
E-Commerce Data Pipeline - Task 05: Return Analysis
Calculates return rates, identifies top refunded customers, and flags anomalies.
"""

import Task2
import Task3
from pyspark.sql import functions as F
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_returns_and_flags(enriched_df, returns_df):
    """Joins returns to enriched orders and flags refund anomalies."""
    logger.info("Joining returns to enriched orders (LEFT JOIN)...")
    
    # JUSTIFICATION: Left Join. We want to keep ALL enriched orders in our master dataset, 
    # and simply attach return data to the ones that were sent back.
    joined_df = enriched_df.join(returns_df, on="order_id", how="left")
    
    # DATA QUALITY ISSUE #7: Flagging Refund Anomalies
    logger.info("Flagging anomalous refunds (refund_amount > net_amount)...")
    joined_df = joined_df.withColumn(
        "refund_exceeds_order",
        F.when(
            (F.col("return_id").isNotNull()) & (F.col("refund_amount") > F.col("net_amount")), 
            True
        ).otherwise(False)
    )
    
    return joined_df

def calculate_return_rates(joined_df):
    """Computes return rate (returns / orders) per category and tier."""
    logger.info("Calculating return rates...")
    
    # JUSTIFICATION: Because our dataset is at the 'item' grain, an order with 5 items 
    # appears as 5 rows. We MUST use countDistinct("order_id") to count the actual 
    # number of orders, rather than counting the items.
    
    # 1. Return Rate per Category
    rate_by_category = joined_df.groupBy("category").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct(F.when(F.col("return_id").isNotNull(), F.col("order_id"))).alias("returned_orders")
    ).withColumn(
        "return_rate_pct",
        F.round((F.col("returned_orders") / F.col("total_orders")) * 100, 2)
    ).orderBy(F.desc("return_rate_pct"))
    
    # 2. Return Rate per Customer Tier
    rate_by_tier = joined_df.groupBy("customer_tier").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct(F.when(F.col("return_id").isNotNull(), F.col("order_id"))).alias("returned_orders")
    ).withColumn(
        "return_rate_pct",
        F.round((F.col("returned_orders") / F.col("total_orders")) * 100, 2)
    ).orderBy(F.desc("return_rate_pct"))
    
    return rate_by_category, rate_by_tier

def get_top_refunded_customers(joined_df):
    """Identifies the top 10 customers by total refund amount."""
    logger.info("Identifying top 10 refunded customers...")
    
    # JUSTIFICATION: Prevent duplicating refund dollars. We must isolate unique 
    # returned orders before summing the refund_amount.
    unique_returns = joined_df.filter(F.col("return_id").isNotNull()) \
        .dropDuplicates(["order_id"])
        
    top_customers = unique_returns.groupBy("customer_id") \
        .agg(F.round(F.sum("refund_amount"), 2).alias("total_refunded_amount")) \
        .orderBy(F.desc("total_refunded_amount")) \
        .limit(10)
        
    return top_customers

def main():
    logger.info("="*50)
    logger.info("STARTING TASK 05: RETURN ANALYSIS")
    logger.info("="*50)
    
    # 1. Get the cleaned returns (Task 2) and enriched orders (Task 3)
    clean_dfs = Task2.main()
    returns_clean = clean_dfs["returns"]
    enriched_orders = Task3.main()
    
    # 2. Process the Return Analysis
    final_master_df = process_returns_and_flags(enriched_orders, returns_clean)
    rate_by_category, rate_by_tier = calculate_return_rates(final_master_df)
    top_refunded_customers = get_top_refunded_customers(final_master_df)
    
    # 3. Print Results
    print("\n" + "="*50)
    print("TASK 05 VERIFICATION OUTPUTS")
    print("="*50)
    
    print("\n---> REFUND ANOMALIES FLAGGED (refund_amount > net_amount):")
    final_master_df.filter(F.col("refund_exceeds_order") == True) \
        .select("order_id", "net_amount", "refund_amount", "refund_exceeds_order") \
        .dropDuplicates(["order_id"]) \
        .show(5)
        
    print("\n---> RETURN RATE BY CATEGORY:")
    rate_by_category.show()
    
    print("\n---> RETURN RATE BY CUSTOMER TIER:")
    rate_by_tier.show()
    
    print("\n---> TOP 10 REFUNDED CUSTOMERS:")
    top_refunded_customers.show()

    return final_master_df

if __name__ == "__main__":
    master_df = main()