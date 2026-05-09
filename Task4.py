"""
E-Commerce Data Pipeline - Task 04: Aggregations & Window Functions
Demonstrates ranking, rolling windows, and percentage shares using PySpark Window functions.
"""

import Task3
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_customer_ranks(enriched_df):
    """6. Customers ranked by lifetime net spend within each country."""
    logger.info("Calculating Customer Ranks per Country...")
    
    # JUSTIFICATION: Because enriched_df is joined to order_items, order-level columns 
    # (like net_amount) are duplicated for every item in the order. 
    # We must extract unique orders first so we don't double-count revenue!
    unique_orders = enriched_df.select(
        "order_id", "customer_id", "country", "net_amount"
    ).dropDuplicates(["order_id"])
    
    # First, get total lifetime spend per customer
    customer_spend = unique_orders.groupBy("customer_id", "country") \
        .agg(F.sum("net_amount").alias("lifetime_spend"))
        
    # Now, use a Window function to rank them WITHIN their country
    window_country = Window.partitionBy("country").orderBy(F.desc("lifetime_spend"))
    
    ranked_customers = customer_spend.withColumn(
        "rank_in_country", 
        F.dense_rank().over(window_country)
    )
    
    return ranked_customers


def get_rolling_order_count(enriched_df):
    """7. A 7-day rolling order count per customer (based on order_date)."""
    logger.info("Calculating 7-day rolling order counts...")
    
    unique_orders = enriched_df.select(
        "order_id", "customer_id", "order_date"
    ).dropDuplicates(["order_id"])
    
    # JUSTIFICATION: To use a time-based rolling window (rangeBetween) in PySpark, 
    # we need to convert the date into the number of days since the Unix Epoch.
    orders_with_days = unique_orders.withColumn(
        "order_day_int", 
        (F.col("order_date").cast("timestamp").cast("long") / 86400).cast("long")
    )
    
    # Define a 7-day rolling window (-6 days to current day = 7 days total)
    rolling_window = Window.partitionBy("customer_id") \
        .orderBy("order_day_int") \
        .rangeBetween(-6, Window.currentRow)
        
    rolling_counts = orders_with_days.withColumn(
        "7_day_rolling_count", 
        F.count("order_id").over(rolling_window)
    ).drop("order_day_int") # Clean up our helper column
    
    return rolling_counts


def get_category_revenue_share(enriched_df):
    """8. Each product category's share of total revenue per calendar month."""
    logger.info("Calculating category revenue share per month...")
    
    # Extract the Year-Month from the order date
    df_with_month = enriched_df.withColumn(
        "order_month", 
        F.date_format("order_date", "yyyy-MM")
    )
    
    # JUSTIFICATION: Since net_amount is an order-level total, we calculate item-level 
    # revenue (quantity * unit_price) to accurately attribute revenue to specific categories.
    df_with_month = df_with_month.withColumn(
        "item_revenue", 
        F.col("quantity") * F.col("unit_price")
    )
    
    # Get total revenue per category, per month
    category_monthly = df_with_month.groupBy("order_month", "category") \
        .agg(F.sum("item_revenue").alias("category_revenue"))
        
    # Use a Window function to get the TOTAL revenue for that specific month, 
    # then divide to find the percentage share
    window_month = Window.partitionBy("order_month")
    
    category_shares = category_monthly.withColumn(
        "total_monthly_revenue", 
        F.sum("category_revenue").over(window_month)
    ).withColumn(
        "revenue_share_pct", 
        F.round((F.col("category_revenue") / F.col("total_monthly_revenue")) * 100, 2)
    ).orderBy("order_month", F.desc("revenue_share_pct"))
    
    return category_shares


def main():
    logger.info("="*50)
    logger.info("STARTING TASK 04: AGGREGATIONS & WINDOW FUNCTIONS")
    logger.info("="*50)
    
    # 1. Grab the enriched dataframe from Task 3
    enriched_df = Task3.main()
    
    # 2. Execute the three required aggregations
    ranked_customers_df = get_customer_ranks(enriched_df)
    rolling_counts_df = get_rolling_order_count(enriched_df)
    category_shares_df = get_category_revenue_share(enriched_df)
    
    # 3. Print Results
    print("\n" + "="*50)
    print("TASK 04 VERIFICATION OUTPUTS")
    print("="*50)
    
    print("\n---> 6. CUSTOMERS RANKED BY LIFETIME SPEND (Within Country):")
    ranked_customers_df.show(5, truncate=False)
    
    print("\n---> 7. 7-DAY ROLLING ORDER COUNT PER CUSTOMER:")
    rolling_counts_df.orderBy("customer_id", "order_date").show(5, truncate=False)
    
    print("\n---> 8. CATEGORY REVENUE SHARE PER MONTH:")
    category_shares_df.show(5, truncate=False)
    
    print("\n---> B4: EXPLAIN PLAN FOR CATEGORY REVENUE SHARE:")
    # This prints the physical execution plan to the terminal
    category_shares_df.explain(mode="formatted")
    return {
        "ranked_customers": ranked_customers_df,
        "rolling_counts": rolling_counts_df,
        "category_shares": category_shares_df
    }

if __name__ == "__main__":
    task4_data = main()