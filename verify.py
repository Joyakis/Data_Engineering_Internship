"""
Validation Script: Proves that Task 02 Cleaning was successful.
Imports the working pipeline from Task2.py without modifying it.
"""

import Task2
from pyspark.sql import functions as F

def run_verification():
    print("⏳ Running Task 2 pipeline in the background to grab data...")
    # This runs your existing code and grabs the returned dictionary!
    clean_dfs = Task2.main()
    
    orders_clean = clean_dfs["orders"]
    customers_clean = clean_dfs["customers"]

    print("\n" + "="*50)
    print("🕵️ VERIFICATION: PROVING THE CLEANING WORKED (ALL 5 STEPS)")
    print("="*50)

    # 1. Verify Deduplication
    print("\n---> STEP 1: DEDUPLICATION VERIFICATION")
    total_rows = orders_clean.count()
    distinct_rows = orders_clean.dropDuplicates().count()
    print(f"Total Rows: {total_rows} | Distinct Rows: {distinct_rows}")
    if total_rows == distinct_rows:
        print("✅ Success: No exact duplicates remain in the dataset.")
    else:
        print("❌ Warning: Duplicates still exist!")

    # 2. Verify Date Normalization
    print("\n---> STEP 2: DATE NORMALIZATION VERIFICATION")
    print("Schema Check for Customers signup_date (Should be 'date'):")
    customers_clean.select("signup_date").printSchema()
    
    failed_dates = customers_clean.filter(F.col("signup_date").isNull()).count()
    print(f"Failed date conversions (NULLs): {failed_dates}")
    
    print("Sample of standardized dates (Should all be YYYY-MM-DD):")
    customers_clean.select("signup_date").distinct().show(3)

    # 3. Verify Casing Normalization
    print("\n---> STEP 3: CUSTOMER TIER CASING NORMALIZED (Lowercase only)")
    customers_clean.select("customer_tier").distinct().show()

    # 4. Verify NULL Key Fields Were Dropped
    print("\n---> STEP 4: NULL IDs DROPPED VERIFICATION")
    null_keys_count = orders_clean.filter(F.col("order_id").isNull() | F.col("customer_id").isNull()).count()
    print(f"Rows with NULL order_id or customer_id: {null_keys_count}")
    if null_keys_count == 0:
        print("✅ Success: All missing key records were successfully dropped.")

    # 5. Verify Negative Amount Flagging
    print("\n---> STEP 5: NEGATIVE AMOUNTS FLAGGED")
    flagged_count = orders_clean.filter(F.col("is_negative_amount") == True).count()
    print(f"Total negative orders flagged: {flagged_count}")
    
    print("Showing top 3 flagged rows:")
    orders_clean.filter(F.col("is_negative_amount") == True) \
        .select("order_id", "total_amount", "is_negative_amount").show(3)

if __name__ == "__main__":
    run_verification()