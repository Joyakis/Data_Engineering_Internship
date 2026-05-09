"""
E-Commerce Data Pipeline - Bonus B1: Unit Tests
Tests the data cleaning transformations using Pytest and synthetic DataFrames.
"""

import pytest
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
import Task2

# -----------------------------------------------------------------------------
# FIXTURE: Create a single Spark session for all tests to use (speeds up testing)
# -----------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("PySpark-Unit-Tests") \
        .master("local[1]") \
        .getOrCreate()

# -----------------------------------------------------------------------------
# TEST 1: Deduplication
# -----------------------------------------------------------------------------
def test_clean_orders_drops_duplicates(spark):
    # Create 3 rows: 2 are exact duplicates, 1 is unique
    data = [
        Row(order_id="O1", customer_id="C1", order_date="2023-01-01", total_amount=100.0),
        Row(order_id="O1", customer_id="C1", order_date="2023-01-01", total_amount=100.0),
        Row(order_id="O2", customer_id="C2", order_date="2023-01-02", total_amount=200.0)
    ]
    raw_df = spark.createDataFrame(data)
    
    # Run the cleaning function
    clean_df = Task2.clean_orders(raw_df)
    
    # Assert that only 2 rows remain
    assert clean_df.count() == 2, "Failed to drop duplicate rows!"

# -----------------------------------------------------------------------------
# TEST 2: Date Normalisation (Regex)
# -----------------------------------------------------------------------------
def test_clean_orders_date_normalisation(spark):
    # Mix of YYYY-MM-DD, DD/MM/YYYY, and absolute garbage data
    data = [
        Row(order_id="O1", customer_id="C1", order_date="2023-12-01", total_amount=10.0),
        Row(order_id="O2", customer_id="C2", order_date="15/12/2023", total_amount=20.0),
        Row(order_id="O3", customer_id="C3", order_date="Bad-Date", total_amount=30.0)
    ]
    raw_df = spark.createDataFrame(data)
    clean_df = Task2.clean_orders(raw_df)
    
    # Collect results into a dictionary for easy assertion checks
    results = {row["order_id"]: row["order_date"] for row in clean_df.collect()}
    
    # Assert the dates were successfully converted to native Python datetime.date objects
    assert results["O1"] == datetime.date(2023, 12, 1), "Failed to parse YYYY-MM-DD"
    assert results["O2"] == datetime.date(2023, 12, 15), "Failed to parse DD/MM/YYYY"
    assert results["O3"] is None, "Failed to nullify invalid date formats"

# -----------------------------------------------------------------------------
# TEST 3: Negative Amount Flagging
# -----------------------------------------------------------------------------
def test_clean_orders_flags_negative_amounts(spark):
    # One positive order, one negative order
    data = [
        Row(order_id="O1", customer_id="C1", order_date="2023-01-01", total_amount=150.0),
        Row(order_id="O2", customer_id="C2", order_date="2023-01-01", total_amount=-50.0)
    ]
    raw_df = spark.createDataFrame(data)
    clean_df = Task2.clean_orders(raw_df)
    
    results = {row["order_id"]: row["is_negative_amount"] for row in clean_df.collect()}
    
    # Assert the boolean flags are correctly applied
    assert results["O1"] is False, "Incorrectly flagged a positive amount"
    assert results["O2"] is True, "Failed to flag a negative amount"