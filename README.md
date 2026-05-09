# E-Commerce Data Pipeline Challenge

A comprehensive PySpark-based data engineering pipeline that processes messy e-commerce data through a structured, multi-stage transformation workflow. This project demonstrates enterprise-grade data quality handling, schema enforcement, and analytical transformations on a real-world dataset with intentional data quality issues.

## Project Overview

This pipeline ingests four  CSV files representing an e-commerce order management system, applies explicit validation and cleaning transformations, and produces enriched analytical datasets. The implementation demonstrates:

- **Explicit schema enforcement** without schema inference
- **Data quality validation** with rejection tracking
- **Date format normalization** across multiple formats
- **Deduplication** and NULL handling
- **Data joins and enrichment** with orphan detection
- **Window functions** for advanced analytics
- **Return analysis** and anomaly detection
- **Partitioned output** for scalable data storage

## Data Model

The project works with four CSV files representing an e-commerce ecosystem:

| File | Records | Purpose |
|------|---------|---------|
| `orders.csv` | Customer orders with dates, amounts, discounts, status | Master transaction table |
| `customers.csv` | Customer master data with tiers, countries, signup dates | Customer dimension |
| `order_items.csv` | Line items per order with products, quantities, prices | Order detail facts |
| `returns.csv` | Return/refund events linked to orders | Returns dimension |

## Intentional Data Quality Issues

The dataset contains realistic quality issues that must be handled explicitly:

| Issue | Prevalence | Handling |
|-------|-----------|----------|
| **Exact Duplicates** | ~8% across all tables | Dropped; duplicate count logged |
| **Mixed Date Formats** | DD/MM/YYYY and YYYY-MM-DD | Parsed to ISO format (YYYY-MM-DD) |
| **NULL customer_id** | ~4% of orders | Dropped; rejection reason logged |
| **NULL total_amount** | ~2% of orders | Dropped; rejection reason logged |
| **Negative amounts** | ~3% of orders | Flagged and retained; marked as anomaly |
| **Orphaned items** | ~4% of order_items | Isolated; separate rejected DataFrame |
| **Tier casing** | BRONZE/Bronze/bronze | Normalized to lowercase |
| **Refund > Order Amount** | ~5% of returns | Flagged as refund anomaly |

## Project Structure

```
.
├── data/                          # Input CSV files
│   ├── orders.csv
│   ├── customers.csv
│   ├── order_items.csv
│   └── returns.csv
├── output/                        # Generated outputs
│   ├── master_enriched_data/      # Parquet partitioned by year/month
│   ├── returns_analysis.csv
│   ├── customer_rankings.csv
│   ├── rejected_*.csv             # Rejected records per table
│   └── anomalies.csv
├── Task1.py                       # Data Ingestion & Schema Enforcement
├── Task2.py                       # Data Cleaning & Normalization
├── Task3.py                       # Joins & Enrichment
├── Task4.py                       # Aggregations & Window Functions
├── Task5.py                       # Return Analysis
├── Task6.py                       # Output & Partitioning
├── tests/
│   └── test_pipeline.py           # Unit tests (pytest)
├── verify.py                      # Validation script
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## Task Breakdown

### Task 1: Data Ingestion & Schema Enforcement
**Objective:** Load CSVs with explicit, defined schemas; catch casting failures.

**Key Actions:**
- Defines explicit `StructType` schemas for all four tables
- Reads CSVs in PERMISSIVE mode to capture malformed rows
- Splits data into `valid` and `rejected` DataFrames using `_corrupt_record` column
- Logs ingestion statistics (total, valid, rejected counts)

**Output:** Valid DataFrames + Rejected DataFrames with error reasons

**Design Decision:** Dates remain as StringType in Task 1 because Task 2 specifically handles normalization. Casting to DateType here would silently fail on non-conforming formats.

### Task 2: Data Cleaning & Normalization
**Objective:** Apply business logic validation, normalize inconsistent values, handle NULLs.

**Key Actions:**
- **Deduplicates** exact duplicate rows; logs count removed
- **Date normalization:** Parses DD/MM/YYYY to ISO YYYY-MM-DD format
- **Casing normalization:** Converts `customer_tier` to lowercase
- **NULL key fields:** Drops orders with NULL `customer_id` or NULL `total_amount`
- **Negative amounts:** Flags but retains (not dropped) for investigation
- **Email validation:** Filters out empty emails

**Output:** Cleaned, standardized DataFrames

**Design Decision:** NULL key fields are dropped (not flagged) because downstream joins require these keys. Negative amounts are retained to preserve complete audit trail.

### Task 3: Joins & Enrichment
**Objective:** Consolidate related data; detect and isolate orphaned records.

**Key Actions:**
- **Orphan detection:** Uses LEFT ANTI join to find order_items with no matching order
- **Order enrichment:** Calculates `net_amount = total_amount * (1 - discount_pct/100)`
- **Multi-table joins:** Merges orders → customers and orders → items using INNER JOINs
- **Outputs:** Enriched master table + isolated orphaned items

**Design Decision:** Uses INNER JOIN for orders→customers to eliminate orders without customer data. LEFT ANTI join for orphan detection is most efficient for large datasets.

### Task 4: Aggregations & Window Functions
**Objective:** Derive analytical metrics using advanced SQL windowing.

**Metrics Calculated:**
- **Customer rankings** by lifetime net spend within each country
- **Customer lifetime value** (LTV) and return rate
- **Rolling windows** for monthly trends
- **Percentage share** calculations

  To ensure the window functions were performing efficiently, I analyzed the physical plan using df.explain()
    ```
    category_shares_df.explain(mode="formatted")
    ```

**Output:** CSV files with rankings, top customers, monthly metrics

### Task 5: Return Analysis
**Objective:** Join returns data and identify anomalies.

**Key Actions:**
- **Returns join:** LEFT JOIN enriched orders to returns (preserves all orders)
- **Refund anomaly detection:** Flags refunds exceeding original order amount
- **Return rates:** Calculates percentage of orders with returns per customer
- **Top refunded products:** Identifies high-return items

**Output:** Returns analysis table + anomalies flagged

### Task 6: Output & Partitioning
**Objective:** Write results to persistent, queryable formats.

**Outputs:**
- **Master dataset:** Parquet partitioned by `order_year` and `order_month` (efficient for BI tools)
- **Summary tables:** CSV exports of rankings, returns, anomalies
- **All writes:** Idempotent (mode='overwrite') for re-runnable pipelines

**Design Decision:** Parquet for master data (compression, columnar format, partitioning support); CSV for summary tables (accessibility, downstream tools).

## Setup Instructions

### Prerequisites

- **Python:** 3.8 or higher
- **Java:** Required by Spark (JDK 8+)
- **Spark:** 3.3+ (installed via `pyspark` pip package)

### Installation

1. **Clone or download the project:**
   ```bash
   cd c:\Users\USER\Downloads\data
   ```

2. **Create a virtual environment (recommended):**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate         # Windows
   # or: source .venv/bin/activate # macOS/Linux
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Verify Spark installation:**
   ```bash
   python -c "from pyspark.sql import SparkSession; print(SparkSession.builder.getOrCreate().version)"
   ```

## Running the Pipeline

### Option 1: Run Full Pipeline (All 6 Tasks)

```bash
python Task6.py
```

This executes all tasks in sequence (1 → 2 → 3 → 4 → 5 → 6) and outputs results to `./output/`.

### Option 2: Run Individual Tasks

```bash
python Task1.py  # Ingestion only
python Task2.py  # Ingestion + Cleaning
python Task3.py  # + Joins & Enrichment
python Task4.py  # + Aggregations
python Task5.py  # + Returns Analysis
python Task6.py  # + Output & Partitioning
```

### Option 3: Validate Cleaning (Task 2)

```bash
python verify.py
```

Runs Task 2 and performs validation checks on:
- Deduplication effectiveness
- Date normalization success
- Casing normalization
- NULL key removal
- Negative amount flagging

### Option 4: Run Unit Tests

```bash
pytest tests/test_pipeline.py -v
```

Executes unit tests covering:
- Deduplication logic
- Date parsing
- Casing normalization
- NULL handling
- Aggregation calculations

## Configuration

### Spark Session Configuration

Tasks automatically configure a Spark session with:
- **Legacy time parser:** Enables parsing of mixed date formats
- **Local mode:** Uses all available CPU cores
- **Shuffle partitions:** Auto-configured based on dataset size

To customize, modify the `get_spark_session()` function in any Task file:

```python
def get_spark_session():
    return SparkSession.builder \
        .appName("E-Commerce Data Pipeline") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
```

### File Paths

**Windows:**
```python
base_path = "c:\\Users\\USER\\Downloads\\data\\data"
```

**macOS/Linux:**
```python
base_path = "/home/username/data"
```

**WSL (Ubuntu on Windows):**
```python
base_path = "/mnt/c/Users/USER/Downloads/data/data"
```

Update `base_path` in Task1.py before running.

## Reproducing Output Files

### Quick Start: Generate All Outputs

To generate all output files shown in the "Output Artifacts" section below, run:

```bash
python Task6.py
```

**What happens:**
1. ✅ Ingests all 4 CSVs with explicit schemas (Task 1)
2. ✅ Cleans and normalizes data (Task 2)
3. ✅ Joins and enriches data (Task 3)
4. ✅ Calculates aggregations and rankings (Task 4)
5. ✅ Analyzes returns and flags anomalies (Task 5)
6. ✅ Writes all outputs to `./output/` (Task 6)

**Expected runtime:** 30–60 seconds (local Spark on single machine)

**Expected output location:**
```
output/
├── master_enriched_data/           # Parquet partitioned by year/month
│   ├── order_year=2023/
│   │   ├── order_month=01/
│   │   ├── order_month=02/
│   │   └── ...
│   └── order_year=2024/
├── customer_rankings.csv           # ~50–100 rows (top customers per country)
├── returns_analysis.csv            # ~500–1000 rows (return metrics)
├── anomalies.csv                   # ~20–50 rows (flagged data issues)
├── orphaned_items.csv              # ~50–200 rows (items with no order)
├── rejected_orders.csv             # Casting/validation failures
├── rejected_customers.csv
├── rejected_items.csv
└── rejected_returns.csv
```

### Validation: Verify Outputs Were Generated

After running `python Task6.py`, verify outputs exist:

**Linux/macOS:**
```bash
ls -lh output/
ls -h output/*.csv
find output/master_enriched_data -type f | head -10
```

**Windows (PowerShell):**
```powershell
Get-ChildItem output/ -Recurse | Measure-Object -Property Length -Sum
```

### Inspect Output Files

**View CSV output (first 5 rows):**
```bash
head -5 output/customer_rankings.csv
head -5 output/returns_analysis.csv
head -5 output/anomalies.csv
```

**Load Parquet into Spark for analysis:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Inspect").getOrCreate()

# Load master enriched data
df = spark.read.parquet("output/master_enriched_data/")
df.printSchema()
df.show(5)

# Load specific partition (e.g., Jan 2023)
df_jan_2023 = spark.read.parquet("output/master_enriched_data/order_year=2023/order_month=01/")
print(f"Rows in Jan 2023: {df_jan_2023.count()}")
```

### Reproduce Single Task Outputs

If you want to inspect intermediate outputs:

```bash
# Task 1: See ingestion and rejection counts
python Task1.py

# Task 2: See what was cleaned
python Task2.py

# Task 3: See enriched data + orphaned items
python Task3.py
python -c "import Task3; Task3.main()"

# Task 4: See customer rankings only
python Task4.py

# Task 5: See return analysis only
python Task5.py
```

Each task prints detailed logs to console showing:
- Total rows ingested/processed
- Number of rows rejected and why
- Count of deduplicates removed
- Validation statistics

### Expected Data Quality Metrics

After running the full pipeline, you should see approximately:

| Metric | Expected Value |
|--------|-----------------|
| Orders ingested | ~2000 rows |
| Orders rejected/cleaned | ~200 rows |
| Customers ingested | ~500 rows |
| Customers cleaned | ~10–20 rows (duplicates + invalid dates) |
| Order items ingested | ~6000–7000 items |
| Orphaned items detected | ~200–300 items (~4% of total) |
| Returns with refund > order amount | ~5–10 flags |
| Duplicates removed | ~150–200 rows |
| Negative amounts flagged | ~60–90 anomalies |

### Troubleshooting Output Generation

**Problem:** `FileNotFoundError: output/ directory doesn't exist`
```bash
mkdir output
python Task6.py
```

**Problem:** Output files are empty or have 0 rows
- Check `rejected_*.csv` files to see what was filtered out
- Run `python verify.py` to validate data cleaning

**Problem:** Parquet files created but reading fails
```python
# Try reading with verbose error:
spark.read.parquet("output/master_enriched_data/").show(1)
```

**Problem:** Script ran but I don't see outputs
- Ensure `base_path` in Task1.py points to correct data location
- Check console logs for errors (look for "ERROR" or "Exception")
- Verify write permissions in current directory

## Output Artifacts

### Parquet Files (Master Data)
- **Location:** `output/master_enriched_data/`
- **Format:** Partitioned by `order_year` and `order_month`
- **Usage:** Load into BI tools (Tableau, Power BI, Looker) or analytical databases
- **Example Query:**
  ```python
  spark.read.parquet("output/master_enriched_data/order_year=2023/order_month=01")
  ```

### CSV Summaries
- **customer_rankings.csv** — Customers ranked by LTV per country
- **returns_analysis.csv** — Return rates and top refunded customers
- **anomalies.csv** — Flagged data quality issues (negatives, refund overage)
- **rejected_orders.csv** — Orders rejected during ingestion
- **rejected_customers.csv** — Customers rejected during ingestion
- **rejected_items.csv** — Items rejected during ingestion
- **rejected_returns.csv** — Returns rejected during ingestion
- **orphaned_items.csv** — Order items with no matching order

### Log Output
All tasks print detailed logs to console including:
- Row counts at each stage
- Rejection counts and reasons
- Data quality metrics
- Join results

## Design Decisions & Rationale

### 1. **Explicit Schemas Over Inference**
**Why:** Catches schema mismatches early; prevents silent data loss from type casting failures.

### 2. **Separate Rejection DataFrames**
**Why:** Enables audit trails; rejected rows are not discarded—they're logged and can be investigated.

### 3. **Dates as Strings in Task 1**
**Why:** Task 2 specifically handles normalization. Casting to DateType in Task 1 would fail silently on mixed formats.

### 4. **Dropping vs. Flagging NULLs**
**Decision:** NULL customer_id/total_amount are **dropped** because downstream joins require these keys. Negative amounts are **flagged but retained** because they represent valid (albeit anomalous) data.

### 5. **LEFT ANTI Join for Orphans**
**Why:** Most efficient; avoids scanning full inner join and then filtering—directly returns non-matching rows.

### 6. **Parquet Partitioning**
**Why:** Enables partition pruning in analytics; queries on specific months scan only relevant files, reducing latency and cost.

### 7. **Idempotent Writes (Overwrite Mode)**
**Why:** Enables re-runnable pipelines; no need to manually delete outputs before re-running.

## Known Limitations & Assumptions

### Assumptions

1. **Data files are always present** at configured path
2. **CSV format is consistent** (commas as delimiters, no special quoting issues beyond Spark defaults)
3. **Dates are either YYYY-MM-DD or DD/MM/YYYY** (no other formats)
4. **Customer tiers are one of:** bronze, silver, gold, platinum (case-insensitive)
5. **Return reasons are one of:** wrong_item, defective, arrived_late, not_as_described, changed_mind, duplicate_order
6. **All order_ids are unique** (within the valid dataset)
7. **Enough disk space** for output Parquet files (~100MB-1GB depending on data size)

### Limitations

1. **No incremental loading:** Entire datasets reprocessed each run (suitable for small-medium data)
2. **Local Spark only:** No distributed cluster support (can be added with minimal changes)
3. **Single-machine memory bound:** Large datasets (>10GB) may require Spark cluster or Hadoop setup
4. **No streaming support:** Batch processing only; not suitable for real-time pipelines
5. **Manual schema updates:** Adding new columns requires code changes (consider external schema registry for production)
6. **Limited error recovery:** Pipeline fails if any task errors; no checkpointing or fault tolerance
7. **No data versioning:** Overwrite mode means previous outputs are lost

### Scalability Considerations

For production deployment at scale:

- **Switch to cloud storage:** S3, Azure Blob Storage, or GCS for data (modify `base_path`)
- **Deploy to Databricks/EMR:** Use managed Spark clusters instead of local
- **Add external schema registry:** AVRO/Protobuf schemas in Confluent Schema Registry
- **Implement incremental loading:** Use watermarks and delta tables for delta processing
- **Add data quality framework:** Implement Great Expectations or Soda for automated validation
- **Parallelize tasks:** Use Airflow/Dagster for orchestration and parallel task execution

## Troubleshooting

### Issue: "Java not found" or Spark fails to initialize

**Solution:** Install Java JDK 8+:
```bash
# macOS
brew install openjdk@11

# Windows: Download from oracle.com/java/technologies/downloads
# Linux (Ubuntu)
sudo apt-get install openjdk-11-jdk
```

### Issue: "ModuleNotFoundError: No module named 'pyspark'"

**Solution:**
```bash
pip install pyspark --upgrade
```

### Issue: "File not found" at `base_path`

**Solution:** Verify path exists and update `base_path` in Task1.py:
```bash
ls -la c:\Users\USER\Downloads\data\data  # Windows
# or: find /path/to/data -name "*.csv"   # macOS/Linux
```

### Issue: Out of memory errors

**Solution:** Increase Spark driver memory:
```python
.config("spark.driver.memory", "8g") \
```

### Issue: "No such file or directory: output/"

**Solution:** Create output directory manually:
```bash
mkdir output
```

## Testing

### Run All Tests
```bash
pytest tests/test_pipeline.py -v
```

### Run Specific Test
```bash
pytest tests/test_pipeline.py::test_clean_orders_drops_duplicates -v
```



Tests cover:
- Deduplication logic
- Date normalization (DD/MM/YYYY parsing)
- Casing normalization
- NULL handling
- Aggregation window functions
- Return rate calculations

## Contributing

When extending the pipeline:

1. **Follow the task structure:** Each task should be self-contained and callable
2. **Add logging:** Use `logger.info()` for debugging and audit trails
3. **Create rejection DataFrames:** Always separate valid from invalid data
6. **Add tests:** New transformations must have unit tests in `tests/test_pipeline.py`
5. **Document assumptions:** Add comments explaining why data is handled certain ways
6. **Validate outputs:** Use `verify.py` pattern to validate each task's correctness

## License & Confidentiality

This project is part of a confidential data engineering assessment. 

## Support

For questions or issues:
- Check logs output by running tasks (contains detailed error messages)
- Review assumptions in this README
- Check tests/test_pipeline.py for usage examples
- Run verify.py to validate data quality at each stage

---

**Last Updated:** May 2026  
**Spark Version:** 3.3+  
**Python Version:** 3.8+
