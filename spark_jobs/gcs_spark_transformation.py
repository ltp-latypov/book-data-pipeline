"""
ETL Pipeline for Books Dataset (Spark + GCS)
This module performs end-to-end data transformation for:
- Books
- Users
- Ratings
Pipeline steps:
1. Read raw CSV data from Google Cloud Storage (GCS)
2. Apply data cleaning and normalization
3. Handle malformed rows and encoding issues
4. Enrich datasets with derived fields
5. Save transformed data as Parquet
Technologies:
- PySpark
- Google Cloud Storage (GCS)
- Logging for observability
"""

# ============================================================
# DATA QUALITY STRATEGY
# ============================================================
# This pipeline includes the following data quality checks:
# - Null validation for all datasets
# - Distinct key validation (e.g., ISBN uniqueness)
# - Detection and correction of malformed rows
# - Age anomaly detection (users dataset)
# - Schema enforcement via predefined schemas
# ============================================================

import os
import time
import logging
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import functions as F
from constants import Config, Schemas
from pyspark_utils import split_location_raw, null_check, clean_simple

# --- 1. CONFIGURE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
logger.info(f"# --- 1. CONFIGURE LOGGING ---\n")

# --- 2. PATH LOGIC ---
start = time.time()
key_path = os.getenv("GCP_KEY_PATH", "gcp-key.json")

if not key_path or not os.path.exists(key_path):
    if os.path.exists("../service-account.json"):
        key_path = "../service-account.json"
    else:
        key_path = "gcp-key.json"



logger.info(f"Using GCP Key from: {key_path}")
logger.info(f"\n\n# --- 2. PATH LOGIC GCP_KEY_PATH ---\n")      
logger.info(f"Using GCP Key from: {key_path}\n")



# --- 3. SPARK SESSION ---
spark = (SparkSession.builder
    .appName("GCS CSV Transform")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
    .getOrCreate())

logger.info(f"\n\n# --- 3. SPARK SESSION ---")
logger.info(f"Spark version: {spark.version}")
# Reduce Spark internal noise
spark.sparkContext.setLogLevel("WARN")


# --- 4. PROCESS BOOKS DATA ---

def process_books(spark, input_path: str, output_path: str) -> None:

    """
    Processes raw books dataset and outputs cleaned Parquet data.
    Steps:
    - Reads CSV with predefined schema
    - Performs null checks and validation
    - Fixes malformed (shifted) rows
    - Cleans encoding issues in titles
    - Standardizes author names
    - Converts year to integer and removes invalid values
    - Adds ingestion timestamp
    Args:
        spark (SparkSession): Active Spark session
        input_path (str): Path to raw books CSV in GCS
        output_path (str): Destination path for cleaned Parquet data
    Returns:
        None
    """

    logger.info(f"\n\n# --- 4. PROCESS BOOKS DATA ---")

    # Data quality checks:
    # - Null validation
    # - Distinct ISBN validation
    # - Detection of malformed (shifted) rows

    logger.info(f"Reading CSV from: {input_path}")
    
    df = spark.read.options(**Config.CSV_OPTIONS).schema(Schemas.BOOKS_SCHEMA).csv(input_path)
    logger.info("Schema loaded. Printing schema to stdout:")
    df.printSchema()

    row_count = df.count()
    logger.info(f"CSV Loaded successfully. Total rows: {row_count}")
    
    df.show(5)
    
    # Null validation
    null_check(df, table_name='BOOKS')
    
    # Handling shifted rows
    df = df.withColumn("split_parts", F.split(F.col("title"), r'\";'))
    shifted_rows = df.filter(F.size(F.col("split_parts")) > 1).count()
    logger.info(f"Found {shifted_rows} rows with title splitting issues")

    # Detect rows where author column actually contains year (shifted data)
    author_condition = (F.col("author").rlike(r'^\d{4}$'))

     # Fix shifted rows by reassigning columns
    df = df.withColumn("publisher", F.when(author_condition, F.col("year")).otherwise(F.col("publisher"))) \
                        .withColumn("title", F.when(author_condition, F.col("split_parts").getItem(0)).otherwise(F.col("title"))) \
                        .withColumn("image_url_large", F.when(author_condition, F.col("image_url_medium")).otherwise(F.col("image_url_large"))) \
                        .withColumn("year", F.when(author_condition, F.col("author")).otherwise(F.col("year"))) \
                        .withColumn("author", F.when(author_condition, F.col("split_parts").getItem(1)).otherwise(F.col("author")))          
                                
    # Fix encoding issues in titles
    logger.info("Apply ENCODING_FIXES from constants")
    for bad_str, good_str in Config.ENCODING_FIXES.items():
        df = df.withColumn("title", F.regexp_replace(F.col("title"), bad_str, good_str))

    # General text cleanup
    df = df.withColumn("title", F.translate(F.col("title"), '\\"', '')) \
                        .withColumn("title", F.regexp_replace(F.col("title"), "&amp;", "&")) \
                        .withColumn("title", F.regexp_replace(F.col("title"), "/", ", ")) \
                        .withColumn("title", F.trim(F.regexp_replace(F.col("title"), "\\s+", " "))
                    )

    # Validate uniqueness of ISBN
    # Drop duplicates
    initial_count = df.count()
    df = df.dropDuplicates(["ISBN"])
    final_count = df.count()
    
    logger.info(f"Dropped {initial_count - final_count} duplicate ISBNs")

    logger.info("Convert year to INT type")
    df = df.drop("split_parts").withColumn("year", F.col("year").cast("int"))

    # Remove invalid year values (0 or future years)
    logger.info("Convert uninformative years to NULL")
    current_year = datetime.now().year
    df = df.withColumn("year", 
                                F.when((F.col("year") == 0) | (F.col("year") > current_year), None)
                                .otherwise(F.col("year"))
                        )

    df = df.withColumn("title", F.trim(F.col("title")))

    # Normalize author names (spacing + capitalization)
    df = df.withColumn("author", F.initcap(F.regexp_replace(F.trim(F.col("author")), r'\.([^\s])', r'. $1')))

    # Add ingestion timestamp
    df = df.withColumn("ingested_at", F.current_timestamp())

    # Write output in Parquet format
    logger.info(f"Writing transformed BOOKS data from {input_path} source to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Write operation completed.")


# --- 5. PROCESS USERS DATA ---

def process_users(spark, input_path: str, output_path: str) -> None:
    
    """
    Processes raw users dataset and enriches location data.
    Key transformations:
    - Converts age to integer
    - Splits raw location into structured fields
    - Uses broadcast join for efficient country normalization
    - Detects and flags age anomalies
    Args:
        spark (SparkSession): Active Spark session
        input_path (str): Path to raw users CSV
        output_path (str): Output path for cleaned data
    """

    logger.info(f"\n\n# --- PROCESS USERS DATA ---")

    # Data quality checks:
    # - Null validation
    # - Age anomaly detection
    # - Location parsing validation

    # Create lookup table for country normalization
    lookup_list = []
    for c in Config.VALID_COUNTRIES:
        lookup_list.append((c.lower(), c))
    for raw, clean in Config.COUNTRY_MAPPING.items():
        lookup_list.append((raw.lower(), clean))
    
    # Remove duplicates and create DF
    lookup_df = spark.createDataFrame(list(set(lookup_list)), ["raw_name", "clean_country"])

    # Read data
    df = spark.read.options(**Config.CSV_OPTIONS).schema(Schemas.USERS_SCHEMA).csv(input_path)

    # Convert age safely (string -> float -> int)
    df = df.withColumn("age", F.expr("try_cast(age AS float)").cast("int"))

    # Validate uniqueness of user_id
    # Drop duplicates
    initial_count = df.count()
    df = df.dropDuplicates(["user_id"])
    final_count = df.count()
    
    logger.info(f"Dropped {initial_count - final_count} duplicate user_id")

    # Null validation
    null_check(df, "USERS")
    df.show(5)

    # Split location string into components
    df = split_location_raw(df)
    
    # Broadcast join for efficient country mapping
    # We join our main data against the small lookup table
    df = df.join(F.broadcast(lookup_df), df.raw_last_part == lookup_df.raw_name, "left")
    
    df = df.withColumn("country", F.coalesce(F.col("clean_country"), F.lit("Unknown")))

    # City and region extraction logic
    first_has_digits = F.col("raw_first_part").rlike(r"\d")

    df = df.withColumn("city", 
        F.when((F.col("loc_size") >= 3) & first_has_digits, clean_simple(F.col("raw_second_part")))
        .otherwise(clean_simple(F.col("raw_first_part")))
    )

    df = df.withColumn("region", 
        # If address exists (size 4), region is usually the 3rd part
        F.when((F.col("loc_size") >= 4) & first_has_digits, clean_simple(F.col("raw_pre_last_part")))
        # Standard case: region is the part before the country
        .when(F.col("loc_size") >= 2, clean_simple(F.col("raw_pre_last_part")))
        .otherwise(F.lit("Unknown"))
    )

    # Flag anomalous ages
    df = df.withColumn("age_anomaly", F.when((F.col("age") < 5) | (F.col("age") > 100), 1).otherwise(0))

    # Add ingestion timestamp
    df = df.withColumn("ingested_at", F.current_timestamp())

    # Write output in Parquet format 
    logger.info(f"Writing transformed USERS to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Write operation completed.")


# --- 6. PROCESS RATING DATA ---

def process_rating(spark, input_path: str, output_path: str) -> None:

    """
    Processes raw ratings dataset.
    Steps:
    - Reads CSV with schema
    - Performs null validation
    - Adds ingestion timestamp
    """

    logger.info(f"\n\n# --- 5. PROCESS RATINGS DATA ---")

    # Data quality checks:
    # - Null validation
    # - Schema validation

    logger.info(f"Reading RATINGS Data from: {input_path}")

    df = spark.read.options(**Config.CSV_OPTIONS).schema(Schemas.RATINGS_SCHEMA).csv(input_path)

    logger.info(f"Schema for RATINGS data loaded")

    df.printSchema()

    row_count = df.count()
    logger.info(f"Total rows for RATINGS data: {row_count}")

    logger.info("Sample rows RATINGS data:")
    df.show(5)

    # Validate uniqueness of user_id and ISBN
    # Drop duplicates
    initial_count = df.count()
    df = df.dropDuplicates(["ISBN", "user_id"])
    final_count = df.count()
    
    logger.info(f"Dropped {initial_count - final_count} duplicate ISBN, user_id")
    
    # Null validation
    null_amount = null_check(df, table_name='RATINGS')

    df = df.withColumn("ingested_at", F.current_timestamp())

    # Write output in Parquet format
    logger.info(f"Writing transformed RATINGS data from {input_path} source to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Write operation completed.")



# --- 7. MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    try:
        # Run Pipeline
        process_books(spark, Config.INPUT_PATH_BOOKS, Config.OUTPUT_PATH_BOOKS)
        process_users(spark, Config.INPUT_PATH_USERS, Config.OUTPUT_PATH_USERS)
        process_rating(spark, Config.INPUT_PATH_RATING, Config.OUTPUT_PATH_RATING)
        
        # FIXED: use 'start' instead of 'start_time'
        total_time = round(time.time() - start, 2) 
        logger.info(f"Pipeline completed successfully in {total_time} seconds")
        
    except Exception as e:
        logger.critical(f"Pipeline failed: {e}", exc_info=True)
    finally:
        spark.stop()