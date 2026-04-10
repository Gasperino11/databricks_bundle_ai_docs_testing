"""
Silver Transform Task
=====================
Reads raw event data from the bronze layer, applies cleaning and validation
rules, deduplicates records, and writes to the silver layer in Unity Catalog.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def parse_args():
    parser = argparse.ArgumentParser(description="Silver transformation")
    parser.add_argument("--source-catalog", required=True, help="Source Unity Catalog name")
    parser.add_argument("--source-schema", required=True, help="Source schema name")
    parser.add_argument("--target-catalog", required=True, help="Target Unity Catalog name")
    parser.add_argument("--target-schema", required=True, help="Target schema name")
    return parser.parse_args()


def run_silver_transform(spark, source_catalog, source_schema, target_catalog, target_schema):
    """
    Transform bronze data to silver layer.

    Applies:
    - Null filtering on required fields (event_id, event_timestamp, user_id)
    - Deduplication by event_id keeping the latest record
    - Data type casting and standardization
    - Adds _processed_at metadata column
    """
    source_table = f"{source_catalog}.{source_schema}.raw_events"
    bronze_df = spark.read.table(source_table)

    # Filter out records with null required fields
    cleaned_df = bronze_df.filter(
        F.col("event_id").isNotNull()
        & F.col("event_timestamp").isNotNull()
        & F.col("user_id").isNotNull()
    )

    # Deduplicate by event_id, keeping the most recently ingested record
    window = Window.partitionBy("event_id").orderBy(F.col("_ingested_at").desc())
    deduped_df = (
        cleaned_df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # Standardize and cast columns
    silver_df = (
        deduped_df
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("user_id", F.col("user_id").cast("string"))
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("_processed_at", F.current_timestamp())
    )

    target_table = f"{target_catalog}.{target_schema}.clean_events"

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"Silver transform complete. Wrote {silver_df.count()} rows to {target_table}")
    return silver_df


if __name__ == "__main__":
    args = parse_args()
    spark = SparkSession.builder.appName("SilverTransform").getOrCreate()
    run_silver_transform(
        spark, args.source_catalog, args.source_schema,
        args.target_catalog, args.target_schema
    )
