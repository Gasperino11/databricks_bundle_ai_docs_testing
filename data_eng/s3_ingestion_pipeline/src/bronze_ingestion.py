"""
Bronze Ingestion Task
=====================
Reads raw event data from AWS S3 in Parquet format and writes it
to the bronze layer in Unity Catalog with minimal transformation.
Adds ingestion metadata columns for lineage tracking.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="Bronze ingestion from S3")
    parser.add_argument("--source", required=True, help="S3 path to raw event data")
    parser.add_argument("--target-catalog", required=True, help="Target Unity Catalog name")
    parser.add_argument("--target-schema", required=True, help="Target schema name")
    return parser.parse_args()


def run_bronze_ingestion(spark, source_path, target_catalog, target_schema):
    """
    Read raw Parquet files from S3 and write to the bronze layer.

    Adds metadata columns:
    - _ingested_at: timestamp of when the record was ingested
    - _source_file: the source file path for lineage
    """
    raw_df = (
        spark.read
        .format("parquet")
        .option("mergeSchema", "true")
        .load(source_path)
    )

    bronze_df = raw_df.withColumn(
        "_ingested_at", F.current_timestamp()
    ).withColumn(
        "_source_file", F.input_file_name()
    )

    target_table = f"{target_catalog}.{target_schema}.raw_events"

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"Bronze ingestion complete. Wrote {bronze_df.count()} rows to {target_table}")
    return bronze_df


if __name__ == "__main__":
    args = parse_args()
    spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()
    run_bronze_ingestion(spark, args.source, args.target_catalog, args.target_schema)
