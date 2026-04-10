"""
Gold Aggregation Task
=====================
Reads clean event data from the silver layer, computes daily aggregated
metrics, and writes to the gold layer in Unity Catalog for BI consumption.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(description="Gold aggregation")
    parser.add_argument("--source-catalog", required=True, help="Source Unity Catalog name")
    parser.add_argument("--source-schema", required=True, help="Source schema name")
    parser.add_argument("--target-catalog", required=True, help="Target Unity Catalog name")
    parser.add_argument("--target-schema", required=True, help="Target schema name")
    return parser.parse_args()


def run_gold_aggregate(spark, source_catalog, source_schema, target_catalog, target_schema):
    """
    Aggregate silver data into gold-layer daily metrics.

    Produces:
    - daily_event_metrics: daily counts, unique users, and event type breakdowns
    """
    source_table = f"{source_catalog}.{source_schema}.clean_events"
    silver_df = spark.read.table(source_table)

    # Daily event metrics
    daily_metrics_df = (
        silver_df
        .groupBy("event_date", "event_type")
        .agg(
            F.count("event_id").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.min("event_timestamp").alias("first_event_at"),
            F.max("event_timestamp").alias("last_event_at"),
        )
        .withColumn("_aggregated_at", F.current_timestamp())
    )

    target_table = f"{target_catalog}.{target_schema}.daily_event_metrics"

    (
        daily_metrics_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"Gold aggregation complete. Wrote {daily_metrics_df.count()} rows to {target_table}")
    return daily_metrics_df


if __name__ == "__main__":
    args = parse_args()
    spark = SparkSession.builder.appName("GoldAggregate").getOrCreate()
    run_gold_aggregate(
        spark, args.source_catalog, args.source_schema,
        args.target_catalog, args.target_schema
    )
