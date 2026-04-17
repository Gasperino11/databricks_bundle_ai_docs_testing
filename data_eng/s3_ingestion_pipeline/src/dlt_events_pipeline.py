"""
DLT Events Pipeline
====================
Delta Live Tables pipeline for streaming event data processing.
Defines bronze, silver, and gold DLT tables using the medallion architecture.
"""

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="bronze_events_raw",
    comment="Raw event data ingested from S3 with minimal transformation.",
    table_properties={"quality": "bronze"},
)
def bronze_events_raw():
    return (
        spark.read  # noqa: F821 - spark is available in DLT context
        .format("parquet")
        .load("s3://acme-data-lake/raw/events/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


@dlt.table(
    name="silver_events_clean",
    comment="Cleaned and deduplicated event data.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_timestamp", "event_timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
def silver_events_clean():
    bronze = dlt.read("bronze_events_raw")
    return (
        bronze
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("_processed_at", F.current_timestamp())
    )


@dlt.table(
    name="gold_daily_metrics",
    comment="Daily aggregated event metrics for BI consumption.",
    table_properties={"quality": "gold"},
)
def gold_daily_metrics():
    silver = dlt.read("silver_events_clean")
    return (
        silver
        .groupBy("event_date", "event_type")
        .agg(
            F.count("event_id").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users"),
            F.min("event_timestamp").alias("first_event_at"),
            F.max("event_timestamp").alias("last_event_at"),
        )
        .withColumn("_aggregated_at", F.current_timestamp())
    )
