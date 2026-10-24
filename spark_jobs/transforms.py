"""Pure Spark transformations (no I/O), so they can be unit tested."""
from pyspark.sql import functions as F

VALID_STATUSES = ["SUCCESS", "FAILED", "REFUNDED"]


def silver_transform(bronze):
    """Return (silver, quarantine) DataFrames from a bronze DataFrame."""
    typed = (
        bronze
        .withColumn("amount_clean", F.col("amount").cast("decimal(12,2)"))
        .withColumn("ts_clean", F.to_timestamp("txn_ts"))
    )

    # First matching rule wins; rows with no problem get a null reason.
    reason = (
        F.when(F.col("_corrupt_record").isNotNull(), "corrupt_json")
        .when(F.col("txn_id").isNull(), "missing_txn_id")
        .when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("amount_clean").isNull(), "invalid_amount")
        .when(F.col("amount_clean") <= 0, "non_positive_amount")
        .when(F.col("ts_clean").isNull(), "invalid_timestamp")
        .when(~F.col("status").isin(VALID_STATUSES), "unknown_status")
    )
    checked = typed.withColumn("reject_reason", reason)

    quarantine = checked.filter(F.col("reject_reason").isNotNull()).select(
        "txn_id", "customer_id", "merchant", "category", "amount", "currency",
        "status", "txn_ts", "_corrupt_record", "reject_reason", "dt",
    )
    silver = (
        checked.filter(F.col("reject_reason").isNull())
        .dropDuplicates(["txn_id"])
        .select(
            "txn_id", "customer_id", "merchant", "category",
            F.col("amount_clean").alias("amount"), "currency", "status",
            F.col("ts_clean").alias("txn_ts"),
            F.current_timestamp().alias("_processed_at"), "dt",
        )
    )
    return silver, quarantine


def gold_transform(silver):
    """Return (merchant_daily, customer_daily) aggregates from a silver DataFrame."""
    is_success = F.col("status") == "SUCCESS"
    is_failed = F.col("status") == "FAILED"
    is_refunded = F.col("status") == "REFUNDED"

    merchant_daily = (
        silver.groupBy("dt", "merchant", "category")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum(F.when(is_success, 1).otherwise(0)).alias("success_count"),
            F.sum(F.when(is_failed, 1).otherwise(0)).alias("failed_count"),
            F.sum(F.when(is_success, F.col("amount")).otherwise(0)).alias("revenue"),
            F.sum(F.when(is_refunded, F.col("amount")).otherwise(0)).alias("refunded_amount"),
        )
        .withColumn("avg_ticket", F.round(F.col("revenue") / F.col("success_count"), 2))
    )
    customer_daily = (
        silver.groupBy("dt", "customer_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.sum(F.when(is_success, F.col("amount")).otherwise(0)).alias("total_spend"),
            F.countDistinct("merchant").alias("distinct_merchants"),
            F.max("txn_ts").alias("last_txn_ts"),
        )
    )
    return merchant_daily, customer_daily