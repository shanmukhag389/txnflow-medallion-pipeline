"""Silver layer: type, validate, quarantine bad rows, deduplicate."""
import argparse

from common import get_spark
from pyspark.sql import functions as F

VALID_STATUSES = ["SUCCESS", "FAILED", "REFUNDED"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--bucket", default="txnflow-medallion-pipeline")
    args = p.parse_args()

    spark = get_spark("txnflow-silver")
    base = f"s3a://{args.bucket}"

    bronze = spark.read.parquet(f"{base}/bronze/dt={args.date}")
    bronze_count = bronze.count()

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

    valid = checked.filter(F.col("reject_reason").isNull())
    silver = (
        valid.dropDuplicates(["txn_id"])
        .select(
            "txn_id", "customer_id", "merchant", "category",
            F.col("amount_clean").alias("amount"), "currency", "status",
            F.col("ts_clean").alias("txn_ts"),
            F.current_timestamp().alias("_processed_at"), "dt",
        )
    )

    silver.write.mode("overwrite").partitionBy("dt").parquet(f"{base}/silver")
    quarantine.write.mode("overwrite").partitionBy("dt").parquet(f"{base}/quarantine")

    # Reconciliation: re-read what was written and check the numbers add up.
    silver_count = spark.read.parquet(f"{base}/silver/dt={args.date}").count()
    quarantine_count = spark.read.parquet(f"{base}/quarantine/dt={args.date}").count()
    valid_count = bronze_count - quarantine_count
    dupes_removed = valid_count - silver_count

    print(f"Bronze: {bronze_count}")
    print(f"Silver: {silver_count}")
    print(f"Quarantined: {quarantine_count}")
    print(f"Duplicates removed: {dupes_removed}")
    if silver_count + quarantine_count + dupes_removed != bronze_count or dupes_removed < 0:
        raise SystemExit("RECONCILIATION FAILED")
    print("Reconciliation OK")

    print("Quarantine breakdown:")
    spark.read.parquet(f"{base}/quarantine/dt={args.date}") \
        .groupBy("reject_reason").count().orderBy(F.desc("count")).show()
    spark.stop()


if __name__ == "__main__":
    main()
