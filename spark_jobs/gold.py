"""Gold layer: business-ready daily aggregates built from silver."""
import argparse

from common import get_spark
from pyspark.sql import functions as F


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--bucket", default="txnflow-medallion-pipeline")
    args = p.parse_args()

    spark = get_spark("txnflow-gold")
    base = f"s3a://{args.bucket}"

    silver = (
        spark.read.parquet(f"{base}/silver/dt={args.date}")
        .withColumn("dt", F.lit(args.date))
    )

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

    merchant_daily.write.mode("overwrite").partitionBy("dt").parquet(f"{base}/gold/merchant_daily")
    customer_daily.write.mode("overwrite").partitionBy("dt").parquet(f"{base}/gold/customer_daily")

    # Reconciliation: gold totals must match silver totals.
    silver_revenue = silver.filter(is_success).agg(F.sum("amount")).first()[0]
    gold_merchant = spark.read.parquet(f"{base}/gold/merchant_daily/dt={args.date}")
    gold_customer = spark.read.parquet(f"{base}/gold/customer_daily/dt={args.date}")
    merchant_revenue = gold_merchant.agg(F.sum("revenue")).first()[0]
    customer_revenue = gold_customer.agg(F.sum("total_spend")).first()[0]

    print(f"Silver successful revenue:  {silver_revenue}")
    print(f"Gold merchant revenue:      {merchant_revenue}")
    print(f"Gold customer spend:        {customer_revenue}")
    print(f"merchant_daily rows: {gold_merchant.count()}, customer_daily rows: {gold_customer.count()}")
    if not (silver_revenue == merchant_revenue == customer_revenue):
        raise SystemExit("RECONCILIATION FAILED")
    print("Reconciliation OK")

    print("Top 5 merchants by revenue:")
    gold_merchant.orderBy(F.desc("revenue")).select(
        "merchant", "category", "txn_count", "revenue", "avg_ticket"
    ).show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
