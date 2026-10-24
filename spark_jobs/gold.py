"""Gold layer job: read silver, apply the gold transform, write the aggregates."""
import argparse

from common import get_spark
from pyspark.sql import functions as F
from transforms import gold_transform


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--bucket", default="txnflow-medallion-pipeline")
    args = p.parse_args()

    spark = get_spark("txnflow-gold")
    base = f"s3a://{args.bucket}"

    silver = spark.read.parquet(f"{base}/silver/dt={args.date}").withColumn("dt", F.lit(args.date))
    merchant_daily, customer_daily = gold_transform(silver)

    merchant_daily.write.mode("overwrite").partitionBy("dt").parquet(f"{base}/gold/merchant_daily")
    customer_daily.write.mode("overwrite").partitionBy("dt").parquet(f"{base}/gold/customer_daily")

    # Reconciliation: gold totals must match silver totals.
    silver_revenue = silver.filter(F.col("status") == "SUCCESS").agg(F.sum("amount")).first()[0]
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