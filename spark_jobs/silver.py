"""Silver layer job: read bronze, apply the silver transform, write silver + quarantine."""
import argparse

from common import get_spark
from pyspark.sql import functions as F
from transforms import silver_transform


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--bucket", default="txnflow-medallion-pipeline")
    args = p.parse_args()

    spark = get_spark("txnflow-silver")
    base = f"s3a://{args.bucket}"

    bronze = spark.read.parquet(f"{base}/bronze/dt={args.date}").withColumn("dt", F.lit(args.date))
    bronze_count = bronze.count()

    silver, quarantine = silver_transform(bronze)
    silver.write.mode("overwrite").partitionBy("dt").parquet(f"{base}/silver")
    quarantine.write.mode("overwrite").partitionBy("dt").parquet(f"{base}/quarantine")

    # Reconciliation: re-read what was written and check the numbers add up.
    silver_count = spark.read.parquet(f"{base}/silver/dt={args.date}").count()
    quarantine_count = spark.read.parquet(f"{base}/quarantine/dt={args.date}").count()
    dupes_removed = bronze_count - quarantine_count - silver_count

    print(f"Bronze: {bronze_count}")
    print(f"Silver: {silver_count}")
    print(f"Quarantined: {quarantine_count}")
    print(f"Duplicates removed: {dupes_removed}")
    if dupes_removed < 0:
        raise SystemExit("RECONCILIATION FAILED")
    print("Reconciliation OK")

    print("Quarantine breakdown:")
    spark.read.parquet(f"{base}/quarantine/dt={args.date}") \
        .groupBy("reject_reason").count().orderBy(F.desc("count")).show()
    spark.stop()


if __name__ == "__main__":
    main()