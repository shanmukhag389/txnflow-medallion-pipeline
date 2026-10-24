"""Generate synthetic card transactions for one day and upload them to S3 (raw layer)."""
import argparse
import json
import os
import random
import tempfile
from datetime import date, datetime, timedelta

import boto3
from faker import Faker

CATEGORIES = ["grocery", "food_delivery", "fuel", "travel",
              "electronics", "fashion", "utilities", "entertainment"]
STATUSES = ["SUCCESS"] * 90 + ["FAILED"] * 7 + ["REFUNDED"] * 3


def build_merchants(fake, n=50):
    return [{"merchant": fake.company(), "category": random.choice(CATEGORIES)}
            for _ in range(n)]


def make_record(i, day, merchants):
    m = random.choice(merchants)
    ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=random.randint(0, 86399))
    return {
        "txn_id": f"T{day:%Y%m%d}{i:06d}",
        "customer_id": f"C{random.randint(1, 20000):05d}",
        "merchant": m["merchant"],
        "category": m["category"],
        "amount": round(random.lognormvariate(5, 1), 2),
        "currency": "INR",
        "status": random.choice(STATUSES),
        "txn_ts": ts.isoformat(),
    }


def corrupt(record):
    """Plant deliberate data problems so the silver layer has something to clean."""
    r = random.random()
    if r < 0.01:
        record["customer_id"] = None
    elif r < 0.02:
        record["amount"] = -abs(record["amount"])
    elif r < 0.025:
        record["amount"] = "N/A"
    return record


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=date.today().isoformat())
    p.add_argument("--rows", type=int, default=100_000)
    p.add_argument("--bucket", default=os.environ.get("TXNFLOW_BUCKET", "txnflow-medallion-pipeline"))
    args = p.parse_args()

    day = date.fromisoformat(args.date)
    random.seed(args.date)       # same date -> same data, so runs are repeatable
    Faker.seed(args.date)
    merchants = build_merchants(Faker())

    records = [corrupt(make_record(i, day, merchants)) for i in range(args.rows)]
    records += random.sample(records, k=args.rows // 100)   # ~1% duplicate rows
    random.shuffle(records)

    key = f"raw/dt={args.date}/transactions.json"
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
        path = f.name
    boto3.client("s3").upload_file(path, args.bucket, key)
    os.remove(path)
    print(f"Uploaded {len(records)} rows to s3://{args.bucket}/{key}")


if __name__ == "__main__":
    main()
