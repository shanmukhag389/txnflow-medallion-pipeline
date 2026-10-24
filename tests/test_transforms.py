from decimal import Decimal

from transforms import gold_transform, silver_transform

SCHEMA = (
    "txn_id string, customer_id string, merchant string, category string, "
    "amount string, currency string, status string, txn_ts string, "
    "_corrupt_record string, dt string"
)


def row(txn_id="T1", customer_id="C1", merchant="Acme", amount="100.50",
        status="SUCCESS", txn_ts="2026-10-24T10:00:00"):
    return (txn_id, customer_id, merchant, "fuel", amount, "INR", status,
            txn_ts, None, "2026-10-24")


def test_valid_row_goes_to_silver_with_typed_amount(spark):
    silver, quarantine = silver_transform(spark.createDataFrame([row()], SCHEMA))
    assert silver.count() == 1
    assert quarantine.count() == 0
    assert silver.collect()[0]["amount"] == Decimal("100.50")


def test_bad_rows_are_quarantined_with_reason(spark):
    rows = [
        row(txn_id="T1", amount="-5.00"),
        row(txn_id="T2", amount="N/A"),
        row(txn_id="T3", customer_id=None),
        row(txn_id="T4", status="PENDING"),
        row(txn_id="T5", txn_ts="not-a-date"),
    ]
    silver, quarantine = silver_transform(spark.createDataFrame(rows, SCHEMA))
    reasons = {r["txn_id"]: r["reject_reason"] for r in quarantine.collect()}
    assert silver.count() == 0
    assert reasons == {
        "T1": "non_positive_amount",
        "T2": "invalid_amount",
        "T3": "missing_customer_id",
        "T4": "unknown_status",
        "T5": "invalid_timestamp",
    }


def test_duplicate_txn_ids_are_removed(spark):
    rows = [row(txn_id="T1"), row(txn_id="T1"), row(txn_id="T2")]
    silver, _ = silver_transform(spark.createDataFrame(rows, SCHEMA))
    assert silver.count() == 2


def test_gold_revenue_counts_only_successful_payments(spark):
    rows = [
        row(txn_id="T1", amount="100.00"),
        row(txn_id="T2", amount="50.00"),
        row(txn_id="T3", amount="30.00", status="FAILED"),
        row(txn_id="T4", amount="20.00", status="REFUNDED"),
    ]
    silver, _ = silver_transform(spark.createDataFrame(rows, SCHEMA))
    merchant_daily, customer_daily = gold_transform(silver)

    m = merchant_daily.collect()[0]
    assert m["txn_count"] == 4
    assert m["failed_count"] == 1
    assert m["revenue"] == Decimal("150.00")
    assert m["refunded_amount"] == Decimal("20.00")
    assert m["avg_ticket"] == Decimal("75.00")

    c = customer_daily.collect()[0]
    assert c["total_spend"] == Decimal("150.00")
    assert c["distinct_merchants"] == 1