# TxnFlow: End-to-End Medallion Data Pipeline & Automated Cloud Platform

An automated, cloud-native batch data pipeline designed to ingest, validate, transform, and analyze over 101,000 daily financial transaction records. This production-grade architecture leverages a three-tier Medallion structure to process semi-structured streaming data into high-performance, analytics-ready data warehouse tables.

## 🏗️ Technical Architecture & Cloud Infrastructure

* **Compute & Execution Layer:** Built entirely using a multi-threaded PySpark processing engine inside an isolated virtual environment, maximizing single-node memory mapping efficiencies without cluster overhead.
* **Decoupled Cloud Storage:** Utilizes AWS S3 as a structural data lake, enforcing clear security boundaries across decoupled prefixes (`raw/` ➡️ `bronze/` ➡️ `silver/` ➡️ `gold/`).
* **Modern Cloud Data Warehouse:** Integrated with Snowflake using native cloud-to-cloud IAM Storage Integrations and External Stages, allowing high-performance, schema-agnostic `COPY INTO` Parquet ingestion pipelines.
* **Orchestration Control:** Managed via Apache Airflow, utilizing an active LocalExecutor backend metadata store and interactive Web UI dashboard to handle sequential dependency enforcement, backfills, and task logging.
* **Continuous Integration & Deployment (CI/CD):** Engineered a master GitHub Actions workflow engine that runs automated styling linters (`ruff`), localized transform unit tests (`pytest`), and direct, secure deployment syncs straight to AWS S3.

## 📊 Medallion Transform Layers & Data Quality Guards

1. **Synthetic Generation & Raw Ingest:** Generates 101,000 JSON Lines transactional records per run partition using Python `Faker`, embedding synthetic anomalies such as null data fields, duplicate transmission strings, and negative account values.
2. **Bronze Layer (Structural Ingestion Replica):** Infuses hardcoded `StructType` schemas to bind data data metrics, writing un-mutated Parquet replicas enriched with system injection timestamps (`LOADED_AT`).
3. **Silver Layer (Cleaning & Quarantine):** Casts data formats, filters out duplicate records on unique transaction keys (`txn_id`), isolates structural corruption strings into an independent quarantine directory, and performs a cross-layer programmatic row evaluation (**Reconciliation OK**) to guarantee zero untracked data drop-offs.
4. **Gold Layer (Analytical Aggregations):** Builds high-density reporting dimensions (Daily summary spending matrices by merchant sectors and consumer customer profiles) utilizing Spark's dynamic partition overwrite configurations to ensure **100% idempotent retry-safety** upon execution failures.
5. **Snowflake Synchronization:** Automates sequential loading pipelines right after the Gold layer transforms finish by executing an optimized python data connector block, updating target analytical reporting tables dynamically.

## 📈 Platform Metrics & Operational Performance

* **Daily Transaction Volume:** 101,000 production records successfully ingested, verified, and mapped per processing batch.
* **End-to-End Processing Efficiency:** Less than 5 minutes total elapsed time from synthetic generation down to Snowflake warehouse storage table synchronization.
* **Quality Assurance Profile:** Robust local integration suite passing automated PEP8 compliance linting and mock transform test assertions.

## 🛠️ Step-by-Step Production Deployment

### 1. Initialize the Environment Layout
```bash
cd ~/txnflow-medallion-pipeline
source scripts/airflow_env.sh
```

### 2. Launch the Orchestration Dashboard
```bash
airflow db init
airflow standalone
```
*Extract the generated `admin` credentials from your running log output and open the dashboard via the preview proxy on port `8080` to toggle the workflow active.*

### 3. Review Analytical Metrics in Snowflake
Run these validation commands directly inside your Snowflake query worksheets to audit your processed datasets:
```sql
SELECT DT, COUNT(DISTINCT CUSTOMER_ID), SUM(TOTAL_SPEND) 
FROM TXNFLOW.GOLD.CUSTOMER_DAILY 
GROUP BY DT;

SELECT * FROM TXNFLOW.GOLD.MERCHANT_DAILY LIMIT 10;
```
