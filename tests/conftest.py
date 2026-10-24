import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Let tests import the modules in spark_jobs/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark_jobs"))


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]").appName("txnflow-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()