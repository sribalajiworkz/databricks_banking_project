from pyspark import pipelines as dp
from pyspark.sql import functions as f
from schemas.customers import CUSTOMERS_SCHEMA

CATALOG = spark.conf.get("banking.catalog").strip()
ENV = spark.conf.get("banking.env").strip().lower()

if not CATALOG:
    raise ValueError("banking.catalog must not be empty")

if ENV not in {"dev", "prod"}:
    raise ValueError("banking.env must be 'dev' or 'prod'")

BRONZE = f"{CATALOG}.bronze_{ENV}"

LANDING = spark.conf.get(
    "banking.landing_path").strip()

if not LANDING:
    raise ValueError(
        "Pipeline configuration 'banking.landing_path' must not be empty."
    )

if not LANDING.startswith("/Volumes/"):
    raise ValueError(
        "Pipeline configuration 'banking.landing_path' "
        "must start with '/Volumes/'."
    )

LANDING = LANDING.rstrip("/")

def read_source(folder,file_format,schema,options=None):
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",file_format)
        .schema(schema)
    )

    for key,value in (options or {}).items():
        reader=reader.option(key,value)

    return (
        reader.load(f"{LANDING}/{folder}")
        .select(
            "*",
            f.col("_metadata.file_path").alias("_source_file"),
            f.current_timestamp().alias("_ingested_at")
        )
    )
        
@dp.table(name=f"{BRONZE}.customers")
def customers():
    return read_source(
        "customers",
        "csv",
        schema=CUSTOMERS_SCHEMA,
        options={
            "header": "true",
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record",
            "cloudFiles.schemaEvolutionMode": "rescue",
            "rescuedDataColumn": "_rescued_data",
        },
    )


@dp.table(name=f"{BRONZE}.accounts")
def accounts():
    return read_source(
        "accounts",
        "parquet",
        """
        account_id STRING,
        customer_id STRING,
        account_type STRING
        """,
        {
            "cloudFiles.schemaEvolutionMode": "rescue",
            "rescuedDataColumn": "_rescued_data",
        },
    )


@dp.table(name=f"{BRONZE}.transactions")
def transactions():
    return read_source(
        "transactions",
        "json",
        """
        transaction_id STRING,
        account_id STRING,
        amount STRING,
        channel STRING,
        currency STRING,
        direction STRING,
        event_ts STRING,
        op STRING,
        source_seq STRING,
        status STRING,
        _corrupt_record STRING
        """,
        {
            "multiLine": "false",
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record",
            "cloudFiles.schemaEvolutionMode": "rescue",
            "rescuedDataColumn": "_rescued_data",
        },
    )


@dp.table(name=f"{BRONZE}.branches")
def branches():
    return read_source(
        "branches",
        "csv",
        """
        branch_code STRING,
        branch_name STRING,
        state STRING,
        _corrupt_record STRING
        """,
        {
            "header": "true",
            "sep": "|",
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record",
            "cloudFiles.schemaEvolutionMode": "rescue",
            "rescuedDataColumn": "_rescued_data",
        },
    )


@dp.table(name=f"{BRONZE}.merchants")
def merchants():
    return read_source(
        "merchants",
        "excel",
        """
        merchant_id STRING,
        merchant_name STRING,
        category STRING,
        country STRING
        """,
        {
        "headerRows": "1",
        "dataAddress": "merchants",
        "ignoreMissingSheet": "false",
        "cloudFiles.schemaEvolutionMode": "none",
        },
    )

