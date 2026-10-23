from pyspark import pipelines as dp
from pyspark.sql import functions as f


CATALOG = spark.conf.get("banking.catalog")
ENV = spark.conf.get("banking.env")

if ENV not in {"dev", "prod"}:
    raise ValueError("banking.env must be 'dev' or 'prod'")

GOLD_SOURCE = f"{CATALOG}.silver_{ENV}"
GOLD_TARGET = f"{CATALOG}.gold_{ENV}"


# ============================================================
# 1. SHARED AGGREGATION
# ============================================================

def summarize_transactions(df, group_columns):
    """
    Count all current transactions.
    Calculate monetary totals for SUCCESS transactions only.
    Always group by currency to avoid mixing currencies.
    """
    successful = f.col("status") == "SUCCESS"

    successful_credit = (
        successful
        & (f.col("direction") == "CREDIT")
    )

    successful_debit = (
        successful
        & (f.col("direction") == "DEBIT")
    )

    zero = f.lit(0).cast("decimal(18,2)")

    aggregated = (
        df
        .groupBy(*group_columns)
        .agg(
            f.count("*").alias("transaction_count"),

            f.sum(
                f.when(successful, 1).otherwise(0)
            ).alias("successful_transaction_count"),

            f.sum(
                f.when(f.col("status") == "FAILED", 1)
                 .otherwise(0)
            ).alias("failed_transaction_count"),

            f.sum(
                f.when(f.col("status") == "PENDING", 1)
                 .otherwise(0)
            ).alias("pending_transaction_count"),

            f.sum(
                f.when(successful_credit, f.col("amount"))
                 .otherwise(zero)
            ).alias("total_credit"),

            f.sum(
                f.when(successful_debit, f.col("amount"))
                 .otherwise(zero)
            ).alias("total_debit")
        )
    )

    return aggregated.withColumn(
        "net_movement",
        f.col("total_credit") - f.col("total_debit")
    )


# ============================================================
# 2. DAILY TRANSACTION SUMMARY
# ============================================================

@dp.materialized_view(
    name=f"{GOLD_TARGET}.daily_transaction_summary",
    comment="Daily transaction activity and successful money movements."
)
def daily_transaction_summary():
    transactions = spark.read.table(
        f"{GOLD_SOURCE}.transactions_current"
    )

    return summarize_transactions(
        transactions,
        group_columns=["business_date", "currency"]
    )


# ============================================================
# 3. ACCOUNT TRANSACTION SUMMARY
# ============================================================

@dp.materialized_view(
    name=f"{GOLD_TARGET}.account_transaction_summary",
    comment="Daily transaction activity per account and currency."
)
def account_transaction_summary():
    transactions = spark.read.table(
        f"{GOLD_SOURCE}.transactions_current"
    )

    return summarize_transactions(
        transactions,
        group_columns=[
            "business_date",
            "account_id",
            "currency"
        ]
    )


# ============================================================
# 4. CUSTOMER TRANSACTION SUMMARY
# ============================================================

@dp.materialized_view(
    name=f"{GOLD_TARGET}.customer_transaction_summary",
    comment="Daily customer activity across accounts, per currency."
)
def customer_transaction_summary():
    transactions = spark.read.table(
        f"{GOLD_SOURCE}.transactions_current"
    )

    accounts = (
        spark.read.table(f"{GOLD_SOURCE}.accounts")
        .select("account_id", "customer_id")
    )

    customers = (
        spark.read.table(f"{GOLD_SOURCE}.customers")
        .select("customer_id", "customer_name", "city")
    )

    enriched = (
        transactions
        .join(accounts, on="account_id", how="inner")
        .join(customers, on="customer_id", how="inner")
    )

    return summarize_transactions(
        enriched,
        group_columns=[
            "business_date",
            "customer_id",
            "customer_name",
            "city",
            "currency"
        ]
    )