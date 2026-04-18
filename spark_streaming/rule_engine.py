"""
rule_engine.py
--------------
Stateless fraud rule definitions applied as Spark column expressions.
Each rule adds a boolean flag column and a score contribution.
Keeping rules as expressions (not UDFs) keeps them Catalyst-optimised.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, abs as spark_abs, array_contains,
    hour, to_timestamp, array, array_remove
)

# High-risk Merchant Category Codes (ISO 18245)
# Commonly associated with fraud: crypto exchanges, money transfers, gambling
HIGH_RISK_MCC_CODES = [
    "6051",  # Non-financial institutions — crypto/currency exchanges
    "6211",  # Security brokers
    "7995",  # Gambling transactions
    "4829",  # Money transfer
    "6099",  # Financial institutions — other
]


def apply_fraud_rules(df: DataFrame) -> DataFrame:
    """
    Apply all fraud detection rules to scored DataFrame.
    Each rule adds:
      - rule_<name>: BooleanType flag
      - score_<name>: IntegerType contribution (0 if rule not triggered)

    Returns DataFrame with all rule columns added.
    """

    return (
        df

        # ── Rule 1: High amount deviation ──────────────────────────────────────
        # Amount > card's 90-day mean + 3 standard deviations
        .withColumn("rule_high_amount_deviation",
            col("amount") > (col("card_mean_amount_90d") + (3 * col("card_stddev_amount_90d")))
        )
        .withColumn("score_high_amount_deviation",
            when(col("rule_high_amount_deviation"), lit(35)).otherwise(lit(0))
        )

        # ── Rule 2: Transaction velocity spike ─────────────────────────────────
        # More than 10 transactions from this card in the last hour
        .withColumn("rule_velocity_spike",
            col("card_txn_count_1h") > 10
        )
        .withColumn("score_velocity_spike",
            when(col("rule_velocity_spike"), lit(40)).otherwise(lit(0))
        )

        # ── Rule 3: Impossible geo-velocity ────────────────────────────────────
        # Distance between this and previous transaction implies speed > 500 km/h
        .withColumn("rule_impossible_travel",
            col("km_per_hour_since_last_txn") > 500
        )
        .withColumn("score_impossible_travel",
            when(col("rule_impossible_travel"), lit(50)).otherwise(lit(0))
        )

        # ── Rule 4: High-risk merchant category ────────────────────────────────
        .withColumn("rule_high_risk_mcc",
            col("merchant_mcc").isin(HIGH_RISK_MCC_CODES)
        )
        .withColumn("score_high_risk_mcc",
            when(col("rule_high_risk_mcc"), lit(25)).otherwise(lit(0))
        )

        # ── Rule 5: Country mismatch + high amount ─────────────────────────────
        # Card billing country differs from transaction country AND amount > 200
        .withColumn("rule_country_mismatch",
            (col("billing_country") != col("merchant_country")) &
            (col("amount") > 200)
        )
        .withColumn("score_country_mismatch",
            when(col("rule_country_mismatch"), lit(30)).otherwise(lit(0))
        )

        # ── Rule 6: Unusual hour + high amount ─────────────────────────────────
        # Transaction between 02:00–04:00 local time with amount > card mean
        .withColumn("txn_hour_local",
            hour(to_timestamp(col("event_timestamp")))
        )
        .withColumn("rule_unusual_hour",
            (col("txn_hour_local").between(2, 4)) &
            (col("amount") > col("card_mean_amount_90d"))
        )
        .withColumn("score_unusual_hour",
            when(col("rule_unusual_hour"), lit(15)).otherwise(lit(0))
        )

        # ── Rule 7: New merchant + high amount ─────────────────────────────────
        # First time this card has transacted with this merchant AND amount > 500
        .withColumn("rule_new_merchant_high_amount",
            (col("is_first_txn_with_merchant")) &
            (col("amount") > 500)
        )
        .withColumn("score_new_merchant_high_amount",
            when(col("rule_new_merchant_high_amount"), lit(10)).otherwise(lit(0))
        )

        # ── Collect triggered rule names for audit trail ────────────────────────
        .withColumn("triggered_rules",
            array_remove(
                array(
                    when(col("rule_high_amount_deviation"),   lit("HIGH_AMOUNT_DEVIATION")),
                    when(col("rule_velocity_spike"),          lit("VELOCITY_SPIKE")),
                    when(col("rule_impossible_travel"),       lit("IMPOSSIBLE_TRAVEL")),
                    when(col("rule_high_risk_mcc"),           lit("HIGH_RISK_MCC")),
                    when(col("rule_country_mismatch"),        lit("COUNTRY_MISMATCH")),
                    when(col("rule_unusual_hour"),            lit("UNUSUAL_HOUR")),
                    when(col("rule_new_merchant_high_amount"),lit("NEW_MERCHANT_HIGH_AMOUNT")),
                ),
                None
            )
        )
    )


def compute_fraud_score(df: DataFrame) -> DataFrame:
    """
    Sum all rule score contributions into a composite fraud_score (0–100).
    Capped at 100 — multiple rules can't push score above the maximum.
    """
    return (
        df.withColumn(
            "fraud_score",
            (
                col("score_high_amount_deviation") +
                col("score_velocity_spike")         +
                col("score_impossible_travel")      +
                col("score_high_risk_mcc")          +
                col("score_country_mismatch")       +
                col("score_unusual_hour")           +
                col("score_new_merchant_high_amount")
            ).cast("integer")
        )
        # Hard cap at 100
        .withColumn("fraud_score",
            when(col("fraud_score") > 100, lit(100))
            .otherwise(col("fraud_score"))
        )
    )
