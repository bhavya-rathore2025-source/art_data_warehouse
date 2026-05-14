from pyspark.sql.functions import col, count, when
from pyspark.sql.functions import col, count, when, trim


def data_quality_summary(df):
    """
    Returns null, empty string, and whitespace counts.
    """

    total_count = count("*").alias("total_count")

    null_checks = [
        count(
            when(col(c).isNull(), c)
        ).alias(f"{c}_nulls")

        for c in df.columns
    ]

    empty_checks = [
        count(
            when(col(c) == "", c)
        ).alias(f"{c}_empty_strings")

        for c in df.columns
    ]

    whitespace_checks = [
        count(
            when(trim(col(c)) == "", c)
        ).alias(f"{c}_whitespace")

        for c in df.columns
    ]

    return df.select(
        total_count,
        *null_checks,
        *empty_checks,
        *whitespace_checks
    )