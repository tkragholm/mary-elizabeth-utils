import logging
import os
from collections.abc import Callable
from typing import Any

import numpy as np
import polars as pl

from .config import OUTPUT_DIR, TABLE_NAMES  # type: ignore

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def check_missing_values(df: pl.LazyFrame, table_name: str) -> None:
    """
    Check for missing values in the DataFrame and log the results.

    Args:
        df (pl.LazyFrame): The LazyFrame to check for missing values.
        table_name (str): The name of the table being checked.

    Raises:
        ValueError: If the DataFrame is empty.
    """
    if df.select(pl.len()).collect().item() == 0:
        raise ValueError(f"The DataFrame for {table_name} is empty.")

    # Use unique aliases for each column
    missing_counts = df.select(
        [pl.col(col).null_count().alias(f"{col}_null_count") for col in df.collect_schema().names()]
    ).collect()
    total_rows = df.select(pl.len()).collect().item()

    logging.info(f"Missing value report for {table_name}:")
    for column, count in zip(df.collect_schema().names(), missing_counts.row(0), strict=False):
        if count > 0:
            percentage = (count / total_rows) * 100
            logging.info(f"  {column}: {count} missing values ({percentage:.2f}%)")


def check_outliers(df: pl.LazyFrame, table_name: str, numeric_columns: list[str]) -> None:
    """
    Check for outliers in numeric columns of the DataFrame and log the results.

    Args:
        df (pl.LazyFrame): The LazyFrame to check for outliers.
        table_name (str): The name of the table being checked.
        numeric_columns (List[str]): List of numeric column names to check for outliers.

    Raises:
        ValueError: If no numeric columns are provided.
    """
    if not numeric_columns:
        raise ValueError("No numeric columns provided for outlier detection.")

    logging.info(f"Outlier report for {table_name}:")
    for column in numeric_columns:
        try:
            q1, q3 = (
                df.select(
                    [
                        pl.col(column).quantile(0.25).alias("q1"),
                        pl.col(column).quantile(0.75).alias("q3"),
                    ]
                )
                .collect()
                .row(0)
            )
            iqr = q3 - q1
            lower_bound, upper_bound = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            outliers = df.filter((pl.col(column) < lower_bound) | (pl.col(column) > upper_bound))
            outlier_count = outliers.select(pl.len()).collect().item()
            if outlier_count > 0:
                logging.info(f"  {column}: {outlier_count} outliers detected")
        except Exception as e:
            logging.error(f"Error checking outliers for column {column}: {str(e)}")


def check_logical_consistency(
    df: pl.LazyFrame, table_name: str, rules: dict[str, Callable[[pl.LazyFrame], pl.Expr]]
) -> None:
    """
    Check logical consistency of the DataFrame based on provided rules and log the results.

    Args:
        df (pl.LazyFrame): The LazyFrame to check for logical consistency.
        table_name (str): The name of the table being checked.
        rules (Dict[str, Callable[[pl.LazyFrame], pl.Expr]]): Dictionary of rule names and their corresponding check functions.

    Raises:
        ValueError: If no consistency rules are provided.
    """
    if not rules:
        raise ValueError("No consistency rules provided.")

    logging.info(f"Logical consistency report for {table_name}:")
    for rule_name, rule_func in rules.items():
        try:
            inconsistent_count = df.filter(rule_func(df)).select(pl.len()).collect().item()
            if inconsistent_count > 0:
                logging.info(f"  {rule_name}: {inconsistent_count} inconsistencies detected")
        except Exception as e:
            logging.error(f"Error checking consistency rule '{rule_name}': {str(e)}")


def impute_missing_values(df: pl.LazyFrame, strategy: dict[str, str]) -> pl.LazyFrame:
    for column, method in strategy.items():
        try:
            if method == "mean":
                df = df.with_columns(pl.col(column).fill_null(pl.col(column).mean()))
            elif method == "median":
                df = df.with_columns(pl.col(column).fill_null(pl.col(column).median()))
            elif method == "mode":
                df = df.with_columns(pl.col(column).fill_null(pl.col(column).mode()))
            elif method in ["forward", "backward"]:
                df = df.with_columns(pl.col(column).fill_null(method))
            else:
                raise ValueError(f"Invalid imputation method '{method}' for column '{column}'")
        except Exception as e:
            logging.error(f"Error imputing missing values for column {column}: {str(e)}")
    return df


def apply_transformations(
    df: pl.LazyFrame, transformations: dict[str, Callable[[Any], Any]]
) -> pl.LazyFrame:
    for column, transform_func in transformations.items():
        try:
            df = df.with_columns(
                pl.col(column).map_elements(transform_func).alias(f"{column}_transformed")
            )
        except Exception as e:
            logging.error(f"Error applying transformation to column {column}: {str(e)}")
    return df


def process_table(table_name: str) -> None:
    logging.info(f"Processing table: {table_name}")

    try:
        df = pl.scan_parquet(os.path.join(OUTPUT_DIR, f"{table_name}.parquet"))

        check_missing_values(df, table_name)

        numeric_columns = [
            col
            for col in df.columns
            if pl.dtype_to_pandas_dtype(df.schema[col]) in ["int64", "float64"]
        ]
        check_outliers(df, table_name, numeric_columns)

        consistency_rules: dict[str, Callable[[pl.LazyFrame], pl.Expr]] = {
            "Birth date after current date": lambda x: pl.col("birth_date") > pl.now(),
            # Add more rules as needed
        }
        check_logical_consistency(df, table_name, consistency_rules)

        imputation_strategy = {
            "numeric_column": "mean",
            "categorical_column": "mode",
            "date_column": "forward",
            # Add more columns and strategies as needed
        }
        df = impute_missing_values(df, imputation_strategy)

        transformations: dict[str, Callable[[Any], Any]] = {
            "income": np.log1p,
            # Add more transformations as needed
        }
        df = apply_transformations(df, transformations)

        output_path = os.path.join(OUTPUT_DIR, f"{table_name}_processed.parquet")
        df.collect().write_parquet(output_path)
        logging.info(f"Processed {table_name} saved to {output_path}")

    except Exception as e:
        logging.error(f"Error processing {table_name}: {str(e)}")


def main() -> None:
    """
    Main function to process all tables defined in TABLE_NAMES.
    """
    for table_name in TABLE_NAMES:
        process_table(table_name)


if __name__ == "__main__":
    main()
