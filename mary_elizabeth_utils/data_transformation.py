import logging
from collections.abc import Callable
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


def impute_missing_values(df: pl.LazyFrame, strategy: dict[str, str]) -> pl.LazyFrame:
    """
    Impute missing values in the DataFrame based on the specified strategy.

    Args:
        df (pl.LazyFrame): Input DataFrame.
        strategy (Dict[str, str]): Dictionary mapping column names to imputation methods.

    Returns:
        pl.LazyFrame: DataFrame with imputed values.

    Raises:
        ValueError: If an invalid imputation method is specified.
    """
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
            logger.error(f"Error imputing missing values for column {column}: {str(e)}")
            raise
    return df


def apply_transformations(
    df: pl.LazyFrame, transformations: dict[str, Callable[[Any], Any]]
) -> pl.LazyFrame:
    """
    Apply custom transformations to specified columns in the DataFrame.

    Args:
        df (pl.LazyFrame): Input DataFrame.
        transformations (Dict[str, Callable[[Any], Any]]): Dictionary mapping column names to transformation functions.

    Returns:
        pl.LazyFrame: DataFrame with applied transformations.
    """
    for column, transform_func in transformations.items():
        try:
            df = df.with_columns(
                pl.col(column).map_elements(transform_func).alias(f"{column}_transformed")
            )
        except Exception as e:
            logger.error(f"Error applying transformation to column {column}: {str(e)}")
            raise
    return df
