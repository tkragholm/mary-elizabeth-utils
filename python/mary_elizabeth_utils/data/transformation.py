import logging

import polars as pl

from ..config.config import Config

logger = logging.getLogger(__name__)


def impute_missing_values(
    df: pl.LazyFrame, numeric_cols: list[str], categorical_cols: list[str]
) -> pl.LazyFrame:
    """
    Impute missing values in the DataFrame.

    Args:
        df (pl.LazyFrame): Input DataFrame.
        numeric_cols (List[str]): List of numeric column names.
        categorical_cols (List[str]): List of categorical column names.

    Returns:
        pl.LazyFrame: DataFrame with imputed values.
    """
    for col in df.columns:
        if col in numeric_cols:
            df = df.with_columns(pl.col(col).fill_null(pl.col(col).mean()))
        elif col in categorical_cols:
            df = df.with_columns(pl.col(col).fill_null(pl.col(col).mode()))
    return df


def apply_custom_transformations(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Apply custom transformations to the DataFrame.

    Args:
        df (pl.LazyFrame): Input DataFrame.

    Returns:
        pl.LazyFrame: DataFrame with applied transformations.
    """
    # Implement custom transformations here
    # Example:
    # if "birth_date" in df.columns:
    #     df = df.with_columns([
    #         ((pl.date(2023, 1, 1) - pl.col("birth_date")).dt.days() / 365.25).alias("age")
    #     ])
    return df


def transform_data(tables: dict[str, pl.LazyFrame], config: Config) -> dict[str, pl.LazyFrame]:
    """
    Apply transformations to all tables.

    Args:
        tables (Dict[str, pl.LazyFrame]): Dictionary of tables to transform.
        config (Dict): Configuration dictionary containing numeric and categorical column lists.

    Returns:
        Dict[str, pl.LazyFrame]: Dictionary of transformed tables.
    """
    transformed_tables = {}
    for name, df in tables.items():
        if df is not None:
            df = impute_missing_values(df, config.NUMERIC_COLS, config.CATEGORICAL_COLS)
            df = apply_custom_transformations(df)
            transformed_tables[name] = df
    return transformed_tables
