import polars as pl
import pytest

from mary_elizabeth_utils.datacheck import check_missing_values, check_outliers


def test_check_missing_values():
    df = pl.DataFrame({"A": [1, 2, None, 4], "B": [1, None, 3, 4]}).lazy()

    with pytest.raises(ValueError):
        check_missing_values(pl.LazyFrame(), "empty_table")

    # This should log missing values but not raise an exception
    check_missing_values(df, "test_table")


def test_check_outliers():
    df = pl.DataFrame({"A": [1, 2, 3, 100], "B": [1, 2, 3, 4]}).lazy()

    # This should log an outlier in column A but not in B
    check_outliers(df, "test_table", ["A", "B"])
