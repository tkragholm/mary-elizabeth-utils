import logging
import os

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns  # type: ignore

from .config import CATEGORICAL_COLS, NUMERIC_COLS, OUTPUT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_data(table_name: str) -> pl.LazyFrame:
    return pl.scan_parquet(os.path.join(OUTPUT_DIR, f"{table_name}_processed.parquet"))


def generate_summary_statistics(
    df: pl.LazyFrame,
    group_col: str,
    numeric_cols: list[str],
    categorical_cols: list[str],
) -> pl.DataFrame:
    numeric_summary = df.group_by(group_col).agg(
        [
            *[pl.col(col).mean().alias(f"{col}_mean") for col in numeric_cols],
            *[pl.col(col).median().alias(f"{col}_median") for col in numeric_cols],
            *[pl.col(col).std().alias(f"{col}_std") for col in numeric_cols],
            *[pl.col(col).min().alias(f"{col}_min") for col in numeric_cols],
            *[pl.col(col).max().alias(f"{col}_max") for col in numeric_cols],
        ]
    )

    categorical_summary = df.group_by(group_col).agg(
        [pl.col(col).value_counts().alias(f"{col}_counts") for col in categorical_cols]
    )

    return numeric_summary.join(categorical_summary, on=group_col).collect()


def plot_numeric_comparisons(df: pl.DataFrame, group_col: str, numeric_cols: list[str]) -> None:
    n_cols = 3
    n_rows = (len(numeric_cols) - 1) // n_cols + 1
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 5 * n_rows))
    axes = axes.flatten()

    for i, col in enumerate(numeric_cols):
        sns.boxplot(x=group_col, y=col, data=df.to_pandas(), ax=axes[i])
        axes[i].set_title(f"Distribution of {col}")
        axes[i].set_xlabel("")

    for i in range(len(numeric_cols), len(axes)):
        axes[i].remove()

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "numeric_comparisons.png"))
    plt.close()


def plot_categorical_comparisons(
    df: pl.DataFrame, group_col: str, categorical_cols: list[str]
) -> None:
    n_cols = 2
    n_rows = (len(categorical_cols) - 1) // n_cols + 1
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 5 * n_rows))
    axes = axes.flatten()

    for i, col in enumerate(categorical_cols):
        contingency_table = df.pivot(
            values="count", index=col, on=group_col, aggregate_function="sum"
        ).to_pandas()
        contingency_table.plot(kind="bar", stacked=True, ax=axes[i])
        axes[i].set_title(f"Distribution of {col}")
        axes[i].set_xlabel("")
        axes[i].legend(title=group_col)

    for i in range(len(categorical_cols), len(axes)):
        axes[i].remove()

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "categorical_comparisons.png"))
    plt.close()


def main() -> None:
    try:
        cohort_df = load_data("cohort_treatment_periods")
        person_df = load_data("Person")
        family_df = load_data("Family")
        socioeconomic_df = load_data("Socioeconomic_Status")

        baseline_data = (
            cohort_df.join(person_df, left_on="child_id", right_on="person_id")
            .join(family_df, on="family_id")
            .join(socioeconomic_df, on="family_id")
        )

        summary_stats = generate_summary_statistics(
            baseline_data, "group", NUMERIC_COLS, CATEGORICAL_COLS
        )
        summary_stats.write_csv(os.path.join(OUTPUT_DIR, "summary_statistics.csv"))
        logging.info("Summary statistics saved to CSV.")

        plot_numeric_comparisons(baseline_data.collect(), "group", NUMERIC_COLS)
        logging.info("Numeric comparisons plot saved.")

        plot_categorical_comparisons(baseline_data.collect(), "group", CATEGORICAL_COLS)
        logging.info("Categorical comparisons plot saved.")

    except Exception as e:
        logging.error(f"An error occurred during baseline characteristics analysis: {str(e)}")


if __name__ == "__main__":
    main()
