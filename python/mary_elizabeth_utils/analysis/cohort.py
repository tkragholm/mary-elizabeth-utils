from pathlib import Path

import polars as pl

from ..utils.caching import cache_result


@cache_result("cache")
def create_cohorts(
    tables: dict[str, pl.LazyFrame], config: dict, icd10_codes: dict[str, str]
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    severe_chronic_cases = identify_severe_chronic_cases(tables, icd10_codes)
    exposed_group = create_exposed_group(severe_chronic_cases, tables)
    unexposed_pool = create_unexposed_group(tables)
    exposed_cohort, unexposed_cohort = match_cohorts(exposed_group, unexposed_pool)

    output_dir = Path(config["output_dir"])
    exposed_cohort.collect().write_parquet(output_dir / "exposed_cohort.parquet")
    unexposed_cohort.collect().write_parquet(output_dir / "unexposed_cohort.parquet")

    return exposed_cohort, unexposed_cohort


@cache_result("cache")
def identify_severe_chronic_cases(
    tables: dict[str, pl.LazyFrame], icd10_codes: dict[str, str]
) -> pl.LazyFrame:
    diagnosis_df = tables.get("Diagnosis")
    if diagnosis_df is None:
        raise ValueError("Diagnosis table not found")

    return diagnosis_df.filter(
        (pl.col("diagnosis_code").is_in(icd10_codes.keys()))
        & (pl.col("diagnosis_date").is_between(pl.date(2000, 1, 1), pl.date(2018, 12, 31)))
    )


@cache_result("cache")
def create_exposed_group(
    severe_chronic_cases: pl.LazyFrame, tables: dict[str, pl.LazyFrame]
) -> pl.LazyFrame:
    child_df = tables.get("Child")
    if child_df is None:
        raise ValueError("Child table not found")

    exposed_children = severe_chronic_cases.join(
        child_df, left_on="person_id", right_on="child_id"
    ).filter((pl.col("diagnosis_date") - pl.col("birth_date")) <= pl.duration(days=5 * 365))

    return exposed_children.select(
        [pl.col("family_id"), pl.col("child_id"), pl.col("diagnosis_date").alias("index_date")]
    ).unique()


@cache_result("cache")
def create_unexposed_group(tables: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    child_df = tables.get("Child")
    if child_df is None:
        raise ValueError("Child table not found")

    return (
        child_df.filter(pl.col("birth_date").is_between(pl.date(1995, 1, 1), pl.date(2018, 12, 31)))
        .select([pl.col("family_id"), pl.col("child_id"), pl.col("birth_date")])
        .unique()
    )


@cache_result("cache")
def match_cohorts(
    exposed_group: pl.LazyFrame, unexposed_pool: pl.LazyFrame
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    matched_unexposed = (
        exposed_group.join(
            unexposed_pool,
            on=["birth_date"],  # Add more matching criteria as needed
            how="left",
        )
        .filter(pl.col("family_id_right").is_not_null())
        .select(
            [
                pl.col("family_id_right").alias("family_id"),
                pl.col("child_id_right").alias("child_id"),
                pl.col("index_date"),
            ]
        )
    )

    return exposed_group, matched_unexposed
