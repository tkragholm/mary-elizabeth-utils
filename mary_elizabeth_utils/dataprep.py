import logging
from pathlib import Path

import polars as pl
from polars.datatypes import (
    DataTypeClass,
    Date,
    Float32,
    Float64,
    Int32,
    Utf8,
)

from .config import DATA_DIR, END_YEAR, OUTPUT_DIR, REGISTERS, START_YEAR

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_register_data(
    register: str, years: list[int], data_dir: str = DATA_DIR
) -> pl.LazyFrame | None:
    try:
        dfs = [
            pl.scan_parquet(Path(data_dir) / f"{register}_{year}.parquet").with_columns(
                pl.lit(year).alias("year")
            )
            for year in years
        ]
        return pl.concat(dfs)
    except Exception as e:
        logging.error(f"Error loading data for register {register}: {str(e)}")
        return None


def check_required_columns(
    df: pl.LazyFrame | None, required_columns: list[str], data_name: str
) -> None:
    if df is None:
        raise ValueError(f"{data_name} data is None")
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Missing required columns in {data_name} data: {', '.join(missing_columns)}"
        )


def create_table(
    df: pl.LazyFrame | None,
    columns: list[tuple[str, str, DataTypeClass]],
    required_columns: list[str],
    data_name: str,
) -> pl.LazyFrame | None:
    if df is None:
        return None
    check_required_columns(df, required_columns, data_name)
    return df.select([pl.col(col[0]).alias(col[1]).cast(col[2]) for col in columns])


def create_person_table(bef_data: pl.LazyFrame | None) -> pl.LazyFrame | None:
    columns: list[tuple[str, str, DataTypeClass]] = [
        ("PNR", "person_id", Utf8),
        ("FOED_DAG", "birth_date", Date),
        ("KOEN", "gender", Utf8),
        ("KOM", "municipality_code", Utf8),
        ("IE_TYPE", "origin_type", Utf8),
        ("STATSB", "citizenship", Utf8),
        ("OPR_LAND", "country_of_origin", Utf8),
        ("BOP_VFRA", "residence_start_date", Date),
        ("CIV_VFRA", "civil_status_date", Date),
        ("CIVST", "civil_status", Utf8),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(bef_data, columns, required_columns, "BEF")


def create_family_table(bef_data: pl.LazyFrame | None) -> pl.LazyFrame | None:
    columns: list[tuple[str, str, DataTypeClass]] = [
        ("FAMILIE_ID", "family_id", Utf8),
        ("FAMILIE_TYPE", "family_type", Utf8),
        ("ANTPERSF", "family_size", Int32),
        ("ANTBOERNF", "number_of_children", Int32),
        ("HUSTYPE", "household_type", Utf8),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(bef_data, columns, required_columns, "BEF")


def create_child_table(mfr_data: pl.LazyFrame | None) -> pl.LazyFrame | None:
    columns: list[tuple[str, str, DataTypeClass]] = [
        ("PNR", "child_id", Utf8),
        ("FAMILIE_ID", "family_id", Utf8),
        ("FOEDSELSDATO", "birth_date", Date),
        ("KOEN_BARN", "gender", Utf8),
        ("FOEDSELSVAEGT", "birth_weight", Float32),
        ("FOEDSELSLANGDE", "birth_length", Float32),
        ("GESTATIONSALDER", "gestational_age", Int32),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(mfr_data, columns, required_columns, "MFR")


def create_diagnosis_table(lpr_data: pl.LazyFrame | None) -> pl.LazyFrame | None:
    columns: list[tuple[str, str, DataTypeClass]] = [
        ("RECNUM", "diagnosis_id", Utf8),
        ("PNR", "person_id", Utf8),
        ("C_DIAG", "diagnosis_code", Utf8),
        ("C_DIAGTYPE", "diagnosis_type", Utf8),
        ("D_INDDTO", "diagnosis_date", Date),
        ("C_AFD", "hospital_department", Utf8),
        ("C_SGH", "hospital_code", Utf8),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(lpr_data, columns, required_columns, "LPR")


def create_employment_table(ind_data: pl.LazyFrame | None) -> pl.LazyFrame | None:
    columns: list[tuple[str, str, DataTypeClass]] = [
        ("PNR", "person_id", Utf8),
        ("ARBGNR", "employer_id", Utf8),
        ("ARBNR", "workplace_id", Utf8),
        ("STILL", "job_type", Utf8),
        ("SOCIO13", "socioeconomic_status", Utf8),
        ("year", "year", Int32),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(ind_data, columns, required_columns, "IND")


def create_education_table(uddf_data: pl.LazyFrame | None) -> pl.LazyFrame | None:
    columns: list[tuple[str, str, DataTypeClass]] = [
        ("PNR", "person_id", Utf8),
        ("HFAUDD", "education_code", Utf8),
        ("HF_VFRA", "education_start_date", Date),
        ("HF_VTIL", "education_end_date", Date),
        ("INSTNR", "institution_code", Utf8),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(uddf_data, columns, required_columns, "UDDF")


def create_healthcare_table(lpr_data: pl.LazyFrame | None) -> pl.LazyFrame | None:
    columns: list[tuple[str, str, DataTypeClass]] = [
        ("RECNUM", "event_id", Utf8),
        ("PNR", "person_id", Utf8),
        ("D_INDDTO", "admission_date", Date),
        ("D_UDDTO", "discharge_date", Date),
        ("C_PATTYPE", "patient_type", Utf8),
        ("C_KONTAARS", "contact_reason", Utf8),
        ("C_SPEC", "speciality", Utf8),
        ("V_SENGDAGE", "bed_days", Int32),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(lpr_data, columns, required_columns, "LPR")


def create_time_table(start_year: int, end_year: int) -> pl.LazyFrame:
    dates = pl.date_range(start=f"{start_year}-01-01", end=f"{end_year}-12-31", interval="1d")
    return pl.DataFrame(
        {
            "date": dates,
            "year": dates.dt.year(),
            "month": dates.dt.month(),
            "quarter": dates.dt.quarter(),
            "is_pre_treatment": pl.lit(True),
        }
    ).lazy()


def create_socioeconomic_status_table(
    ind_data: pl.LazyFrame | None, uddf_data: pl.LazyFrame | None
) -> pl.LazyFrame | None:
    if ind_data is None:
        return None
    columns: list[tuple[str, str, DataTypeClass]] = [
        ("FAMILIE_ID", "family_id", Utf8),
        ("year", "year", Int32),
        ("SOCIO13", "socioeconomic_status", Utf8),
        ("PERINDKIALT_13", "total_income", Float64),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(ind_data, columns, required_columns, "IND")


def create_treatment_period_table(
    diagnosis_df: pl.LazyFrame | None, child_df: pl.LazyFrame | None
) -> pl.LazyFrame | None:
    if diagnosis_df is None or child_df is None:
        return None
    check_required_columns(diagnosis_df, ["person_id", "diagnosis_date"], "Diagnosis")
    check_required_columns(child_df, ["child_id", "family_id"], "Child")

    return diagnosis_df.join(child_df, left_on="person_id", right_on="child_id").select(
        [
            pl.col("family_id").cast(Utf8),
            pl.col("diagnosis_date").alias("treatment_start_date").cast(Date),
            (pl.col("diagnosis_date") - pl.duration(days=365))
            .alias("pre_treatment_start")
            .cast(Date),
            pl.col("diagnosis_date").alias("pre_treatment_end").cast(Date),
            (pl.col("diagnosis_date") + pl.duration(days=365 * 5))
            .alias("post_treatment_end")
            .cast(Date),
        ]
    )


def create_person_child_table(
    bef_data: pl.LazyFrame | None, child_df: pl.LazyFrame | None
) -> pl.LazyFrame | None:
    if bef_data is None or child_df is None:
        return None
    check_required_columns(bef_data, ["PNR", "FM_MARK"], "BEF")
    check_required_columns(child_df, ["child_id"], "Child")

    return bef_data.join(child_df, left_on="PNR", right_on="child_id").select(
        [
            pl.col("PNR").alias("person_id").cast(Utf8),
            pl.col("child_id").cast(Utf8),
            pl.col("FM_MARK").alias("relationship_type").cast(Utf8),
        ]
    )


def create_person_family_table(
    bef_data: pl.LazyFrame | None, family_df: pl.LazyFrame | None
) -> pl.LazyFrame | None:
    if bef_data is None or family_df is None:
        return None
    check_required_columns(bef_data, ["PNR", "FAMILIE_ID", "PLADS"], "BEF")
    check_required_columns(family_df, ["family_id"], "Family")

    return bef_data.join(family_df, left_on="FAMILIE_ID", right_on="family_id").select(
        [
            pl.col("PNR").alias("person_id").cast(Utf8),
            pl.col("FAMILIE_ID").alias("family_id").cast(Utf8),
            pl.col("PLADS").alias("role").cast(Utf8),
        ]
    )


def create_person_year_income_table(ind_data: pl.LazyFrame | None) -> pl.LazyFrame | None:
    columns: list[tuple[str, str, DataTypeClass]] = [
        ("PNR", "person_id", Utf8),
        ("year", "year", Int32),
        ("PERINDKIALT_13", "total_income", Float64),
        ("LOENMV_13", "wage_income", Float64),
        ("ERHVERVSINDK_13", "business_income", Float64),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(ind_data, columns, required_columns, "IND")


def collect_and_integrate_data() -> None:
    try:
        register_data = {
            register: load_register_data(register, list(range(START_YEAR, END_YEAR + 1)))
            for register in REGISTERS
        }

        tables: dict[str, pl.LazyFrame | None] = {
            "Person": create_person_table(register_data.get("BEF")),
            "Family": create_family_table(register_data.get("BEF")),
            "Child": create_child_table(register_data.get("MFR")),
            "Diagnosis": create_diagnosis_table(register_data.get("LPR")),
            "Employment": create_employment_table(register_data.get("IND")),
            "Education": create_education_table(register_data.get("UDDF")),
            "Healthcare": create_healthcare_table(register_data.get("LPR")),
            "Time": create_time_table(START_YEAR, END_YEAR),
            "Socioeconomic_Status": create_socioeconomic_status_table(
                register_data.get("IND"), register_data.get("UDDF")
            ),
            "Treatment_Period": create_treatment_period_table(
                create_diagnosis_table(register_data.get("LPR")),
                create_child_table(register_data.get("MFR")),
            ),
            "Person_Child": create_person_child_table(
                register_data.get("BEF"), create_child_table(register_data.get("MFR"))
            ),
            "Person_Family": create_person_family_table(
                register_data.get("BEF"), create_family_table(register_data.get("BEF"))
            ),
            "Person_Year_Income": create_person_year_income_table(register_data.get("IND")),
        }

        output_dir = Path(OUTPUT_DIR)
        output_dir.mkdir(parents=True, exist_ok=True)

        for name, df in tables.items():
            if df is not None:
                output_path = output_dir / f"{name}.parquet"
                df.collect().write_parquet(output_path)
                logging.info(f"Successfully saved {name} table to {output_path}")
            else:
                logging.warning(f"Table {name} could not be created due to missing data")

    except Exception as e:
        logging.error(f"An error occurred during data collection and integration: {str(e)}")
        raise


if __name__ == "__main__":
    collect_and_integrate_data()
