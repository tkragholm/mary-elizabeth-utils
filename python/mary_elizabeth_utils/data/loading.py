import csv
import logging
from pathlib import Path
from typing import Any

import polars as pl
from polars import Date, Float32, Float64, Int32, Utf8

from ..config.config import Config, RegisterConfig

logger = logging.getLogger(__name__)


def load_all_register_data(config: Config) -> dict[str, pl.LazyFrame]:
    register_data = {}
    for register, register_config in config.REGISTERS.items():
        years = list(range(config.START_YEAR, config.END_YEAR + 1))
        register_data[register] = load_register_data(
            register, years, register_config, config.BASE_DIR
        )
    return register_data


def process_all_data(register_data: dict[str, pl.LazyFrame]) -> dict[str, pl.LazyFrame]:
    tables = {}

    # Process health data
    lpr_diag = register_data.get("LPR_DIAG")
    if lpr_diag is not None:
        tables["Diagnosis"] = create_diagnosis_table(lpr_diag)

    mfr = register_data.get("MFR")
    if mfr is not None:
        tables["Birth"] = create_child_table(mfr)

    lmdb = register_data.get("LMDB")
    if lmdb is not None:
        tables["Medication"] = create_medication_table(lmdb)

    # Process economic data
    ind = register_data.get("IND")
    if ind is not None:
        tables["Income"] = create_person_year_income_table(ind)

    akm = register_data.get("AKM")
    if akm is not None:
        tables["Employment"] = create_employment_table(akm)

    # Process demographic data
    bef = register_data.get("BEF")
    if bef is not None:
        tables["Person"] = create_person_table(bef)

    uddf = register_data.get("UDDF")
    if uddf is not None:
        tables["Education"] = create_education_table(uddf)

    return tables  # type: ignore


def load_icd10_codes(config: Config) -> dict[str, str]:
    icd10_codes = {}
    file_path = config.ICD10_CODES_FILE
    logger.debug(f"Loading ICD10 codes from: {file_path}")
    with open(file_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            codes = row["ICD10-codes"].split(";")
            for code in codes:
                code = code.strip()
                if "-" in code:
                    start, end = code.split("-")
                    icd10_codes[start] = row["Diagnoses"]
                    icd10_codes[end] = row["Diagnoses"]
                else:
                    icd10_codes[code] = row["Diagnoses"]
    return icd10_codes


def check_required_columns(
    df: pl.LazyFrame | None, required_columns: list[str], data_name: str
) -> None:
    """
    Check if the DataFrame contains all required columns.

    Args:
        df (Optional[pl.LazyFrame]): The DataFrame to check.
        required_columns (List[str]): List of required column names.
        data_name (str): Name of the data source for logging purposes.

    Raises:
        ValueError: If df is None or if any required columns are missing.
    """
    if df is None:
        raise ValueError(f"{data_name} data is None")
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Missing required columns in {data_name} data: {', '.join(missing_columns)}"
        )


def load_register_data(
    register: str, years: list[int], register_config: RegisterConfig, base_dir: Path
) -> pl.LazyFrame:
    try:
        dfs = []
        for year in years:
            file_path = register_config.get_file_path(year, base_dir)
            logger.debug(f"Attempting to load file for {register}, year {year}: {file_path}")

            if not file_path.exists():
                raise FileNotFoundError(
                    f"Data file not found for {register}, year {year}: {file_path}"
                )

            logger.info(f"Loading file: {file_path}")

            if file_path.suffix.lower() == ".parquet":
                df = pl.scan_parquet(file_path)
            elif file_path.suffix.lower() == ".csv":
                df = pl.scan_csv(file_path)
            else:
                raise ValueError(
                    f"Unsupported file format for {register}, year {year}: {file_path}"
                )

            df = df.with_columns(pl.lit(year).alias("year"))
            dfs.append(df)

        return pl.concat(dfs)
    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
        raise
    except ValueError as e:
        logger.error(f"Unsupported file format: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error loading data for register {register}: {str(e)}")
        raise


def create_table(
    df: pl.LazyFrame | None,
    columns: list[tuple[str, str, Any]],
    required_columns: list[str],
    data_name: str,
) -> pl.LazyFrame | None:
    """
    Create a table with specified columns from the input DataFrame.

    Args:
        df (Optional[pl.LazyFrame]): Input DataFrame.
        columns (List[Tuple[str, str, pl.DataType]]): List of (original_name, new_name, data_type) tuples.
        required_columns (List[str]): List of required column names.
        data_name (str): Name of the data source for logging purposes.

    Returns:
        Optional[pl.LazyFrame]: Created table, or None if input is None.

    Raises:
        ValueError: If required columns are missing.
    """
    if df is None:
        return None
    check_required_columns(df, required_columns, data_name)
    return df.select([pl.col(col[0]).alias(col[1]).cast(col[2]) for col in columns])


def create_person_table(bef_data: pl.LazyFrame | None) -> pl.LazyFrame | None:
    columns: list[tuple[str, str, Any]] = [
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
    columns: list[tuple[str, str, Any]] = [
        ("FAMILIE_ID", "family_id", Utf8),
        ("FAMILIE_TYPE", "family_type", Utf8),
        ("ANTPERSF", "family_size", Int32),
        ("ANTBOERNF", "number_of_children", Int32),
        ("HUSTYPE", "household_type", Utf8),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(bef_data, columns, required_columns, "BEF")


def create_child_table(mfr_data: pl.LazyFrame | None) -> pl.LazyFrame | None:
    columns: list[tuple[str, str, Any]] = [
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
    columns: list[tuple[str, str, Any]] = [
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
    columns: list[tuple[str, str, Any]] = [
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
    columns: list[tuple[str, str, Any]] = [
        ("PNR", "person_id", Utf8),
        ("HFAUDD", "education_code", Utf8),
        ("HF_VFRA", "education_start_date", Date),
        ("HF_VTIL", "education_end_date", Date),
        ("INSTNR", "institution_code", Utf8),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(uddf_data, columns, required_columns, "UDDF")


def create_medication_table(lmdb_data: pl.LazyFrame | None) -> pl.LazyFrame | None:
    columns: list[tuple[str, str, Any]] = [
        ("PNR12", "person_id", Utf8),
        ("ATC", "atc_code", Utf8),
        ("EKSD", "prescription_date", Date),
        ("VOLUMEN", "volume", Float32),
        ("STYRKE", "strength", Float32),
        ("PAKSTR", "package_size", Int32),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(lmdb_data, columns, required_columns, "LMDB")


def create_healthcare_table(lpr_data: pl.LazyFrame | None) -> pl.LazyFrame | None:
    columns: list[tuple[str, str, Any]] = [
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
    """
    Create a time dimension table.

    Args:
        start_year (int): Start year of the time range.
        end_year (int): End year of the time range.

    Returns:
        pl.LazyFrame: Time dimension table.
    """
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
    columns: list[tuple[str, str, Any]] = [
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
    columns: list[tuple[str, str, Any]] = [
        ("PNR", "person_id", Utf8),
        ("year", "year", Int32),
        ("PERINDKIALT_13", "total_income", Float64),
        ("LOENMV_13", "wage_income", Float64),
        ("ERHVERVSINDK_13", "business_income", Float64),
    ]
    required_columns = [col[0] for col in columns]
    return create_table(ind_data, columns, required_columns, "IND")
