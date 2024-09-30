import csv
import logging
from collections.abc import Mapping
from pathlib import Path

import polars as pl

from ..config.config import Config, RegisterConfig
from ..data.table_creation import (
    DiagnosisData,
    create_child_table,
    create_diagnosis_table,
    create_education_table,
    create_employment_table,
    create_healthcare_table,
    create_medication_table,
    create_person_table,
    create_person_year_income_table,
)

logger = logging.getLogger(__name__)


def load_all_register_data(config: Config) -> Mapping[str, pl.LazyFrame | None]:
    register_data: dict[str, pl.LazyFrame | None] = {}
    for register, register_config in config.REGISTERS.items():
        years = list(range(config.START_YEAR, config.END_YEAR + 1))
        register_data[register] = load_register_data(
            register, years, register_config, config.BASE_DIR
        )
    return register_data


def process_all_data(
    register_data: Mapping[str, pl.LazyFrame | None],
) -> Mapping[str, pl.LazyFrame | None]:
    tables: dict[str, pl.LazyFrame | None] = {}

    # Process health data
    lpr_diag = register_data.get("LPR_DIAG")
    lpr_adm = register_data.get("LPR_ADM")
    priv_diag = register_data.get("PRIV_DIAG")
    priv_adm = register_data.get("PRIV_ADM")
    psyk_diag = register_data.get("PSYK_DIAG")
    psyk_adm = register_data.get("PSYK_ADM")
    lpr_sksopr = register_data.get("LPR_SKSOPR")
    priv_sksopr = register_data.get("PRIV_SKSOPR")

    diagnosis_data = DiagnosisData(
        lpr_diag=lpr_diag,
        lpr_adm=lpr_adm,
        priv_diag=priv_diag,
        priv_adm=priv_adm,
        psyk_diag=psyk_diag,
        psyk_adm=psyk_adm,
    )

    tables["Diagnosis"] = create_diagnosis_table(diagnosis_data)
    tables["Healthcare"] = create_healthcare_table(
        lpr_adm, priv_adm, psyk_adm, lpr_sksopr, priv_sksopr
    )

    mfr = register_data.get("MFR")
    if mfr is not None:
        tables["Birth"] = create_child_table(mfr)

    lmdb = register_data.get("LMDB")
    if lmdb is not None:
        tables["Medication"] = create_medication_table(lmdb)

    # Process economic data
    ind = register_data.get("IND")
    idan = register_data.get("IDAN")
    akm = register_data.get("AKM")
    if ind is not None and idan is not None and akm is not None:
        tables["Employment"] = create_employment_table(ind, idan, akm)

    if ind is not None:
        tables["Income"] = create_person_year_income_table(ind)

    # Process demographic data
    bef = register_data.get("BEF")
    dod = register_data.get("DOD")
    dodsaars = register_data.get("DODSAARS")
    dodsaasg = register_data.get("DODSAASG")
    if bef is not None:
        tables["Person"] = create_person_table(bef, dod, dodsaars, dodsaasg)

    uddf = register_data.get("UDDF")
    if uddf is not None:
        tables["Education"] = create_education_table(uddf)

    return tables


def load_icd10_codes(config: Config) -> dict[str, str]:
    icd10_codes = {}
    file_path = config.ICD10_CODES_FILE
    logger.debug(f"Loading ICD10 codes from: {file_path}")
    with open(file_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            codes = row["ICD10-codes"].split(";")
            for code_item in codes:
                code = code_item.strip()
                if "-" in code:
                    start, end = code.split("-")
                    icd10_codes[start] = row["Diagnoses"]
                    icd10_codes[end] = row["Diagnoses"]
                else:
                    icd10_codes[code] = row["Diagnoses"]
    return icd10_codes


def load_register_data(
    register: str, years: list[int], register_config: RegisterConfig, base_dir: Path
) -> pl.LazyFrame:
    try:
        dfs = []
        # Check if the register has specific years defined
        years_to_load = register_config.years or years

        for year in years_to_load:
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
        logger.error(f"File not found: {e!s}")
        raise
    except ValueError as e:
        logger.error(f"Unsupported file format: {e!s}")
        raise
    except Exception as e:
        logger.error(f"Error loading data for register {register}: {e!s}")
        raise
