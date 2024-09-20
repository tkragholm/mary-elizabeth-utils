from pathlib import Path

from mary_elizabeth_utils.analysis.cohort import create_cohorts
from mary_elizabeth_utils.config.config import Config, RegisterConfig
from mary_elizabeth_utils.data.loading import (
    load_all_register_data,
    load_icd10_codes,
    process_all_data,
)
from mary_elizabeth_utils.data.transformation import transform_data
from mary_elizabeth_utils.data.validation import (
    check_logical_consistency,
    check_missing_values,
    check_outliers,
)
from mary_elizabeth_utils.utils.reports import (
    generate_demographic_report,
    generate_economic_report,
    generate_health_report,
    generate_integrated_analysis_report,
)

# Define base paths
base_path = Path(r"\\e")
workdata_path = base_path / "dir" / "dir" / "dir" / "dir" / "dir"
data_path = base_path / "dir"

# Manually create the Config object
config = Config(
    BASE_DIR=workdata_path,
    OUTPUT_DIR=workdata_path / "output",
    CSV_DIR=data_path,
    PARQUET_DIR=base_path,
    START_YEAR=2000,
    END_YEAR=2022,
    REGISTERS={
        "LPR_DIAG": RegisterConfig(
            file_pattern="lpr_diag_{year}.parquet", location=str(data_path / "lpr_diag")
        ),
        "MFR": RegisterConfig(file_pattern="mfr_{year}.parquet", location=str(data_path / "mfr")),
        "AKM": RegisterConfig(file_pattern="akm_{year}.parquet", location=str(base_path / "akm")),
        "FAIK": RegisterConfig(
            file_pattern="faik_{year}.parquet", location=str(data_path / "faik")
        ),
        "IND": RegisterConfig(file_pattern="ind_{year}.parquet", location=str(data_path / "ind")),
        "BEF": RegisterConfig(file_pattern="bef_{year}.parquet", location=str(base_path / "bef")),
        "UDDF": RegisterConfig(
            file_pattern="uddf_{year}.parquet", location=str(data_path / "uddf")
        ),
    },
    TABLE_NAMES=[
        "Person",
        "Family",
        "Child",
        "Diagnosis",
        "Medication",
        "Healthcare",
        "Employment",
        "Income",
        "Education",
        "Death",
        "Birth",
        "SocioEconomic",
        "Psychiatric",
    ],
    SEVERE_CHRONIC_CODES=[],  # Add codes if needed
    NUMERIC_COLS=[
        "birth_date",
        "family_size",
        "total_income",
        "VAEGT_BARN",
        "LAENGDE_BARN",
        "GESTATIONSALDER_DAGE",
        "APGARSCORE_EFTER5MINUTTER",
        "VMO_A_INDK_AM_BIDRAG_BETAL",
        "LOENMV_13",
        "PERINDKIALT_13",
    ],
    CATEGORICAL_COLS=[
        "gender",
        "origin_type",
        "family_type",
        "socioeconomic_status",
        "KOEN",
        "IE_TYPE",
        "CIVST",
        "SOCSTIL_KODE",
        "SAGSART",
        "C_PATTYPE",
    ],
    ICD10_CODES_FILE=workdata_path / "data" / "icd10.csv",
)

# Now you can use this config object to run your functionality

# Load register data
register_data = load_all_register_data(config)

# Process the data
processed_data = process_all_data(register_data)

# Load ICD10 codes
icd10_codes = load_icd10_codes(config)

# Transform data
transformed_data = transform_data(processed_data, config)

# Validate data
for name, df in transformed_data.items():
    if df is not None:
        check_missing_values(df, name)
        check_outliers(df, name, config.NUMERIC_COLS)
        check_logical_consistency(df, name)

# Create cohorts
exposed_cohort, unexposed_cohort = create_cohorts(transformed_data, config, icd10_codes)

# Generate reports
generate_health_report(transformed_data)
generate_economic_report(transformed_data)
generate_demographic_report(transformed_data)
generate_integrated_analysis_report(transformed_data)
