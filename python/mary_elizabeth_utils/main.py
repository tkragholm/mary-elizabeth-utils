import logging
import os
from pathlib import Path

from .config import Config
from .data_processor import DataProcessor

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_config() -> Config:
    """
    Load configuration from environment variables or a configuration file.

    Returns:
        Config: Configuration object.
    """
    config_dict = {
        "BASE_DIR": Path(os.getenv("BASE_DIR", "/path/to/base/dir")),
        "DATA_DIR": Path(os.getenv("DATA_DIR", "/path/to/data/dir")),
        "OUTPUT_DIR": Path(os.getenv("OUTPUT_DIR", "/path/to/output/dir")),
        "START_YEAR": int(os.getenv("START_YEAR", "2000")),
        "END_YEAR": int(os.getenv("END_YEAR", "2022")),
        "REGISTERS": os.getenv("REGISTERS", "LPR,MFR,IND,BEF,UDDF").split(","),
        "TABLE_NAMES": os.getenv(
            "TABLE_NAMES",
            "Person,Family,Child,Diagnosis,Employment,Education,Healthcare,Time,Socioeconomic_Status,Treatment_Period,Person_Child,Person_Family",
        ).split(","),
        "SEVERE_CHRONIC_CODES": os.getenv(
            "SEVERE_CHRONIC_CODES", "Q20,Q21,Q22,Q23,Q24,Q25,Q26,Q27,Q28,E10,C00,C97,G40,G80"
        ).split(","),
        "NUMERIC_COLS": os.getenv("NUMERIC_COLS", "birth_date,family_size,total_income").split(","),
        "CATEGORICAL_COLS": os.getenv(
            "CATEGORICAL_COLS", "gender,origin_type,family_type,socioeconomic_status"
        ).split(","),
    }
    return Config.from_dict(config_dict)


def main() -> None:
    """Main function to run the data analysis."""
    config = load_config()
    processor = DataProcessor(config)

    try:
        processor.run()
    except Exception as e:
        logging.error(f"An error occurred during data processing: {str(e)}")


if __name__ == "__main__":
    main()
