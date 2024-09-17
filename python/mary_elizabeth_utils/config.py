from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    """
    Configuration class for the Mary Elizabeth Utils package.

    Attributes:
        BASE_DIR (Path): Base directory for the project.
        DATA_DIR (Path): Directory containing the data files.
        OUTPUT_DIR (Path): Directory for output files.
        START_YEAR (int): Start year for data processing.
        END_YEAR (int): End year for data processing.
        REGISTERS (list[str]): List of registers to process.
        TABLE_NAMES (list[str]): List of table names to create.
        SEVERE_CHRONIC_CODES (list[str]): List of severe chronic disease codes.
        NUMERIC_COLS (list[str]): List of numeric columns.
        CATEGORICAL_COLS (list[str]): List of categorical columns.
    """

    BASE_DIR: Path
    DATA_DIR: Path
    OUTPUT_DIR: Path
    START_YEAR: int
    END_YEAR: int
    REGISTERS: list[str]
    TABLE_NAMES: list[str]
    SEVERE_CHRONIC_CODES: list[str]
    NUMERIC_COLS: list[str]
    CATEGORICAL_COLS: list[str]

    @classmethod
    def from_dict(cls, config_dict: dict) -> "Config":
        """
        Create a Config instance from a dictionary.

        Args:
            config_dict (dict): Dictionary containing configuration parameters.

        Returns:
            Config: An instance of the Config class.
        """
        return cls(**config_dict)
