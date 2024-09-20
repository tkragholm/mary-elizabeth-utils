import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def resolve_variables(config: dict[str, Any], variables: dict[str, str]) -> dict[str, Any]:
    def replace_var(match: re.Match[str]) -> str:
        var_name = match.group(1)
        return variables.get(var_name, match.group(0))

    def process_value(value: Any) -> Any:
        if isinstance(value, str):
            return re.sub(r"\$\{(\w+)\}", replace_var, value)
        elif isinstance(value, dict):
            return {k: process_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [process_value(item) for item in value]
        return value

    return process_value(config)  # type: ignore


@dataclass
class RegisterConfig:
    file_pattern: str
    location: str

    def get_file_path(self, year: int, base_dir: Path) -> Path:
        location = Path(self.location)
        if not location.is_absolute():
            location = base_dir / location
        return location / self.file_pattern.format(year=year)


@dataclass
class Config:
    BASE_DIR: Path
    OUTPUT_DIR: Path
    CSV_DIR: Path
    PARQUET_DIR: Path
    START_YEAR: int
    END_YEAR: int
    REGISTERS: dict[str, RegisterConfig]
    TABLE_NAMES: list[str]
    SEVERE_CHRONIC_CODES: list[str]
    NUMERIC_COLS: list[str]
    CATEGORICAL_COLS: list[str]
    ICD10_CODES_FILE: Path

    @classmethod
    def from_dict(cls, config_dict: dict) -> "Config":
        variables = config_dict.get("variables", {})
        resolved_config = resolve_variables(config_dict, variables)

        base_dir = Path(resolved_config["variables"]["base_dir"])
        output_dir = Path(resolved_config["variables"]["output_dir"])
        csv_dir = Path(resolved_config["variables"]["csv_dir"])
        parquet_dir = Path(resolved_config["variables"]["parquet_dir"])
        icd10_codes_file = Path(resolved_config["icd10_codes_file"])

        registers = {
            name: RegisterConfig(**reg_config)
            for name, reg_config in resolved_config["registers"].items()
        }

        return cls(
            BASE_DIR=base_dir,
            OUTPUT_DIR=output_dir,
            ICD10_CODES_FILE=icd10_codes_file,
            CSV_DIR=csv_dir,
            PARQUET_DIR=parquet_dir,
            START_YEAR=resolved_config["start_year"],
            END_YEAR=resolved_config["end_year"],
            REGISTERS=registers,
            TABLE_NAMES=resolved_config["table_names"],
            SEVERE_CHRONIC_CODES=resolved_config.get("severe_chronic_codes", []),
            NUMERIC_COLS=resolved_config.get("numeric_cols", []),
            CATEGORICAL_COLS=resolved_config.get("categorical_cols", []),
        )
