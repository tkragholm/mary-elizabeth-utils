import re
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator


class RegisterConfig(BaseModel):
    file_pattern: str
    location: str
    years: list[int] | None = None
    include_month: bool = False
    combined_year_month: bool = False

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def get_file_path(self, year: int, month: int | str | None, base_dir: Path) -> Path:
        location = Path(self.location)
        if not location.is_absolute():
            location = base_dir / location

        if self.include_month:
            if self.combined_year_month:
                return location / self.file_pattern.format(
                    yearmonth=f"{year}{'*' if month == '*' else f'{month:02d}'}"
                )
            else:
                return location / self.file_pattern.format(year=year, month=month)
        else:
            return location / self.file_pattern.format(year=year)


class Config(BaseModel):
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

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("END_YEAR")
    def end_year_must_be_after_start_year(cls, v: int, info: ValidationInfo) -> int:  # noqa: N805
        if "START_YEAR" in info.data and v <= info.data["START_YEAR"]:
            raise ValueError("END_YEAR must be after START_YEAR")
        return v

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> "Config":
        variables = config_dict.get("variables", {})
        resolved_config = cls.resolve_variables(config_dict, variables)
        return cls.model_validate(resolved_config)

    @staticmethod
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

        return cast(dict[str, Any], process_value(config))


def load_config(config_path: str) -> Config:
    import yaml

    with open(config_path) as file:
        config_dict = yaml.safe_load(file)

    # Convert paths to Path objects
    for key in ["base_dir", "output_dir", "csv_dir", "parquet_dir", "icd10_codes_file"]:
        if key in config_dict["variables"]:
            config_dict["variables"][key] = Path(config_dict["variables"][key])

    # Create the Config object
    config = Config.from_dict(config_dict)

    # Set the path fields
    config.BASE_DIR = Path(config_dict["variables"]["base_dir"])
    config.OUTPUT_DIR = Path(config_dict["variables"]["output_dir"])
    config.CSV_DIR = Path(config_dict["variables"]["csv_dir"])
    config.PARQUET_DIR = Path(config_dict["variables"]["parquet_dir"])
    config.ICD10_CODES_FILE = Path(config_dict["icd10_codes_file"])

    return config
