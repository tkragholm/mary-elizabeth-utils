import re
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel, validator


class RegisterConfig(BaseModel):
    file_pattern: str
    location: str
    years: list[int] | None | None = None
    include_month: bool = False
    combined_year_month: bool = False

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

    @validator("END_YEAR")
    def end_year_must_be_after_start_year(self, v: int, values: dict[str, Any]) -> int:
        if "START_YEAR" in values and v <= values["START_YEAR"]:
            raise ValueError("END_YEAR must be after START_YEAR")
        return v

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> "Config":
        variables = config_dict.get("variables", {})
        resolved_config = cls.resolve_variables(config_dict, variables)
        return cls(**resolved_config)

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

    class ConfigMeta:
        arbitrary_types_allowed = True

    Config = ConfigMeta
