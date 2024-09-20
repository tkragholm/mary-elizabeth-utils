import polars as pl
import yaml  # type: ignore

from ..analysis.cohort import create_cohorts
from ..config.config import Config
from ..data.loading import load_all_register_data, load_icd10_codes, process_all_data
from ..data.transformation import transform_data
from ..data.validation import check_logical_consistency, check_missing_values, check_outliers
from ..utils.logger import setup_colored_logger
from ..utils.pipeline import Pipeline
from ..utils.reports import (
    generate_demographic_report,
    generate_economic_report,
    generate_health_report,
    generate_integrated_analysis_report,
)


class DataProcessor:
    def __init__(self, config_path: str):
        with open(config_path) as f:
            config_dict = yaml.safe_load(f)
        self.config = Config.from_dict(config_dict)
        self.register_data: dict[str, pl.LazyFrame] = {}
        self.tables: dict[str, pl.LazyFrame] = {}
        self.pipeline = Pipeline()
        self.icd10_codes = load_icd10_codes(self.config)
        self.logger = setup_colored_logger(__name__)
        self._setup_pipeline()

    def _setup_pipeline(self) -> None:
        self.pipeline.add_step(self.load_data)
        self.pipeline.add_step(self.process_data)
        self.pipeline.add_step(self.validate_data)
        self.pipeline.add_step(self.transform_data)
        self.pipeline.add_step(self.create_cohorts)
        self.pipeline.add_step(self.generate_reports)
        self.pipeline.add_step(self.analyze_data)

    def run(self) -> None:
        self.pipeline.run()

    def load_data(self) -> None:
        self.logger.info("Loading data from registers")
        self.register_data = load_all_register_data(self.config)
        self.logger.info("Data loaded successfully from all registers")

    def process_data(self) -> None:
        self.logger.info("Processing data")
        self.tables = process_all_data(self.register_data)
        self.logger.info("Data processed successfully")

    def validate_data(self) -> None:
        self.logger.info("Validating data")
        for name, df in self.tables.items():
            if df is not None:
                check_missing_values(df, name)
                check_outliers(df, name, self.config.NUMERIC_COLS)
                check_logical_consistency(df, name)
        self.logger.info("Data validation completed")

    def transform_data(self) -> None:
        self.logger.info("Applying data transformations")
        self.tables = transform_data(self.tables, self.config)
        self.logger.info("Data transformations applied successfully")

    def create_cohorts(self) -> None:
        self.logger.info("Creating cohorts")
        exposed_cohort, unexposed_cohort = create_cohorts(
            self.tables, self.config, self.icd10_codes
        )
        self.logger.info("Cohorts created and saved successfully")

    def generate_reports(self) -> None:
        self.logger.info("Generating reports")
        generate_health_report(self.tables)
        generate_economic_report(self.tables)
        generate_demographic_report(self.tables)
        generate_integrated_analysis_report(self.tables)
        self.logger.info("Reports generated successfully")

    def analyze_data(self) -> None:
        self.logger.info("Performing data analysis")
        # Implement your specific data analysis here
        # This could include statistical tests, modeling, etc.
        self.logger.info("Data analysis completed")
