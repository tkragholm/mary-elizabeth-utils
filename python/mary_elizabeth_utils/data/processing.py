from collections.abc import Mapping

import polars as pl
from tqdm import tqdm

from ..analysis.cohort import create_cohorts
from ..config.config import Config, load_config
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
        self.config: Config = load_config(config_path)
        self.register_data: Mapping[str, pl.LazyFrame | None] = {}
        self.tables: Mapping[str, pl.LazyFrame | None] = {}

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
        total_steps = len(self.pipeline.steps)
        with tqdm(total=total_steps, desc="Overall Progress") as pbar:
            for step in self.pipeline.steps:
                step()
                pbar.update(1)

    def load_data(self) -> None:
        self.logger.info("Loading data from registers")
        self.register_data = load_all_register_data(self.config)
        self.logger.info("Data loaded successfully from all registers")

    def process_data(self) -> None:
        self.logger.info("Processing data")
        self.tables = process_all_data(dict(self.register_data))
        self.logger.info("Data processed successfully")

    def transform_data(self) -> None:
        self.logger.info("Applying data transformations")
        self.tables = transform_data(dict(self.tables), self.config)
        self.logger.info("Data transformations applied successfully")

    def validate_data(self) -> None:
        self.logger.info("Validating data")
        for name, df in tqdm(self.tables.items(), desc="Validating Data"):
            if df is not None:
                check_missing_values(df, name)
                check_outliers(df, name, self.config.NUMERIC_COLS)
                check_logical_consistency(df, name)
        self.logger.info("Data validation completed")

    def create_cohorts(self) -> None:
        self.logger.info("Creating cohorts")
        exposed_cohort, unexposed_cohort = create_cohorts(
            self.tables, self.config, self.icd10_codes
        )
        self.logger.info("Cohorts created and saved successfully")

    def generate_reports(self) -> None:
        self.logger.info("Generating reports")
        reports = [
            generate_health_report,
            generate_economic_report,
            generate_demographic_report,
            generate_integrated_analysis_report,
        ]
        for report in tqdm(reports, desc="Generating Reports"):
            report(self.tables)
        self.logger.info("Reports generated successfully")

    def analyze_data(self) -> None:
        self.logger.info("Performing data analysis")
        # Implement your specific data analysis here
        # This could include statistical tests, modeling, etc.
        self.logger.info("Data analysis completed")
