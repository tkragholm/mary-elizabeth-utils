import logging
import os
from pathlib import Path

import polars as pl

from .config import Config
from .data_analysis import (
    generate_summary_statistics,
    plot_categorical_comparisons,
    plot_numeric_comparisons,
)
from .data_loading import create_table, load_register_data
from .data_transformation import apply_transformations, impute_missing_values
from .data_validation import check_missing_values, check_outliers


class DataProcessor:
    """
    A class to process and analyze register data.

    This class handles loading, validating, transforming, and analyzing data
    from various registers based on the provided configuration.
    """

    def __init__(self, config: Config):
        """
        Initialize the DataProcessor with the given configuration.

        Args:
            config (Config): Configuration object containing processing parameters.
        """
        self.config = config
        self.register_data: dict[str, pl.LazyFrame | None] = {}
        self.tables: dict[str, pl.LazyFrame | None] = {}
        self.logger = logging.getLogger(__name__)

    def load_data(self) -> None:
        """
        Load data from registers specified in the configuration.

        This method populates the register_data dictionary with LazyFrames
        for each register.
        """
        try:
            self.register_data = {
                register: load_register_data(
                    register,
                    list(range(self.config.START_YEAR, self.config.END_YEAR + 1)),
                    self.config.DATA_DIR,
                )
                for register in self.config.REGISTERS
            }
            self.logger.info("Data loaded successfully from all registers.")
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise

    def create_tables(self) -> None:
        """
        Create tables from the loaded register data.

        This method should implement the logic to create various tables
        based on the loaded register data.
        """
        # Implement table creation logic here
        # For example:
        try:
            self.tables["person"] = create_table(
                self.register_data.get("BEF"),
                [("PNR", "person_id", pl.Utf8), ("FOED_DAG", "birth_date", pl.Date)],
                ["PNR", "FOED_DAG"],
                "Person",
            )
            # Add more table creations as needed
            self.logger.info("Tables created successfully.")
        except Exception as e:
            self.logger.error(f"Error creating tables: {str(e)}")
            raise

    def validate_data(self) -> None:
        """
        Validate the data in created tables.

        This method checks for missing values, outliers, and logical consistency
        in the created tables.
        """
        for name, df in self.tables.items():
            if df is not None:
                try:
                    check_missing_values(df, name)
                    check_outliers(df, name, self.config.NUMERIC_COLS)
                    # Implement logical consistency checks
                    self.logger.info(f"Data validation completed for table: {name}")
                except Exception as e:
                    self.logger.error(f"Error validating data for table {name}: {str(e)}")

    def transform_data(self) -> None:
        """
        Apply transformations to the data.

        This method should implement data transformation logic, such as
        imputing missing values and applying custom transformations.
        """
        # Implement data transformation logic here
        # For example:
        try:
            for name, df in self.tables.items():
                if df is not None:
                    imputation_strategy = {"numeric_col": "mean", "categorical_col": "mode"}
                    df = impute_missing_values(df, imputation_strategy)
                    custom_transformations = {"age": lambda x: 2023 - x}
                    df = apply_transformations(df, custom_transformations)
                    self.tables[name] = df
            self.logger.info("Data transformations applied successfully.")
        except Exception as e:
            self.logger.error(f"Error transforming data: {str(e)}")
            raise

    def analyze_data(self) -> None:
        """Perform data analysis on all tables."""
        try:
            for name, df in self.tables.items():
                if df is not None:
                    self.logger.info(f"Analyzing table: {name}")

                    # Create a subdirectory for each table's output
                    table_output_dir = Path(self.config.OUTPUT_DIR) / name
                    os.makedirs(table_output_dir, exist_ok=True)

                    # Generate summary statistics
                    summary_stats = generate_summary_statistics(
                        df, "group_column", self.config.NUMERIC_COLS, self.config.CATEGORICAL_COLS
                    )

                    # Save summary statistics
                    summary_stats_path = table_output_dir / f"{name}_summary_stats.csv"
                    summary_stats.write_csv(summary_stats_path)
                    self.logger.info(f"Summary statistics saved to {summary_stats_path}")

                    # Generate plots
                    plot_numeric_comparisons(
                        df.collect(), "group_column", self.config.NUMERIC_COLS, table_output_dir
                    )
                    self.logger.info(f"Numeric comparisons plot saved in {table_output_dir}")

                    plot_categorical_comparisons(
                        df.collect(), "group_column", self.config.CATEGORICAL_COLS, table_output_dir
                    )
                    self.logger.info(f"Categorical comparisons plot saved in {table_output_dir}")

            self.logger.info("Data analysis completed successfully.")
        except Exception as e:
            self.logger.error(f"Error analyzing data: {str(e)}")
            raise

    def run(self) -> None:
        """
        Run the entire data processing pipeline.

        This method executes all steps of the data processing pipeline in sequence.
        """
        try:
            self.load_data()
            self.create_tables()
            self.validate_data()
            self.transform_data()
            self.analyze_data()
            self.logger.info("Data processing pipeline completed successfully.")
        except Exception as e:
            self.logger.error(f"Error in data processing pipeline: {str(e)}")
            raise
