import csv
from pathlib import Path

import polars as pl
import yaml  # type: ignore

from .caching import cache_result
from .colored_logger import setup_colored_logger
from .pipeline import Pipeline


class DataProcessor:
    def __init__(self, config_path: str):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.register_data: dict[str, pl.LazyFrame] = {}
        self.tables: dict[str, pl.LazyFrame] = {}
        self.pipeline = Pipeline()
        self.icd10_codes = self.load_icd10_codes()
        self.logger = setup_colored_logger(__name__)
        self._setup_pipeline()

    def load_icd10_codes(self) -> dict[str, str]:
        icd10_codes = {}
        file_path = Path(self.config["icd10_codes_file"])
        with open(file_path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                codes = row["ICD10-codes"].split(";")
                for code in codes:
                    code = code.strip()
                    if "-" in code:
                        start, end = code.split("-")
                        icd10_codes[start] = row["Diagnoses"]
                        icd10_codes[end] = row["Diagnoses"]
                    else:
                        icd10_codes[code] = row["Diagnoses"]
        return icd10_codes

    def _setup_pipeline(self) -> None:
        self.pipeline.add_step(self.load_data)
        self.pipeline.add_step(self.process_health_data)
        self.pipeline.add_step(self.process_economic_data)
        self.pipeline.add_step(self.process_demographic_data)
        self.pipeline.add_step(self.validate_data)
        self.pipeline.add_step(self.transform_data)
        self.pipeline.add_step(self.create_cohorts)
        self.pipeline.add_step(self.generate_reports)
        self.pipeline.add_step(self.analyze_data)

    def run(self) -> None:
        self.pipeline.run()

    def load_data(self) -> None:
        self.logger.info("Loading data from registers")
        for register, config in self.config["registers"].items():
            years = range(self.config["start_year"], self.config["end_year"] + 1)
            self.register_data[register] = self._load_register_data(register, years, config)
        self.logger.info("Data loaded successfully from all registers")

    def _load_register_data(self, register: str, years: range, config: dict) -> pl.LazyFrame:
        dfs = []
        for year in years:
            file_path = Path(config["location"]) / config["file_pattern"].format(year=year)
            if file_path.exists():
                df = pl.scan_parquet(file_path).with_columns(pl.lit(year).alias("year"))
                dfs.append(df)
            else:
                self.logger.warning(
                    f"File not found for register {register}, year {year}: {file_path}"
                )
        return pl.concat(dfs)

    def process_health_data(self) -> None:
        self.logger.info("Processing health data")
        lpr_diag = self.register_data.get("LPR_DIAG")
        if lpr_diag is not None:
            self.tables["Diagnosis"] = lpr_diag.filter(
                pl.col("C_DIAG").is_in(self.icd10_codes.keys())
            ).select(
                [
                    pl.col("PNR").alias("person_id"),
                    pl.col("C_DIAG").alias("diagnosis_code"),
                    pl.col("D_INDDTO").alias("diagnosis_date"),
                ]
            )

        mfr = self.register_data.get("MFR")
        if mfr is not None:
            self.tables["Birth"] = mfr.select(
                [
                    pl.col("CPR_BARN").alias("child_id"),
                    pl.col("FOEDSELSDATO").alias("birth_date"),
                    pl.col("GESTATIONSALDER_DAGE").alias("gestational_age"),
                    pl.col("VAEGT_BARN").alias("birth_weight"),
                ]
            )

        lmdb = self.register_data.get("LMDB")
        if lmdb is not None:
            self.tables["Medication"] = lmdb.select(
                [
                    pl.col("PNR12").alias("person_id"),
                    pl.col("ATC").alias("atc_code"),
                    pl.col("EKSD").alias("prescription_date"),
                ]
            )
        self.logger.info("Health data processed successfully")

    def process_economic_data(self) -> None:
        self.logger.info("Processing economic data")
        ind = self.register_data.get("IND")
        if ind is not None:
            self.tables["Income"] = ind.select(
                [
                    pl.col("PNR").alias("person_id"),
                    pl.col("PERINDKIALT_13").alias("total_income"),
                    pl.col("LOENMV_13").alias("wage_income"),
                ]
            )

        akm = self.register_data.get("AKM")
        if akm is not None:
            self.tables["Employment"] = akm.select(
                [
                    pl.col("PNR").alias("person_id"),
                    pl.col("SOCIO13").alias("socioeconomic_status"),
                ]
            )
        self.logger.info("Economic data processed successfully")

    def process_demographic_data(self) -> None:
        self.logger.info("Processing demographic data")
        bef = self.register_data.get("BEF")
        if bef is not None:
            self.tables["Person"] = bef.select(
                [
                    pl.col("PNR").alias("person_id"),
                    pl.col("KOEN").alias("gender"),
                    pl.col("FOED_DAG").alias("birth_date"),
                    pl.col("IE_TYPE").alias("origin_type"),
                ]
            )

        uddf = self.register_data.get("UDDF")
        if uddf is not None:
            self.tables["Education"] = uddf.select(
                [
                    pl.col("PNR").alias("person_id"),
                    pl.col("HFAUDD").alias("education_code"),
                    pl.col("HF_VFRA").alias("education_start_date"),
                ]
            )
        self.logger.info("Demographic data processed successfully")

    def validate_data(self) -> None:
        self.logger.info("Validating data")
        for name, df in self.tables.items():
            if df is not None:
                self._check_missing_values(df, name)
                self._check_outliers(df, name)
                self._check_logical_consistency(df, name)
        self.logger.info("Data validation completed")

    def _check_missing_values(self, df: pl.LazyFrame, table_name: str) -> None:
        missing_counts = df.select([pl.col(col).null_count() for col in df.columns]).collect()
        for col, count in zip(df.columns, missing_counts.row(0), strict=False):
            if count > 0:
                self.logger.warning(f"Missing values in {table_name}.{col}: {count}")

    def _check_outliers(self, df: pl.LazyFrame, table_name: str) -> None:
        numeric_cols = [col for col in df.columns if col in self.config["numeric_cols"]]
        for col in numeric_cols:
            q1, q3 = (
                df.select(
                    [pl.col(col).quantile(0.25).alias("q1"), pl.col(col).quantile(0.75).alias("q3")]
                )
                .collect()
                .row(0)
            )
            iqr = q3 - q1
            lower_bound, upper_bound = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            outliers = df.filter((pl.col(col) < lower_bound) | (pl.col(col) > upper_bound))
            outlier_count = outliers.select(pl.count()).collect().item()
            if outlier_count > 0:
                self.logger.warning(f"Outliers detected in {table_name}.{col}: {outlier_count}")

    def _check_logical_consistency(self, df: pl.LazyFrame, table_name: str) -> None:
        # Implement logical consistency checks here
        # Example:
        if table_name == "Person":
            invalid_ages = df.filter(pl.col("birth_date") > pl.date(2023, 1, 1))
            invalid_count = invalid_ages.select(pl.count()).collect().item()
            if invalid_count > 0:
                self.logger.warning(
                    f"Invalid birth dates detected in {table_name}: {invalid_count}"
                )

    def transform_data(self) -> None:
        self.logger.info("Applying data transformations")
        for name, df in self.tables.items():
            if df is not None:
                df = self._impute_missing_values(df)
                df = self._apply_custom_transformations(df)
                self.tables[name] = df
        self.logger.info("Data transformations applied successfully")

    def _impute_missing_values(self, df: pl.LazyFrame) -> pl.LazyFrame:
        for col in df.columns:
            if col in self.config["numeric_cols"]:
                df = df.with_columns(pl.col(col).fill_null(pl.col(col).mean()))
            elif col in self.config["categorical_cols"]:
                df = df.with_columns(pl.col(col).fill_null(pl.col(col).mode()))
        return df

    def _apply_custom_transformations(self, df: pl.LazyFrame) -> pl.LazyFrame:
        # Implement custom transformations here
        # Example:
        # if "birth_date" in df.columns:
        #     df = df.with_columns([
        #         ((pl.date(2023, 1, 1) - pl.col("birth_date")).dt.days() / 365.25).alias("age")
        #     ])
        return df

    @cache_result("cache")
    def create_cohorts(self) -> None:
        self.logger.info("Creating cohorts")
        severe_chronic_cases = self._identify_severe_chronic_cases()
        exposed_group = self._create_exposed_group(severe_chronic_cases)
        unexposed_pool = self._create_unexposed_group()
        exposed_cohort, unexposed_cohort = self._match_cohorts(exposed_group, unexposed_pool)

        output_dir = Path(self.config["output_dir"])
        exposed_cohort.collect().write_parquet(output_dir / "exposed_cohort.parquet")
        unexposed_cohort.collect().write_parquet(output_dir / "unexposed_cohort.parquet")
        self.logger.info("Cohorts created and saved successfully")

    @cache_result("cache")
    def _identify_severe_chronic_cases(self) -> pl.LazyFrame:
        diagnosis_df = self.tables.get("Diagnosis")
        if diagnosis_df is None:
            raise ValueError("Diagnosis table not found")

        return diagnosis_df.filter(
            (pl.col("diagnosis_code").is_in(self.icd10_codes.keys()))
            & (pl.col("diagnosis_date").is_between(pl.date(2000, 1, 1), pl.date(2018, 12, 31)))
        )

    @cache_result("cache")
    def _create_exposed_group(self, severe_chronic_cases: pl.LazyFrame) -> pl.LazyFrame:
        child_df = self.tables.get("Child")
        if child_df is None:
            raise ValueError("Child table not found")

        exposed_children = severe_chronic_cases.join(
            child_df, left_on="person_id", right_on="child_id"
        ).filter((pl.col("diagnosis_date") - pl.col("birth_date")) <= pl.duration(days=5 * 365))

        return exposed_children.select(
            [pl.col("family_id"), pl.col("child_id"), pl.col("diagnosis_date").alias("index_date")]
        ).unique()

    @cache_result("cache")
    def _create_unexposed_group(self) -> pl.LazyFrame:
        child_df = self.tables.get("Child")
        if child_df is None:
            raise ValueError("Child table not found")

        return (
            child_df.filter(
                pl.col("birth_date").is_between(pl.date(1995, 1, 1), pl.date(2018, 12, 31))
            )
            .select([pl.col("family_id"), pl.col("child_id"), pl.col("birth_date")])
            .unique()
        )

    @cache_result("cache")
    def _match_cohorts(
        self, exposed_group: pl.LazyFrame, unexposed_pool: pl.LazyFrame
    ) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        matched_unexposed = (
            exposed_group.join(
                unexposed_pool,
                on=["birth_date"],  # Add more matching criteria as needed
                how="left",
            )
            .filter(pl.col("family_id_right").is_not_null())
            .select(
                [
                    pl.col("family_id_right").alias("family_id"),
                    pl.col("child_id_right").alias("child_id"),
                    pl.col("index_date"),
                ]
            )
        )

        return exposed_group, matched_unexposed

    def generate_reports(self) -> None:
        self.logger.info("Generating reports")
        self._generate_health_report()
        self._generate_economic_report()
        self._generate_demographic_report()
        self._generate_integrated_analysis_report()
        self.logger.info("Reports generated successfully")

    def _generate_health_report(self) -> None:
        # Implement health report generation
        pass

    def _generate_economic_report(self) -> None:
        # Implement economic report generation
        pass

    def _generate_demographic_report(self) -> None:
        # Implement demographic report generation
        pass

    def _generate_integrated_analysis_report(self) -> None:
        # Implement integrated analysis report generation
        pass

    def analyze_data(self) -> None:
        self.logger.info("Performing data analysis")
        # Implement your specific data analysis here
        # This could include statistical tests, modeling, etc.
        self.logger.info("Data analysis completed")
