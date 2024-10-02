import json
import logging
import re
import traceback
from pathlib import Path

import polars as pl
import polars.selectors as cs
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
from dateutil.parser import ParserError
from dateutil.parser import parse as dateutil_parse
from rich import print as rprint
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, TaskID

# Set up rich console and logging
console = Console()
logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True)],
)
logger = logging.getLogger("rich")

# List of special variables
SPECIAL_VARS = [
    "PNR",
    "SENR",
    "CPR",
    "CVR",
    "PNR12",
    "AEGTE_ID",
    "E_FAELLE_ID",
    "FAR_ID",
    "MOR_ID",
    "FAMILIE_ID",
    "ARBGNR",
    "ARBNR",
    "RECNUM",
    "PK_MFR",
    "FK_MFR",
    "YDERNR",
    "CPR_BARN",
    "CPR_FADER",
    "CPR_MODER",
]


def safe_float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def safe_date_parse(value):
    if value is None:
        return None
    try:
        return dateutil_parse(str(value)).strftime("%Y-%m-%d")
    except (ParserError, ValueError, TypeError):
        return None


def is_categorical(series, threshold=0.1):
    n_unique = series.n_unique()
    n_total = len(series)
    return n_unique / n_total < threshold


def read_file(file_path: Path) -> pl.DataFrame:
    try:
        if file_path.suffix.lower() == ".parquet":
            # Read Parquet file with pyarrow
            arrow_table = pq.read_table(file_path)
        elif file_path.suffix.lower() == ".csv":
            # Read CSV file with pyarrow
            read_options = pa_csv.ReadOptions(encoding="utf8")
            parse_options = pa_csv.ParseOptions(ignore_empty_lines=True)
            convert_options = pa_csv.ConvertOptions(
                strings_can_be_null=True, null_values=["", "NULL", "null", "NA", "na", "NaN", "nan"]
            )
            arrow_table = pa_csv.read_csv(
                file_path,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options,
            )
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")

        # Convert SPECIAL_VARS to strings
        schema = arrow_table.schema
        new_fields = []
        for field in schema:
            if field.name in SPECIAL_VARS:
                new_fields.append(pa.field(field.name, pa.string()))
            else:
                new_fields.append(field)
        new_schema = pa.schema(new_fields)

        # Cast the table to the new schema
        arrow_table = arrow_table.cast(new_schema)

        # Convert Arrow table to Polars DataFrame
        result = pl.from_arrow(arrow_table)

        # Ensure we return a DataFrame
        if isinstance(result, pl.DataFrame):
            return result
        elif isinstance(result, pl.Series):
            return result.to_frame()
        else:
            raise ValueError(f"Unexpected type returned from pl.from_arrow: {type(result)}")

    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e!s}")
        logger.error(traceback.format_exc())
        return pl.DataFrame()


def analyze_register(file_path: Path, progress: Progress, task: TaskID):
    try:
        progress.update(task, description=f"Analyzing {file_path.name}")
        df = read_file(file_path)

        summary = {
            "file_name": file_path.name,
            "num_rows": len(df),
            "num_columns": len(df.columns),
            "columns": {},
        }

        for col in df.columns:
            try:
                col_summary = {
                    "dtype": str(df[col].dtype),
                    "num_unique": df[col].n_unique(),
                    "num_null": df[col].null_count(),
                }

                if col not in SPECIAL_VARS:
                    if col in df.select(cs.numeric()).columns:
                        col_summary.update(
                            {
                                "min": safe_float(df[col].min()),
                                "max": safe_float(df[col].max()),
                                "mean": safe_float(df[col].mean()),
                                "median": safe_float(df[col].median()),
                                "std": safe_float(df[col].std()),
                            }
                        )
                    elif col in df.select(cs.temporal()).columns:
                        min_date = df[col].min()
                        max_date = df[col].max()
                        col_summary.update(
                            {
                                "min": safe_date_parse(min_date),
                                "max": safe_date_parse(max_date),
                            }
                        )

                    # Check for categorical variables
                    if is_categorical(df[col]):
                        col_summary["is_categorical"] = True
                        try:
                            value_counts = df.select(
                                pl.col(col).value_counts(sort=True).alias("value_counts")
                            )
                            top_10_values = value_counts.select(
                                pl.col("value_counts").struct.field(col).alias("value"),
                                pl.col("value_counts").struct.field("count").alias("count"),
                            )
                            col_summary["top_10_values"] = [
                                {"value": str(row["value"]), "count": int(row["count"])}
                                for row in top_10_values.limit(10).to_dicts()
                            ]
                        except Exception as vc_error:
                            col_summary["value_counts_error"] = str(vc_error)
                    else:
                        col_summary["is_categorical"] = False

                    # Check for potential date columns in string type
                    if df[col].dtype == pl.Utf8:
                        sample = df[col].drop_nulls().head(100)
                        potential_dates = [safe_date_parse(val) for val in sample]
                        if any(potential_dates):
                            col_summary["potential_date_column"] = True
                            col_summary.update(
                                {
                                    "min": safe_date_parse(df[col].min()),
                                    "max": safe_date_parse(df[col].max()),
                                }
                            )

                summary["columns"][col] = col_summary
            except Exception as e:
                logger.error(f"Error processing column {col} in file {file_path}: {e!s}")
                summary["columns"][col] = {"error": str(e)}

        progress.update(task, advance=1)
        return summary
    except Exception as e:
        logger.error(f"Error analyzing file {file_path}: {e!s}")
        progress.update(task, advance=1)
        return {"error": str(e), "traceback": traceback.format_exc()}


def analyze_registers(input_directory: str, output_directory: str):
    results = {}
    files = list(Path(input_directory).rglob("*.parquet")) + list(
        Path(input_directory).rglob("*.csv")
    )

    with Progress() as progress:
        task = progress.add_task("[green]Analyzing registers...", total=len(files))

        for file_path in files:
            try:
                # Extract register name and year from filename
                file_stem = file_path.stem

                # Try to match patterns: register_2000, register2000, or just register
                match = re.match(r"([A-Za-z]+)(_?\d*)", file_stem)

                if match:
                    register_name = match.group(1).upper()
                    year = match.group(2).lstrip("_")  # Remove leading underscore if present
                else:
                    # If no match, use the whole filename as register name
                    register_name = file_stem.upper()
                    year = ""

                if register_name not in results:
                    results[register_name] = {}

                # Use 'single' as the key if there's no year
                year_key = year if year else "single"
                results[register_name][year_key] = analyze_register(file_path, progress, task)

                # Save the dataframe as a parquet file
                df = read_file(file_path)
                output_filename = f"{year}.parquet" if year else f"{register_name}.parquet"
                output_path = Path(output_directory) / "registers" / register_name / output_filename
                output_path.parent.mkdir(parents=True, exist_ok=True)
                df.write_parquet(output_path)

            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e!s}")
                results[str(file_path)] = {"error": str(e), "traceback": traceback.format_exc()}

    return results


def save_summary(summary: dict, output_file: str):
    try:
        with open(output_file, "w") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Summary saved to {output_file}")
    except Exception as e:
        logger.error(f"Error saving summary to {output_file}: {e!s}")


if __name__ == "__main__":
    input_directory = "/path/to/input/directory"
    output_directory = "/path/to/output/directory"
    summary_file = "register_summary.log"

    try:
        with console.status("[bold green]Analyzing registers...") as status:
            summary = analyze_registers(input_directory, output_directory)

        save_summary(summary, summary_file)

        rprint("[bold green]Analysis complete!")
        rprint(f"[bold blue]Total registers analyzed: {len(summary)}")
        rprint(f"[bold blue]Summary saved to: {summary_file}")
        rprint(f"[bold blue]Parquet files saved to: {output_directory}/registers")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e!s}")
        logger.error(traceback.format_exc())
