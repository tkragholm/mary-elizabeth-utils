import json
import logging
import traceback
from pathlib import Path

import polars as pl
import polars.selectors as cs
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
    if file_path.suffix.lower() == ".parquet":
        return pl.read_parquet(file_path)
    elif file_path.suffix.lower() == ".csv":
        return pl.read_csv(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")


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


def analyze_registers(directory: str):
    results = {}
    files = list(Path(directory).rglob("*.parquet")) + list(Path(directory).rglob("*.csv"))

    with Progress() as progress:
        task = progress.add_task("[green]Analyzing registers...", total=len(files))

        for file_path in files:
            try:
                register_name = file_path.stem.split("_")[0].upper()
                year = file_path.stem.split("_")[1]

                if register_name not in results:
                    results[register_name] = {}

                results[register_name][year] = analyze_register(file_path, progress, task)
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
    data_directory = "/Users/tobiaskragholm/dev/mary-elizabeth-utils/synth_data"
    output_file = "register_summary.log"

    try:
        with console.status("[bold green]Analyzing registers...") as status:
            summary = analyze_registers(data_directory)

        save_summary(summary, output_file)

        rprint("[bold green]Analysis complete!")
        rprint(f"[bold blue]Total registers analyzed: {len(summary)}")
        rprint(f"[bold blue]Summary saved to: {output_file}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e!s}")
        logger.error(traceback.format_exc())
