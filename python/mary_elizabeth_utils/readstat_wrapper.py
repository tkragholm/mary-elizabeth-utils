import json
import subprocess
from pathlib import Path
from typing import Any


class ReadstatWrapper:
    def __init__(self, readstat_path: str):
        self.readstat_path = Path(readstat_path)
        if not self.readstat_path.exists():
            raise FileNotFoundError(f"readstat binary not found at {self.readstat_path}")

    def _run_command(self, args: list[str]) -> str:
        command = [str(self.readstat_path)] + args
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return result.stdout

    def metadata(self, input_file: str, as_json: bool = False) -> str | dict[str, Any]:
        args = ["metadata", input_file]
        if as_json:
            args.append("--as-json")
        output = self._run_command(args)
        return json.loads(output) if as_json else output

    def preview(self, input_file: str, rows: int = 10) -> str:
        args = ["preview", input_file, "--rows", str(rows)]
        return self._run_command(args)

    def convert(
        self,
        input_file: str,
        output_file: str,
        format: str = "csv",
        rows: int | None = None,
        overwrite: bool = False,
        parallel: bool = False,
        stream_rows: int = 10000,
    ) -> None:
        args = ["data", input_file, "--output", output_file, "--format", format]

        if rows is not None:
            args.extend(["--rows", str(rows)])
        if overwrite:
            args.append("--overwrite")
        if parallel:
            args.append("--parallel")

        args.extend(["--stream-rows", str(stream_rows)])

        self._run_command(args)

    def _convert_with_kwargs(
        self, input_file: str, output_file: str, format: str, **kwargs: Any
    ) -> None:
        rows = kwargs.get("rows")
        overwrite = kwargs.get("overwrite", False)
        parallel = kwargs.get("parallel", False)
        stream_rows = kwargs.get("stream_rows", 10000)

        self.convert(input_file, output_file, format, rows, overwrite, parallel, stream_rows)

    def convert_to_csv(self, input_file: str, output_file: str, **kwargs: Any) -> None:
        self._convert_with_kwargs(input_file, output_file, format="csv", **kwargs)

    def convert_to_feather(self, input_file: str, output_file: str, **kwargs: Any) -> None:
        self._convert_with_kwargs(input_file, output_file, format="feather", **kwargs)

    def convert_to_ndjson(self, input_file: str, output_file: str, **kwargs: Any) -> None:
        self._convert_with_kwargs(input_file, output_file, format="ndjson", **kwargs)

    def convert_to_parquet(self, input_file: str, output_file: str, **kwargs: Any) -> None:
        self._convert_with_kwargs(input_file, output_file, format="parquet", **kwargs)
