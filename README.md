# mary-elizabeth-utils

My utilities for the different projects

## Installation

```bash
pip install mary-elizabeth-utils
```

## Usage

```python
from mary_elizabeth_utils import get_readstat_path
from readstat_wrapper import ReadstatWrapper

# Initialize the wrapper with the path to the readstat binary
readstat = ReadstatWrapper(get_readstat_path())

# Get metadata
metadata = readstat.metadata("/path/to/example.sas7bdat")
print(metadata)

# Preview data
preview = readstat.preview("/path/to/example.sas7bdat", rows=20)
print(preview)

# Convert to CSV
readstat.convert_to_csv("/path/to/example.sas7bdat", "/path/to/output.csv", rows=50000, overwrite=True, parallel=True)

# Convert to Parquet
readstat.convert_to_parquet("/path/to/example.sas7bdat", "/path/to/output.parquet", overwrite=True, parallel=True)
```
