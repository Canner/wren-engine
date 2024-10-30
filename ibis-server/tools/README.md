# Description

This folder contains useful tools and scripts for debugging and validation.

# Tools
- `mdl_validation.py`: Used to validate a Wren MDL. This script attempts to select all columns in all models.
  - Requires the `wren_core` library. Run `just install-core` and `just install` before using it.
```
poetry run python tools/mdl_validation.py mdl.json function_list/bigquery.csv
```
