---
description: Two-component pipeline architecture of the partition fixer
---

# Architecture

Two-component pipeline:

## 1. `extract_result.sh` — Bash wrapper

- Loads MySQL credentials from `.env` file (required for live extraction)
- Queries `information_schema.partitions` for partition metadata
- Invokes `missing_date.py` with extracted data
- Supports `-f <file>` to skip MySQL and analyze an existing result file
- Supports `-x` to clean up generated `.txt` and `.sql` output files

## 2. `missing_date.py` — Python analyzer

- `PartitionChecker` class: analyzes a single table's partitions, finds date gaps, generates
  `REORGANIZE PARTITION` or `ADD PARTITION` SQL
- `parse_result_file()`: parses the custom tab-separated text format produced by MySQL extraction
- Outputs two files:
  - `partition_analysis.txt` — human-readable report
  - `partition_fix.sql` — executable SQL ready for review and execution

## Data flow

```
MySQL (information_schema.partitions)
    → extract_result.sh
        → result.txt (intermediate)
            → missing_date.py
                → partition_analysis.txt
                → partition_fix.sql
```
