# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Centreon MBI Missing Partition Fixer - a tool to detect and fix missing date partitions in Centreon MBI database tables (`mod_bi_*` and `data_bin`). It extracts partition metadata from MySQL, identifies gaps in partition sequences, and generates SQL `REORGANIZE PARTITION` commands to fill them.

## Commands

### Run the tool
```bash
bash extract_result.sh              # Extract from MySQL and analyze
bash extract_result.sh -f result.txt  # Analyze existing file (no MySQL)
bash extract_result.sh -x           # Delete all .txt and .sql files
```

### Run Python analysis directly
```bash
python3 missing_date.py result.txt
```

## Architecture

Two-component pipeline:

1. **extract_result.sh** - Bash wrapper that:
   - Loads MySQL credentials from `.env` file (required)
   - Queries `information_schema.partitions` for partition metadata
   - Invokes `missing_date.py` with extracted data

2. **missing_date.py** - Python analyzer containing:
   - `PartitionChecker` class: analyzes a single table's partitions, finds date gaps, generates `REORGANIZE PARTITION` SQL
   - `parse_result_file()`: parses the custom text format from MySQL extraction
   - Outputs `partition_analysis.txt` (human-readable report) and `partition_fix.sql` (executable SQL)

## Configuration

Copy `.env.example` to `.env` and configure:
- `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`
- `MYSQL_MBI_TABLE` - comma-separated list of specific tables, or empty to analyze all `mod_bi_*` tables

## Testing

### Run the test suite
```bash
bash tests/run_tests.sh
```

### Test structure

Tests live in the `tests/` directory and are split into two categories:

**Python analysis tests** — run `missing_date.py` against fixture input files and diff the output against expected reference files:
- `tests/result_ok.txt` → `tests/partition_analysis_ok.txt` (no missing partitions)
- `tests/result_NOK.txt` → `tests/partition_analysis_NOK.txt` (missing partitions detected)

**Shell script tests** — validate `extract_result.sh` behavior without a MySQL connection:
- `-f <file>` mode works on valid fixture files
- Script exits non-zero on a missing input file

### Regenerating expected output

After an intentional change to `missing_date.py` output format, regenerate the reference files:

```bash
REGENERATE=1 bash tests/run_tests.sh
```

This overwrites the `tests/partition_analysis_*.txt` files. Commit the updated files alongside the code change.

### CI

The test suite runs automatically via GitHub Actions on every pull request to `master` (Python 3.10, Ubuntu latest). See `.github/workflows/ci.yml`.

## Key Implementation Details

- Partitions use daily granularity with Unix timestamps as boundaries
- Partition names follow pattern `pYYYYMMDD` (e.g., `p20250210`)
- Missing dates are grouped into consecutive ranges for efficient `REORGANIZE PARTITION` commands
- The SQL generation splits existing partitions without data loss
