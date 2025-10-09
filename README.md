# Script Parser MBI

## Description

This script is designed to parse and analyze result.txt files, providing a parser and a check to if a date is missing, if a date is missing, reorganize partitioned tables if a date is missing between two dates for each partition following this process.

## Dependencies
- Python 3.x

## Usage

You need to add inside this folder the result.txt output from Confluence Documentation related to the partitioned table's analysis.

```bash
python3 missing_date.py
```

You can follow the file `partition_analysis.txt` for more details of what the script found.

If you validate the results, you can execute the generated SQL `partition_fix.sql` script to fix the partitioned tables.

```bash
mysql centreon_storage < partition_fix.sql
```
