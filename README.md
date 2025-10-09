# Centreon MBI - Missing Partition Fixer

## ⚠️ WARNING
```
This script need to be executed in a specific environment. And need to be helped with the Centreon's Customer Care team.
```

## Description

This script is designed to parse and analyze result.txt files, providing a parser and a check to if a date is missing, if a date is missing, reorganize partitioned tables if a date is missing between two dates for each partition following this process.

## Dependencies
- Python 3.x

## Usage

```bash
bash extract_result.sh
```

You can follow the file `partition_analysis.txt` for more details of what the script found.

If you validate the results, you can execute the generated SQL `partition_fix.sql` script to fix the partitioned tables.

### Apply fix

```bash
mysql centreon_storage < partition_fix.sql
```

## Credits

`Claude 4.5 Thinking` for helping me in this python script.
