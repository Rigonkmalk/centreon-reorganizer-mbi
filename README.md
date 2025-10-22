# Centreon MBI - Missing Partition Fixer

A tool to automatically detect and fix missing date partitions in Centreon MBI database tables.

## ⚠️ WARNING

**This script must be executed in a Centreon environment with proper database access. It is strongly recommended to work with the Centreon Customer Care team before making any modifications to production databases.**

## Overview

This tool analyzes Centreon Storage partitioned tables (`mod_bi_*` and `data_bin`) to identify missing dates in the partition sequences. When gaps are detected, it automatically generates SQL commands to reorganize the partitions and fill the missing dates.

### What it does

1. **Extracts** partition information from the `centreon_storage` database
2. **Analyzes** the partition sequences to detect missing dates
3. **Generates** SQL reorganization commands to fix the gaps
4. **Reports** detailed findings in a human-readable analysis file

## Prerequisites

- **Python 3.x**
- **MySQL/MariaDB** with access to the `centreon_storage` database
- **Bash shell** (Linux/Unix environment)
- **Centreon MBI** installation with partitioned tables

## Installation

No installation required. Simply clone or download this repository:

```bash
git clone <repository-url>
cd script-parser-mbi
```

## Usage

### Basic Usage

Extract partition data from MySQL and analyze it:

```bash
bash extract_result.sh
```

### Advanced Options

```bash
bash extract_result.sh [OPTIONS]
```

**Available Options:**

| Option | Description |
|--------|-------------|
| `-f <file>` | Analyze an existing file without connecting to MySQL. If the file doesn't exist, extract MySQL data to this file. |
| `-x` | Delete all `.txt` and `.sql` files before execution (cleanup) |
| `-h` | Show help message |

### Examples

**Extract and analyze from MySQL:**
```bash
bash extract_result.sh
```

**Analyze an existing result file (no MySQL connection needed):**
```bash
bash extract_result.sh -f result.txt
```

**Clean up old files, then extract and analyze:**
```bash
bash extract_result.sh -x
```

**Extract to a custom file:**
```bash
bash extract_result.sh -f custom_output.txt
```

## Output Files

The tool generates the following files:

| File | Description |
|------|-------------|
| `result.txt` | Raw partition data extracted from MySQL |
| `partition_analysis.txt` | Detailed human-readable analysis of missing dates |
| `partition_fix.sql` | SQL commands to fix the missing partitions |

## Workflow

### Step 1: Extract and Analyze

```bash
bash extract_result.sh
```

The script will:
- Connect to MySQL and extract partition information
- Detect any missing dates in partition sequences
- Generate analysis and fix files

### Step 2: Review the Analysis

```bash
cat partition_analysis.txt
```

Review the detailed report to understand:
- Which tables have missing partitions
- What dates are missing
- What SQL commands will be executed

### Step 3: Review the SQL Fix

```bash
cat partition_fix.sql
```

**Important:** Carefully review the generated SQL commands before execution.

### Step 4: Apply the Fix

**IMPORTANT: Backup your database before applying any changes!**

```bash
mysql centreon_storage < partition_fix.sql
```

## Error Handling

The script includes robust error handling for common issues:

- **MySQL connection failures**: Clear error messages with troubleshooting steps
- **Missing files**: Helpful guidance on which option to use
- **Empty extractions**: Warnings when no data is found
- **Invalid partition data**: Automatic skipping with warnings

### Common Issues

**MySQL Connection Error:**
```
❌ Error: MySQL extraction failed!
```
**Solutions:**
- Ensure MySQL/MariaDB is running
- Check MySQL credentials and permissions
- Verify `centreon_storage` database exists
- Check MySQL socket path is correct

**File Not Found:**
```
❌ Error: File 'result.txt' not found!
```
**Solution:** Run without `-f` option to extract from MySQL first, or provide a valid file path.

## Database Compatibility

This tool works with:
- **Centreon MBI** partitioned tables
- **MySQL 5.x+** / **MariaDB 10.x+**
- Tables matching patterns: `mod_bi_*` and `data_bin`

## Safety Features

- **Non-destructive analysis**: Analysis phase only reads data
- **SQL preview**: Review all commands before execution
- **Warnings and recommendations**: Built-in safety checks
- **Error recovery**: Graceful handling of failures

## Technical Details

### Partition Detection Query

The tool queries partitioned tables using:
```sql
SELECT distinct TABLE_NAME
FROM information_schema.partitions
WHERE TABLE_SCHEMA='centreon_storage'
  AND (TABLE_NAME LIKE 'mod_bi%' OR TABLE_NAME LIKE 'data_bin')
  AND PARTITION_NAME IS NOT NULL;
```

### Reorganization Strategy

Missing partitions are filled using `ALTER TABLE ... REORGANIZE PARTITION` commands that split existing partitions without data loss.

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing style conventions
- All features are documented
- Testing is performed in non-production environments first

## Credits

- **Authors:** mgarde & Paul "Rigonkmalk" Azema
- **AI Assistance:** Claude 3.5 Sonnet for Python script development

## License

[Specify your license here]

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review generated `partition_analysis.txt` for detailed diagnostics
3. Contact Centreon Customer Care for production environment assistance
