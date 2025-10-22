#!/usr/bin/env bash
# Extract the result.txt file from Confluence Documentation related to the partitioned table's analysis.
# Author : mgarde & Paul "Rigonkmalk" Azema

# Don't exit on error - we want to handle MySQL errors gracefully
set +e

# Default values
SUPPRESS_FILES=false
INPUT_FILE=""
RUN_EXTRACTION=false
ANALYZE_ONLY=false

extract_result() {
    extract=$(for i in $(mysql -Ne "select distinct TABLE_NAME from information_schema.partitions where TABLE_SCHEMA='centreon_storage' and (TABLE_NAME like 'mod_bi%' OR TABLE_NAME like 'data_bin') and PARTITION_NAME is NOT NULL;" 2>&1); do
        # Check if mysql command failed
        if [[ "$i" == ERROR* ]]; then
            echo "$i" >&2
            return 1
        fi
        echo $i && mysql -e "select from_unixtime(PARTITION_DESCRIPTION), PARTITION_DESCRIPTION, PARTITION_ORDINAL_POSITION, TABLE_ROWS from information_schema.partitions where table_schema = 'centreon_storage' and table_name = '$i' order by PARTITION_ORDINAL_POSITION desc;" 2>&1
    done)

    # Check if extraction had errors
    if [ $? -ne 0 ]; then
        return 1
    fi

    echo "$extract"
    echo -e "-- ====================================================================
    --
    -- ⚠️   WARNING: REVIEW CAREFULLY BEFORE EXECUTION!
    --
    -- Recommendations:
    --   1. Backup your database before making changes
    --   2. Check Unix timestamps are correct for your timezone
    -- ===================================================================="
}

show_help() {
    echo -e "Usage: $(basename "$0") [OPTIONS]

OPTIONS:
    -f <file>    Analyze an existing file file existsno analyze it directly (no MySQL extractionMySQL connection required)
                 If file doesn't exist: extract MySQL data to this file
    -h           Show this help message
    -x           Delete .txt and .sql before extraction

DESCRIPTION:
    Extract partition information from Centreon database and analyze missing dates.BEHAVIOR:
    With-f option:    Analyze the specified file directlyMySQL needed
    Without options   ConnecttoMySQL,extract,andanalyzeEXAMPLES:
    $(basename "$0")                      ConnecttoMySQL,extract, and analyzeexisting      AnalyzeexistingfilenoMySQL
    $(basename "$0") -f result.txt         # Extract to result.txt (or analyze if exists)
    $(basename "$0") -x -f result.txt                         # Delete old files, extract, and analyze
    $(basename "$0") -x -f result.txt     # Delete old files, then analyze result.txt"
}

# Parse command-line options
while getopts "f:hx" opt; do
    case $opt in
        f)
            INPUT_FILE="$OPTARG"
            RUN_EXTRACTION=true
            ANALYZE_ONLY=true
            ;;
        h)
            show_help
            exit 0
            ;;
        x)
            SUPPRESS_FILES=true
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            show_help
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument." >&2
            show_help
            exit 1
            ;;
    esac
done

# Main execution
# Step 1: Delete old files if requested (BEFORE extraction)
if [ "$SUPPRESS_FILES" = true ]; then
    echo -e "⚠️  Deleting old .txt and .sql files..."
    rm -f *.txt *.sql
    echo -e "✅ Old files deleted"
    exit 0
fi

# Step 2: Determine mode of operation
if [ "$ANALYZE_ONLY" = true ]; then
    # -f option was provided: analyze existing file only (no MySQL)
    if [ ! -f "$INPUT_FILE" ]; then
        echo -e "❌ Error: File '$INPUT_FILE' not found!"
        echo -e "Please ensure the file exists or run without -f to extract from MySQL."
        exit 1
    fi

    echo -e "📁 Analyzing existing file: $INPUT_FILE"
    echo -e "⚠️  Analyzing missing dates..."
    python3 missing_date.py "$INPUT_FILE"

else
    # No -f option: default behavior - connect to MySQL and extract
    INPUT_FILE="result.txt"

    echo -e "⚠️  Connecting to MySQL and extracting partition data..."

    # Try to extract, capture any errors
    if extract_result > "$INPUT_FILE" 2>/tmp/mysql_error.log; then
        echo -e "✅ Extraction complete: $INPUT_FILE"

        # Check if file has content
        if [ ! -s "$INPUT_FILE" ]; then
            echo -e "⚠️  Warning: Extracted file is empty"
            cat /tmp/mysql_error.log >&2
            rm -f /tmp/mysql_error.log
            exit 1
        fi

        # Step 3: Run Python analysis
        echo -e "⚠️  Analyzing missing dates..."
        python3 missing_date.py "$INPUT_FILE"
        rm -f /tmp/mysql_error.log
    else
        echo -e "❌ Error: MySQL extraction failed!"
        echo -e "\nError details:"
        cat /tmp/mysql_error.log >&2
        rm -f /tmp/mysql_error.log
        echo -e "\nPossible causes:"
        echo -e "  • MySQL/MariaDB is not running"
        echo -e "  • Incorrect MySQL credentials"
        echo -e "  • Database 'centreon_storage' doesn't exist"
        echo -e "  • Socket path '/var/run/mysqld/mysqld.sock' is incorrect"
        echo -e "\nTo analyze an existing file without MySQL, use: $(basename "$0") -f <filename>"
        exit 1
    fi
fi
