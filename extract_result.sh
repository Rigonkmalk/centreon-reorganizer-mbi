#!/bin/bash -e
# Extract the result.txt file from Confluence Documentation related to the partitioned table's analysis.

suppress_old_file() {
    if [ -n "$(ls *.txt)" ]; then
        for i in *.txt; do
            rm "$i"
        done
    fi
    if [ -n "$(ls *.sql)" ]; then
        for i in *.sql; do
            rm "$i"
        done
    fi}

extract_result() {
    extract=$(for i in $(mysql -Ne "select distinct TABLE_NAME from information_schema.partitions where TABLE_SCHEMA='centreon_storage' and (TABLE_NAME like 'mod_bi%' OR TABLE_NAME like 'data_bin') and PARTITION_NAME is NOT NULL;"); do echo $i && mysql -e "select from_unixtime(PARTITION_DESCRIPTION), PARTITION_DESCRIPTION, PARTITION_ORDINAL_POSITION, TABLE_ROWS from information_schema.partitions where table_schema = 'centreon_storage' and table_name = '$i' order by PARTITION_ORDINAL_POSITION desc;";done)
    echo "$extract"
}

echo -e "suppress old files"
suppress_old_file
echo -e "extract result"
extract_result
echo -e "extract missing date"
python3 missing_date.py
