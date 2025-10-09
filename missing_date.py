#!/usr/bin/env python3
"""
Partition Gap Checker - File-based Input
Reads partition data from result.txt and generates SQL fix commands
"""

import sys
import re
from datetime import datetime, timedelta
from typing import List, Dict, Tuple


class PartitionChecker:
    def __init__(self, table_name: str, partition_data: List[Tuple], output_file=None):
        """
        Initialize with table name and partition data
        partition_data format: [(date_str, unix_timestamp, ordinal, row_count), ...]
        """
        self.table_name = table_name
        self.partitions = []
        self.output_file = output_file

        for row in partition_data:
            try:
                self.partitions.append(
                    {
                        "date": datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"),
                        "unix_ts": int(row[1]),
                        "ordinal": int(row[2]),
                        "rows": int(row[3]),
                    }
                )
            except (ValueError, IndexError) as e:
                print(f"⚠️  Warning: Skipping invalid row: {row} - {e}")
                continue

        # Sort by date
        self.partitions.sort(key=lambda x: x["date"])

    def print_output(self, message):
        """Print to both console and file"""
        print(message)
        if self.output_file:
            self.output_file.write(message + "\n")

    def find_missing_dates(self) -> List[datetime]:
        """Find missing dates in the partition sequence"""
        if len(self.partitions) < 2:
            return []

        missing_dates = []

        for i in range(len(self.partitions) - 1):
            current_date = self.partitions[i]["date"]
            next_date = self.partitions[i + 1]["date"]

            # Check if there's a gap
            expected_next = current_date + timedelta(days=1)

            while expected_next < next_date:
                missing_dates.append(expected_next)
                expected_next += timedelta(days=1)

        return missing_dates

    def generate_partition_name(self, date: datetime) -> str:
        """Generate partition name from date"""
        return f"p{date.strftime('%Y%m%d')}"

    def generate_reorganize_commands(self, missing_dates: List[datetime]) -> List[str]:
        """Generate SQL commands to add missing partitions"""
        if not missing_dates:
            return []

        commands = []

        # Group consecutive missing dates for efficient reorganization
        groups = self.group_consecutive_dates(missing_dates)

        for group in groups:
            # Find the partition range that needs reorganization
            start_date = group[0]
            end_date = group[-1]

            # Find surrounding existing partitions
            before_partition = None
            after_partition = None

            for p in self.partitions:
                if p["date"] < start_date:
                    before_partition = p
                elif p["date"] > end_date and after_partition is None:
                    after_partition = p
                    break

            if before_partition and after_partition:
                # Generate reorganize command
                cmd = self.create_reorganize_command(
                    before_partition, after_partition, group
                )
                commands.append(cmd)

        return commands

    def group_consecutive_dates(self, dates: List[datetime]) -> List[List[datetime]]:
        """Group consecutive dates together"""
        if not dates:
            return []

        groups = []
        current_group = [dates[0]]

        for i in range(1, len(dates)):
            if dates[i] - current_group[-1] == timedelta(days=1):
                current_group.append(dates[i])
            else:
                groups.append(current_group)
                current_group = [dates[i]]

        groups.append(current_group)
        return groups

    def create_reorganize_command(
        self, before_part: Dict, after_part: Dict, missing_dates: List[datetime]
    ) -> str:
        """Create ALTER TABLE REORGANIZE PARTITION command"""

        partition_name_after = self.generate_partition_name(after_part["date"])

        # Build the reorganize command
        cmd = f"ALTER TABLE {self.table_name}\n"
        cmd += f"REORGANIZE PARTITION {partition_name_after} INTO (\n"

        # Add all missing partitions
        all_partitions = []
        for missing_date in missing_dates:
            part_name = self.generate_partition_name(missing_date)
            unix_ts = int(missing_date.timestamp())
            all_partitions.append(
                f"  PARTITION {part_name} VALUES LESS THAN ({unix_ts}) ENGINE = InnoDB"
            )

        # Add the after partition back
        part_name = self.generate_partition_name(after_part["date"])
        all_partitions.append(
            f"  PARTITION {part_name} VALUES LESS THAN ({after_part['unix_ts']}) ENGINE = InnoDB"
        )

        cmd += ",\n".join(all_partitions)
        cmd += "\n);"

        return cmd

    def display_report(self):
        """Display analysis report"""
        self.print_output(f"\n{'=' * 70}")
        self.print_output(f"PARTITION ANALYSIS: {self.table_name}")
        self.print_output(f"{'=' * 70}")

        self.print_output(f"\nTotal Partitions: {len(self.partitions)}")
        if self.partitions:
            self.print_output(
                f"Date Range: {self.partitions[0]['date'].date()} to {self.partitions[-1]['date'].date()}"
            )

        # Display partition details
        self.print_output(f"\nExisting Partitions:")
        self.print_output(
            f"{'Date':<20} {'Unix Timestamp':<15} {'Ordinal':<10} {'Rows':<10}"
        )
        self.print_output(f"{'-' * 70}")
        for p in reversed(self.partitions):
            self.print_output(
                f"{str(p['date']):<20} {p['unix_ts']:<15} {p['ordinal']:<10} {p['rows']:<10}"
            )

        missing_dates = self.find_missing_dates()

        if missing_dates:
            self.print_output(
                f"\n⚠️  WARNING: Found {len(missing_dates)} missing date(s):"
            )
            for md in missing_dates:
                self.print_output(f"   - {md.date()}")

            return missing_dates
        else:
            self.print_output(
                f"\n✅ No missing dates found. Partitions are continuous."
            )
            return []


def parse_result_file(filename: str) -> Dict[str, List[Tuple]]:
    """
    Parse result.txt file to extract table names and partition data

    Expected format in result.txt:

    table_name
    from_unixtime(PARTITION_DESCRIPTION) PARTITION_DESCRIPTION PARTITION_ORDINAL_POSITION TABLE_ROWS
    2025-10-09 00:00:00 1759964400 90 0
    2025-10-08 00:00:00 1759878000 89 0
    ...

    another_table_name
    from_unixtime(PARTITION_DESCRIPTION) PARTITION_DESCRIPTION PARTITION_ORDINAL_POSITION TABLE_ROWS
    2025-10-09 00:00:00 1759964400 90 1250
    ...
    """

    tables_data = {}
    current_table = None
    current_data = []

    try:
        with open(filename, "r", encoding="utf-8") as f:
            lines = f.readlines()

        i = 0
        while i < len(lines):
            line = lines[i].strip()

            # Skip empty lines
            if not line:
                i += 1
                continue

            # Check if this is a table name line (no spaces at start, not a date)
            if not line.startswith(" ") and not re.match(r"\d{4}-\d{2}-\d{2}", line):
                # Save previous table data if exists
                if current_table and current_data:
                    tables_data[current_table] = current_data
                    current_data = []

                # Check if it's a header line or table name
                if (
                    "from_unixtime" in line.lower()
                    or "partition_description" in line.lower()
                ):
                    # This is a header, skip it
                    i += 1
                    continue
                else:
                    # This is a table name
                    current_table = line
                    i += 1

                    # Skip the header line if it follows
                    if i < len(lines) and "from_unixtime" in lines[i].lower():
                        i += 1
                    continue

            # Parse data line (date time unix_ts ordinal rows)
            if re.match(r"\d{4}-\d{2}-\d{2}", line):
                parts = line.split()

                if len(parts) >= 4:
                    # Format: date time unix_ts ordinal rows
                    date_str = f"{parts[0]} {parts[1]}"
                    unix_ts = parts[2]
                    ordinal = parts[3]
                    rows = parts[4] if len(parts) > 4 else "0"

                    current_data.append((date_str, unix_ts, ordinal, rows))

            i += 1

        # Save last table
        if current_table and current_data:
            tables_data[current_table] = current_data

    except FileNotFoundError:
        print(f"❌ Error: File '{filename}' not found!")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error parsing file: {e}")
        sys.exit(1)

    return tables_data


def main():
    input_filename = "result.txt"
    analysis_filename = "partition_analysis.txt"
    sql_filename = "partition_fix.sql"

    print(f"{'=' * 70}")
    print(f"PARTITION CHECKER - Reading from {input_filename}")
    print(f"{'=' * 70}\n")

    # Parse input file
    print(f"📖 Reading partition data from '{input_filename}'...")
    tables_data = parse_result_file(input_filename)

    if not tables_data:
        print(f"❌ No table data found in '{input_filename}'")
        print(f"\nExpected format:")
        print(f"table_name")
        print(
            f"from_unixtime(PARTITION_DESCRIPTION) PARTITION_DESCRIPTION PARTITION_ORDINAL_POSITION TABLE_ROWS"
        )
        print(f"2025-10-09 00:00:00 1759964400 90 0")
        print(f"2025-10-08 00:00:00 1759878000 89 0")
        sys.exit(1)

    print(f"✅ Found {len(tables_data)} table(s): {', '.join(tables_data.keys())}\n")

    # Open output files
    with (
        open(analysis_filename, "w", encoding="utf-8") as analysis_file,
        open(sql_filename, "w", encoding="utf-8") as sql_file,
    ):
        # Write headers
        analysis_file.write(f"{'=' * 70}\n")
        analysis_file.write(f"PARTITION ANALYSIS REPORT\n")
        analysis_file.write(
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        analysis_file.write(f"Source: {input_filename}\n")
        analysis_file.write(f"{'=' * 70}\n")

        sql_file.write(f"-- {'=' * 68}\n")
        sql_file.write(f"-- PARTITION FIX SQL COMMANDS\n")
        sql_file.write(
            f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        sql_file.write(f"-- Source: {input_filename}\n")
        sql_file.write(f"-- {'=' * 68}\n")
        sql_file.write(f"-- \n")
        sql_file.write(f"-- ⚠️  WARNING: REVIEW CAREFULLY BEFORE EXECUTION!\n")
        sql_file.write(f"-- \n")
        sql_file.write(f"-- Recommendations:\n")
        sql_file.write(f"--   1. Backup your database before making changes\n")
        sql_file.write(f"--   2. Verify partition names match your naming convention\n")
        sql_file.write(f"--   3. Check Unix timestamps are correct for your timezone\n")
        sql_file.write(f"--   4. Test on a non-production environment first\n")
        sql_file.write(f"--   5. Execute commands one at a time and verify results\n")
        sql_file.write(f"-- {'=' * 68}\n\n")

        print(f"{'=' * 70}")
        print(f"PARTITION ANALYSIS REPORT")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'=' * 70}")

        # Process each table
        total_tables = len(tables_data)
        tables_with_issues = 0
        all_sql_commands = []

        for table_name, partition_data in tables_data.items():
            if not partition_data:
                print(f"⚠️  Skipping {table_name} - no data provided")
                continue

            checker = PartitionChecker(table_name, partition_data, analysis_file)
            missing_dates = checker.display_report()

            if missing_dates:
                tables_with_issues += 1

                # Generate SQL commands
                sql_commands = checker.generate_reorganize_commands(missing_dates)

                if sql_commands:
                    # Write to analysis file
                    analysis_file.write(f"\n{'=' * 70}\n")
                    analysis_file.write(f"PROPOSED SQL COMMANDS FOR: {table_name}\n")
                    analysis_file.write(f"{'=' * 70}\n\n")

                    # Write to SQL file
                    sql_file.write(f"\n-- {'=' * 68}\n")
                    sql_file.write(f"-- Table: {table_name}\n")
                    sql_file.write(
                        f"-- Missing {len(missing_dates)} partition(s): {', '.join([d.strftime('%Y-%m-%d') for d in missing_dates])}\n"
                    )
                    sql_file.write(f"-- {'=' * 68}\n\n")

                    for cmd in sql_commands:
                        if cmd.strip().startswith("ALTER TABLE"):
                            analysis_file.write(cmd + "\n\n")
                            sql_file.write(cmd + "\n\n")
                            all_sql_commands.append(cmd)

            analysis_file.write("\n\n")
            print("\n")

        # Write summary
        summary = f"\n{'=' * 70}\n"
        summary += f"SUMMARY\n"
        summary += f"{'=' * 70}\n"
        summary += f"Total tables analyzed: {total_tables}\n"
        summary += f"Tables with missing partitions: {tables_with_issues}\n"
        summary += f"Tables OK: {total_tables - tables_with_issues}\n"
        summary += f"Total SQL commands generated: {len(all_sql_commands)}\n"
        summary += f"{'=' * 70}\n"

        analysis_file.write(summary)
        print(summary)

        if all_sql_commands:
            sql_file.write(f"\n-- {'=' * 68}\n")
            sql_file.write(f"-- SUMMARY\n")
            sql_file.write(f"-- {'=' * 68}\n")
            sql_file.write(f"-- Total tables with issues: {tables_with_issues}\n")
            sql_file.write(f"-- Total SQL commands: {len(all_sql_commands)}\n")
            sql_file.write(f"-- {'=' * 68}\n")

    # Final output
    print(f"\n📄 Output files created:")
    print(f"   ✅ {analysis_filename} - Detailed analysis report")
    print(f"   ✅ {sql_filename} - SQL commands ready for execution")

    if tables_with_issues > 0:
        print(f"\n⚠️  {tables_with_issues} table(s) require attention!")
        print(f"   Review '{sql_filename}' before executing commands.")
    else:
        print(f"\n✅ All tables are OK! No missing partitions detected.")

    print()


if __name__ == "__main__":
    main()
