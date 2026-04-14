#!/usr/bin/env python3
"""
Partition Gap Checker - File-based Input
Reads partition data from result.txt and generates SQL fix commands
"""

import sys
import re
import argparse
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple


class ParseError(Exception):
    """Raised when the input partition file cannot be parsed."""


class PartitionChecker:
    def __init__(self, table_name: str, partition_data: List[Tuple], output_file=None, reference_date: Optional[datetime] = None):
        """
        Initialize with table name and partition data
        partition_data format: [(date_str, unix_timestamp, ordinal, row_count), ...]
        reference_date: naive UTC datetime used as "today" for tail-end gap detection.
            Must not carry tzinfo — pass datetime(2026, 2, 20) not datetime(..., tzinfo=utc).
            Raises ValueError if a timezone-aware datetime is provided.
            Defaults to today at midnight UTC (naive).
        """
        if reference_date is not None and reference_date.tzinfo is not None:
            raise ValueError(
                "reference_date must be a naive datetime (no tzinfo). "
                "Strip the timezone before passing, e.g. "
                "datetime.now(timezone.utc).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)."
            )
        self.table_name = table_name
        self.partitions = []
        self.output_file = output_file
        self.reference_date = reference_date or datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=None
        )

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
        if not self.partitions:
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

        # Check for missing dates after the last partition up to the reference date
        expected_next = self.partitions[-1]["date"] + timedelta(days=1)
        while expected_next <= self.reference_date:
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
                # Gap between two existing partitions: use REORGANIZE PARTITION
                cmd = self.create_reorganize_command(
                    before_partition, after_partition, group
                )
                commands.append(cmd)
            elif before_partition and not after_partition:
                # Tail-end gap (missing dates after the last partition): use ADD PARTITION
                cmd = self.create_add_partition_command(group)
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
            unix_ts = int(missing_date.replace(tzinfo=timezone.utc).timestamp())
            all_partitions.append(
                f"  PARTITION {part_name} VALUES LESS THAN ({unix_ts}) ENGINE = InnoDB"
            )

        # Add the after partition back.
        # Re-derive the timestamp from the date string (UTC) so all boundaries in
        # the command are computed consistently — the stored unix_ts from a
        # non-UTC MySQL server would otherwise produce mixed boundaries.
        part_name = self.generate_partition_name(after_part["date"])
        after_unix_ts = int(after_part["date"].replace(tzinfo=timezone.utc).timestamp())
        all_partitions.append(
            f"  PARTITION {part_name} VALUES LESS THAN ({after_unix_ts}) ENGINE = InnoDB"
        )

        cmd += ",\n".join(all_partitions)
        cmd += "\n);"

        return cmd

    def create_add_partition_command(self, missing_dates: List[datetime]) -> str:
        """Create ALTER TABLE ADD PARTITION command for tail-end missing partitions"""
        cmd = f"ALTER TABLE {self.table_name}\n"
        cmd += "ADD PARTITION (\n"

        all_partitions = []
        for missing_date in missing_dates:
            part_name = self.generate_partition_name(missing_date)
            unix_ts = int(missing_date.replace(tzinfo=timezone.utc).timestamp())
            all_partitions.append(
                f"  PARTITION {part_name} VALUES LESS THAN ({unix_ts}) ENGINE = InnoDB"
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
        self.print_output("\nExisting Partitions:")
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
                "\n✅ No missing dates found. Partitions are continuous."
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
        raise ParseError(f"File '{filename}' not found")
    except ParseError:
        raise
    except Exception as e:
        raise ParseError(f"Error parsing file: {e}") from e

    return tables_data


def main():
    parser = argparse.ArgumentParser(description="Partition Gap Checker")
    parser.add_argument("input_file", nargs="?", default="result.txt",
                        help="Input file with partition data (default: result.txt)")
    parser.add_argument("--reference-date", metavar="YYYY-MM-DD",
                        help="Reference date for tail-end gap detection (default: today UTC)")
    args = parser.parse_args()

    input_filename = args.input_file
    reference_date = None
    if args.reference_date:
        try:
            reference_date = datetime.strptime(args.reference_date, "%Y-%m-%d")
        except ValueError:
            print("❌ Error: --reference-date must be in YYYY-MM-DD format")
            sys.exit(1)

    analysis_filename = "partition_analysis.txt"
    sql_filename = "partition_fix.sql"

    print(f"{'=' * 70}")
    print(f"PARTITION CHECKER - Reading from {input_filename}")
    print(f"{'=' * 70}\n")

    # Parse input file
    print(f"📖 Reading partition data from '{input_filename}'...")
    try:
        tables_data = parse_result_file(input_filename)
    except ParseError as e:
        print(f"❌ {e}")
        sys.exit(1)

    if not tables_data:
        print(f"❌ No table data found in '{input_filename}'")
        print("\nExpected format:")
        print("table_name")
        print(
            "from_unixtime(PARTITION_DESCRIPTION) PARTITION_DESCRIPTION PARTITION_ORDINAL_POSITION TABLE_ROWS"
        )
        print("2025-10-09 00:00:00 1759964400 90 0")
        print("2025-10-08 00:00:00 1759878000 89 0")
        sys.exit(1)

    print(f"✅ Found {len(tables_data)} table(s): {', '.join(tables_data.keys())}\n")

    # Open output files
    with (
        open(analysis_filename, "w", encoding="utf-8") as analysis_file,
        open(sql_filename, "w", encoding="utf-8") as sql_file,
    ):
        # Write headers
        analysis_file.write(f"{'=' * 70}\n")
        analysis_file.write("PARTITION ANALYSIS REPORT\n")
        analysis_file.write(
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        analysis_file.write(f"Source: {input_filename}\n")
        analysis_file.write(f"{'=' * 70}\n")

        sql_file.write(f"-- {'=' * 68}\n")
        sql_file.write("-- PARTITION FIX SQL COMMANDS\n")
        sql_file.write(
            f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        )
        sql_file.write(f"-- Source: {input_filename}\n")
        sql_file.write(f"-- {'=' * 68}\n")
        sql_file.write("-- \n")
        sql_file.write("-- ⚠️  WARNING: REVIEW CAREFULLY BEFORE EXECUTION!\n")
        sql_file.write("-- \n")
        sql_file.write("-- Recommendations:\n")
        sql_file.write("--   1. Backup your database before making changes\n")
        sql_file.write("--   2. Verify partition names match your naming convention\n")
        sql_file.write("--   3. Check Unix timestamps are correct for your timezone\n")
        sql_file.write("--   4. Test on a non-production environment first\n")
        sql_file.write("--   5. Execute commands one at a time and verify results\n")
        sql_file.write(f"-- {'=' * 68}\n\n")

        print(f"{'=' * 70}")
        print("PARTITION ANALYSIS REPORT")
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

            checker = PartitionChecker(table_name, partition_data, analysis_file, reference_date)
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
        summary += "SUMMARY\n"
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
            sql_file.write("-- SUMMARY\n")
            sql_file.write(f"-- {'=' * 68}\n")
            sql_file.write(f"-- Total tables with issues: {tables_with_issues}\n")
            sql_file.write(f"-- Total SQL commands: {len(all_sql_commands)}\n")
            sql_file.write(f"-- {'=' * 68}\n")

    # Final output
    print("\n📄 Output files created:")
    print(f"   ✅ {analysis_filename} - Detailed analysis report")
    print(f"   ✅ {sql_filename} - SQL commands ready for execution")

    if tables_with_issues > 0:
        print(f"\n⚠️  {tables_with_issues} table(s) require attention!")
        print(f"   Review '{sql_filename}' before executing commands.")
    else:
        print("\n✅ All tables are OK! No missing partitions detected.")

    print()


if __name__ == "__main__":
    main()
