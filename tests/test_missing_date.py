"""
Unit tests for missing_date.py — PartitionChecker and parse_result_file.

Framework: pytest
Reference date used throughout: 2026-02-20 (naive, no timezone — mirrors how
PartitionChecker stores its reference_date after stripping tzinfo).
"""

import pathlib
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from missing_date import PartitionChecker, parse_result_file

TESTS_DIR = pathlib.Path(__file__).parent
REFERENCE_DATE = datetime(2026, 2, 20)  # naive, matches PartitionChecker internals


# ── helpers ──────────────────────────────────────────────────────────────────


def _utc_ts(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp())


def _row(year: int, month: int, day: int, ordinal: int, rows: int = 0) -> tuple:
    """Build a (date_str, unix_ts_str, ordinal_str, rows_str) tuple."""
    dt = datetime(year, month, day)
    return (dt.strftime("%Y-%m-%d %H:%M:%S"), str(_utc_ts(year, month, day)), str(ordinal), str(rows))


def _checker(rows: list, reference_date: datetime = REFERENCE_DATE) -> PartitionChecker:
    return PartitionChecker("test_table", rows, reference_date=reference_date)


# ── PartitionChecker.find_missing_dates() ────────────────────────────────────

# Test 1
def test_find_missing_dates_empty_partitions():
    assert _checker([]).find_missing_dates() == []


# Test 2
def test_find_missing_dates_continuous_no_gaps():
    rows = [_row(2026, 2, 18, 1), _row(2026, 2, 19, 2), _row(2026, 2, 20, 3)]
    assert _checker(rows).find_missing_dates() == []


# Test 3
def test_find_missing_dates_single_mid_sequence_gap():
    # Jan 1, Jan 3 present — Jan 2 missing; reference == Jan 3 so no tail-end gap
    rows = [_row(2026, 1, 1, 1), _row(2026, 1, 3, 2)]
    missing = _checker(rows, reference_date=datetime(2026, 1, 3)).find_missing_dates()
    assert missing == [datetime(2026, 1, 2)]


# Test 4
def test_find_missing_dates_multi_day_consecutive_gap():
    # Jan 1, Jan 5 present — Jan 2, 3, 4 missing; reference == Jan 5
    rows = [_row(2026, 1, 1, 1), _row(2026, 1, 5, 2)]
    missing = _checker(rows, reference_date=datetime(2026, 1, 5)).find_missing_dates()
    assert missing == [datetime(2026, 1, 2), datetime(2026, 1, 3), datetime(2026, 1, 4)]


# Test 5
def test_find_missing_dates_tail_end_gap():
    # Last partition is Feb 18, reference is Feb 20 — Feb 19 and Feb 20 are tail-end missing
    rows = [_row(2026, 2, 17, 1), _row(2026, 2, 18, 2)]
    missing = _checker(rows).find_missing_dates()
    assert missing == [datetime(2026, 2, 19), datetime(2026, 2, 20)]


# Test 6
def test_find_missing_dates_no_tail_end_gap_when_last_equals_reference():
    rows = [_row(2026, 2, 19, 1), _row(2026, 2, 20, 2)]
    assert _checker(rows).find_missing_dates() == []


# Test 7
def test_find_missing_dates_both_mid_and_tail_end_gaps():
    # Feb 16, Feb 18 present → mid-gap: Feb 17; tail-end: Feb 19, Feb 20
    rows = [_row(2026, 2, 16, 1), _row(2026, 2, 18, 2)]
    missing = _checker(rows).find_missing_dates()
    assert missing == [datetime(2026, 2, 17), datetime(2026, 2, 19), datetime(2026, 2, 20)]


# ── PartitionChecker.group_consecutive_dates() ───────────────────────────────

# Test 8
def test_group_consecutive_empty():
    assert _checker([]).group_consecutive_dates([]) == []


# Test 9
def test_group_consecutive_single_date():
    d = datetime(2026, 1, 1)
    assert _checker([]).group_consecutive_dates([d]) == [[d]]


# Test 10
def test_group_consecutive_all_consecutive():
    dates = [datetime(2026, 1, d) for d in range(1, 5)]
    groups = _checker([]).group_consecutive_dates(dates)
    assert groups == [dates]


# Test 11
def test_group_consecutive_two_separate_ranges():
    dates = [datetime(2026, 1, 1), datetime(2026, 1, 2), datetime(2026, 1, 5), datetime(2026, 1, 6)]
    groups = _checker([]).group_consecutive_dates(dates)
    assert len(groups) == 2
    assert groups[0] == [datetime(2026, 1, 1), datetime(2026, 1, 2)]
    assert groups[1] == [datetime(2026, 1, 5), datetime(2026, 1, 6)]


# ── PartitionChecker.generate_partition_name() ───────────────────────────────

# Test 12
def test_generate_partition_name():
    assert _checker([]).generate_partition_name(datetime(2025, 2, 10)) == "p20250210"


# ── PartitionChecker.generate_reorganize_commands() ─────────────────────────

# Test 13
def test_generate_reorganize_commands_no_missing():
    rows = [_row(2026, 2, 19, 1), _row(2026, 2, 20, 2)]
    assert _checker(rows).generate_reorganize_commands([]) == []


# Test 14
def test_generate_reorganize_commands_mid_sequence_gap():
    # Feb 18, Feb 20 present; reference == Feb 20 → only Feb 19 missing (mid-sequence)
    rows = [_row(2026, 2, 18, 1), _row(2026, 2, 20, 2)]
    checker = _checker(rows, reference_date=datetime(2026, 2, 20))
    missing = checker.find_missing_dates()
    commands = checker.generate_reorganize_commands(missing)
    assert len(commands) == 1
    assert "REORGANIZE PARTITION" in commands[0]
    assert "p20260219" in commands[0]


# Test 15
def test_generate_reorganize_commands_tail_end_gap():
    # Feb 18, Feb 19 present; reference == Feb 20 → Feb 20 is tail-end missing
    rows = [_row(2026, 2, 18, 1), _row(2026, 2, 19, 2)]
    checker = _checker(rows)
    missing = checker.find_missing_dates()
    commands = checker.generate_reorganize_commands(missing)
    assert len(commands) == 1
    assert "ADD PARTITION" in commands[0]
    assert "p20260220" in commands[0]


# Test 16
def test_generate_reorganize_commands_both_gap_types():
    # Feb 16, Feb 18 → mid-gap Feb 17 (REORGANIZE); tail-end Feb 19, Feb 20 (ADD)
    rows = [_row(2026, 2, 16, 1), _row(2026, 2, 18, 2)]
    checker = _checker(rows)
    missing = checker.find_missing_dates()
    commands = checker.generate_reorganize_commands(missing)
    assert len(commands) == 2
    has_reorganize = any("REORGANIZE PARTITION" in c for c in commands)
    has_add = any("ADD PARTITION" in c for c in commands)
    assert has_reorganize
    assert has_add


# ── PartitionChecker.create_reorganize_command() ─────────────────────────────

# Test 17
def test_create_reorganize_command_single_missing_date():
    rows = [_row(2026, 2, 18, 1), _row(2026, 2, 20, 2)]
    checker = _checker(rows)
    before = checker.partitions[0]
    after = checker.partitions[1]
    sql = checker.create_reorganize_command(before, after, [datetime(2026, 2, 19)])
    assert "REORGANIZE PARTITION p20260220" in sql
    assert "p20260219" in sql
    # The after-partition boundary must be preserved in the INTO clause
    assert str(after["unix_ts"]) in sql


# ── PartitionChecker.create_add_partition_command() ──────────────────────────

# Test 18
def test_create_add_partition_command_two_dates():
    checker = _checker([_row(2026, 2, 18, 1)])
    sql = checker.create_add_partition_command([datetime(2026, 2, 19), datetime(2026, 2, 20)])
    assert "ADD PARTITION" in sql
    assert "p20260219" in sql
    assert "p20260220" in sql


# ── parse_result_file() ───────────────────────────────────────────────────────

# Test 19
def test_parse_result_file_ok_fixture():
    result = parse_result_file(str(TESTS_DIR / "result_ok.txt"))
    assert isinstance(result, dict)
    assert len(result) > 0
    assert "data_bin" in result
    assert len(result["data_bin"]) > 0


# Test 20
def test_parse_result_file_nok_fixture():
    result = parse_result_file(str(TESTS_DIR / "result_NOK.txt"))
    expected_tables = {
        "mod_bi_metriccentilemonthlyvalue",
        "mod_bi_hostavailability",
        "data_bin",
        "mod_bi_metrichourlyvalue",
        "mod_bi_metricdailyvalue",
        "mod_bi_serviceavailability",
    }
    assert expected_tables.issubset(set(result.keys()))


# Test 21
def test_parse_result_file_missing_file_exits(tmp_path):
    with pytest.raises(SystemExit):
        parse_result_file(str(tmp_path / "does_not_exist.txt"))
