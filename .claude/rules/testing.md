---
description: Test structure, fixtures, and CI for the partition fixer
---

# Testing

## Test suite

```bash
bash tests/run_tests.sh
```

## Test structure

Tests live in `tests/` and split into two categories:

### Python analysis tests (snapshot-based)

Run `missing_date.py` against fixture input files and diff the output against expected references.
A fixed `--reference-date 2026-02-20` is passed so results are deterministic.

| Input fixture         | Expected output                      | Scenario                    |
| --------------------- | ------------------------------------ | --------------------------- |
| `tests/result_ok.txt` | `tests/partition_analysis_ok.txt`    | No missing partitions       |
| `tests/result_NOK.txt`| `tests/partition_analysis_NOK.txt`   | Missing partitions detected |

The diff ignores lines starting with `Generated:` to avoid timestamp churn.

### Shell script tests

Validate `extract_result.sh` behavior without a MySQL connection:

- `-f <file>` mode works on valid fixture files (exit 0)
- Script exits non-zero on a missing input file

## Regenerating expected output

After an intentional change to `missing_date.py` output format:

```bash
REGENERATE=1 bash tests/run_tests.sh
```

Commit the updated `tests/partition_analysis_*.txt` files alongside the code change.

## CI

GitHub Actions runs the test suite on every pull request to `master`.
See `.github/workflows/ci.yml` (Python 3.13, Ubuntu latest, uv).

## Unit tests

`pytest` is the preferred framework for unit testing `missing_date.py` logic directly.
Run with:

```bash
pytest tests/
```
