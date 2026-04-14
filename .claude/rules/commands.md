---
description: Commands to run the tool and tests
---

# Commands

## Run the tool

```bash
bash extract_result.sh              # Extract from MySQL and analyze
bash extract_result.sh -f result.txt  # Analyze existing file (no MySQL)
bash extract_result.sh -x           # Delete all .txt and .sql files
```

## Run Python analysis directly

```bash
python3 missing_date.py result.txt
python3 missing_date.py result.txt --reference-date 2026-02-20
```

## Run the test suite

```bash
bash tests/run_tests.sh
```

## Regenerate expected output after intentional format changes

```bash
REGENERATE=1 bash tests/run_tests.sh
```

Commit the updated `tests/partition_analysis_*.txt` files alongside the code change.
