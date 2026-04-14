---
description: Project overview and key implementation details for Centreon MBI Missing Partition Fixer
---

# Project Overview

Centreon MBI Missing Partition Fixer — a tool to detect and fix missing date partitions in
Centreon MBI database tables (`mod_bi_*` and `data_bin`). It extracts partition metadata from
MySQL, identifies gaps in partition sequences, and generates SQL `REORGANIZE PARTITION` commands
to fill them.

## Key Implementation Details

- Partitions use daily granularity with Unix timestamps as boundaries
- Partition names follow the pattern `pYYYYMMDD` (e.g., `p20250210`)
- Missing dates are grouped into consecutive ranges for efficient `REORGANIZE PARTITION` commands
- The SQL generation splits existing partitions without data loss
- Tail-end gaps (dates after the last partition up to today) use `ADD PARTITION` instead of
  `REORGANIZE PARTITION`
