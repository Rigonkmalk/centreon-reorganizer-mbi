---
description: Environment configuration for MySQL connection
---

# Configuration

Copy `.env.example` to `.env` and configure the following variables:

| Variable           | Description                                                                 |
| ------------------ | --------------------------------------------------------------------------- |
| `MYSQL_HOST`       | MySQL server hostname                                                       |
| `MYSQL_USER`       | MySQL username                                                              |
| `MYSQL_PASSWORD`   | MySQL password                                                              |
| `MYSQL_DATABASE`   | Target database name                                                        |
| `MYSQL_MBI_TABLE`  | Comma-separated list of specific tables, or empty to analyze all `mod_bi_*` tables |

The `.env` file is only required when running `extract_result.sh` without `-f`. In `-f` mode
(file-based analysis), no MySQL connection is made and `.env` is not needed.
