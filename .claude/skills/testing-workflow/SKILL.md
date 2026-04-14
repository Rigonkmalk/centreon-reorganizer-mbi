---
name: testing-workflow
description: Mandatory testing workflow for all code changes in this repository
---

# Testing Workflow

Apply this workflow whenever writing or reviewing tests for changes in this repository.

## Steps (always follow in order)

### 1. Identify the test framework

Determine which stack is affected by the changes in the current branch:

| Stack                        | Framework       |
| ---------------------------- | --------------- |
| Python (`missing_date.py`)   | `pytest`        |
| Shell (`extract_result.sh`)  | `bash` + `diff` |
| Frontend TypeScript / React  | Cypress         |
| Rust                         | `cargo-nextest` |

State the identified framework explicitly in the plan before proceeding.

### 2. Enter plan mode

Use `EnterPlanMode` before writing any tests. Do not write code before the plan is approved.

### 3. Write the test plan

The plan must include:

- Which files/functions are under test
- Which framework will be used (from step 1)
- Test cases to cover: happy path, edge cases, error paths
- For `pytest`: location of new test file, fixture strategy, parametrize opportunities
- For snapshot tests: whether reference files need regenerating

### 4. Wait for approval

Do not proceed to implementation until the supervisor explicitly accepts the plan.
If changes are requested, revise the plan and wait again.

### 5. Write the tests

Implement tests exactly as described in the approved plan. Do not add untested cases or
refactor production code beyond what the plan covers.

### 6. Flag misconceptions

After writing tests, list as numbered items any unclear intent, wrong assumptions, or
design issues found in the code under test. Each item must include a concrete suggestion.
