# FAA Aircraft Registry ETL (Config-Driven)

A small, reproducible ETL pipeline that ingests the FAA “Releasable” aircraft registry dataset,
joins reference tables, applies configurable filters, and exports a clean CSV slice.

The pipeline is config-driven and designed for deterministic, repeatable execution.

---

## Technical Highlights

- `src/` layout (package-based project structure)
- Config-driven filtering (YAML)
- Deterministic CSV output
- Explicit string normalization and type handling
- Pandas-based transformation layer
- No raw FAA dataset committed to the repository
- PII-safe export configuration

---

## Data Source

FAA Aircraft Registry “Releasable” dataset (ZIP download).

Download manually from the FAA website and extract into:

```
data/raw/
```

Expected files (minimum required):

- `MASTER.txt`
- `ACFTREF.txt`

The pipeline currently requires only these two files for the default join/filter process.

---

## Default Use Case (Example Configuration)

The included configuration (`configs/filters.yaml`) filters:

- Make: CESSNA
- Model family: 172 variants
- Registrant type: Individual
- State allowlist (regional example)
- Certificate issue date cutoff
- Export columns only (PII excluded)

All filtering behavior is controlled through the YAML configuration file.

---

## Run Instructions

1. Download the FAA Aircraft Registry “Releasable” ZIP.
2. Extract required files into:

```
data/raw/
```

3. Run:

```bash
PYTHONPATH=src python3 main.py
```

---

## Output

Filtered CSV output will be written to:

```
data/output/
```

File naming convention:

```
<make>_<model>_individuals_pre<YYYYMMDD>.csv
```

Example:

```
cessna_172_individuals_pre20111231.csv
```

---

## Project Structure

```
configs/        YAML filter configuration
data/raw/       Raw FAA input files (not committed)
data/output/    Generated CSV output
src/faa_etl/    ETL pipeline implementation
main.py         Entry point
```

---

## Notes

- Raw FAA data is intentionally excluded from version control.
- Output is fully reproducible given the same input files and configuration.
- Designed as a minimal, inspectable data engineering example using public data.
