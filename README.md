# FAA Registry ETL (Pandas) — Cessna 172 Regional Filter

Small, reproducible ETL pipeline that downloads (manually), ingests, joins, filters, and exports a clean CSV slice from the FAA aircraft registry “Releasable” dataset.

**Use case (default config):**
- Make: **CESSNA**
- Model family: **172** variants (per `configs/filters.yaml`)
- Registrant type: **Individual**
- States: **OH, IN, KY, TN, FL, GA, SC, NC**
- Cert Issue Date cutoff: **<= 2011-12-31**
- Export columns only (PII excluded)

---

## Data source

FAA Aircraft Registry “Releasable” download (zip).  
You download it manually from the FAA site and place it under `data/raw/`.

Expected extracted files (subset):
- `MASTER.txt`
- `ACFTREF.txt`
- (others may exist; pipeline only needs the two above for the default join/filter)

---

## Project layout

```text
faa-registry-etl/
  configs/
    filters.yaml
  data/
    raw/        # put FAA files here (not committed)
    output/     # generated CSVs
  src/
    faa_etl/
      pipeline.py
