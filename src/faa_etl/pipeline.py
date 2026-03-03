from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DATA_RAW = ROOT / "data" / "raw"
DATA_OUT = ROOT / "data" / "output"
CONFIG = ROOT / "configs" / "filters.yaml"


@dataclass(frozen=True)
class Filters:
    aircraft_make: str
    aircraft_models: List[str]
    registered_on_or_before: str  # YYYY-MM-DD
    state_allowlist: List[str]
    export_include_pii: bool
    export_columns: List[str]


def load_filters(path: Path) -> Filters:
    # Keep deps minimal: parse your simple YAML manually (no pyyaml required)
    # Assumes the exact structure you showed earlier.
    lines = [ln.rstrip("\n") for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip() and not ln.strip().startswith("#")]
    d: Dict[str, Any] = {}
    export: Dict[str, Any] = {}

    def parse_list(s: str) -> List[str]:
        # ["OH","IN"] style
        s = s.strip()
        if not (s.startswith("[") and s.endswith("]")):
            return []
        inner = s[1:-1].strip()
        if not inner:
            return []
        parts = [p.strip() for p in inner.split(",")]
        return [p.strip().strip('"').strip("'") for p in parts]

    i = 0
    while i < len(lines):
        ln = lines[i]
        if ln.startswith("export:"):
            i += 1
            while i < len(lines) and lines[i].startswith("  "):
                k, v = lines[i].strip().split(":", 1)
                v = v.strip()
                if k == "include_pii":
                    export[k] = (v.lower() == "true")
                elif k == "columns":
                    export[k] = parse_list(v)
                i += 1
            continue
        if ":" in ln:
            k, v = ln.split(":", 1)
            k = k.strip()
            v = v.strip()
            if v.startswith('"') or v.startswith("'"):
                v = v.strip('"').strip("'")
            if k in ("aircraft_models", "state_allowlist"):
                d[k] = parse_list(v)
            else:
                d[k] = v
        i += 1

    return Filters(
        aircraft_make=str(d.get("aircraft_make", "")).upper().strip(),
        aircraft_models=[m.upper().strip() for m in d.get("aircraft_models", [])],
        registered_on_or_before=str(d.get("registered_on_or_before", "")).strip(),  # YYYY-MM-DD
        state_allowlist=[s.upper().strip() for s in d.get("state_allowlist", [])],
        export_include_pii=bool(export.get("include_pii", False)),
        export_columns=list(export.get("columns", [])),
    )


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Drop pandas' trailing "Unnamed: X" columns from the FAA CSVs
    return df.loc[:, ~df.columns.str.contains(r"^Unnamed", na=False)].copy()


def normalize_str_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip().str.upper()
    return df


def date_yyyymmdd_from_iso(iso_yyyy_mm_dd: str) -> str:
    # "2011-12-31" -> "20111231"
    return iso_yyyy_mm_dd.replace("-", "")


def run(filters: Filters) -> Path:
    master_path = DATA_RAW / "MASTER.txt"
    acftref_path = DATA_RAW / "ACFTREF.txt"

    # Read as strings (keeps leading zeros / blanks stable)
    master = pd.read_csv(master_path, dtype=str)
    acft = pd.read_csv(acftref_path, dtype=str)

    master = clean_columns(master)
    acft = clean_columns(acft)

    merged = master.merge(
        acft,
        left_on="MFR MDL CODE",
        right_on="CODE",
        how="left",
    )

    merged = normalize_str_cols(
        merged,
        ["MFR", "MODEL", "STATE", "TYPE REGISTRANT", "CERT ISSUE DATE", "YEAR MFR"],
    )

    # 1) registrant type: Individual only
    df = merged[merged["TYPE REGISTRANT"] == "1"]

    # 2) make
    df = df[df["MFR"] == filters.aircraft_make]

    # 3) model list: exact match against ACFTREF MODEL
    if filters.aircraft_models:
        df = df[df["MODEL"].isin(filters.aircraft_models)]

    # 4) geography: state allowlist
    if filters.state_allowlist:
        df = df[df["STATE"].isin(filters.state_allowlist)]

    # 5) registration / cert issue date (MASTER "CERT ISSUE DATE" is YYYYMMDD string)
    cutoff = date_yyyymmdd_from_iso(filters.registered_on_or_before)
    df = df[df["CERT ISSUE DATE"].str.fullmatch(r"\d{8}", na=False)]
    df = df[df["CERT ISSUE DATE"] <= cutoff]

    # Export mapping (your yaml keys -> your requested column names)
    out = pd.DataFrame(
        {
            "n_number": df["N-NUMBER"].astype(str).str.strip(),
            "aircraft_year": df["YEAR MFR"].astype(str).str.strip(),
            "make": df["MFR"].astype(str).str.strip(),
            "model": df["MODEL"].astype(str).str.strip(),
            "state": df["STATE"].astype(str).str.strip(),
            "cert_issue_date": df["CERT ISSUE DATE"].astype(str).str.strip(),
        }
    )

    # Respect export.columns order
    cols = filters.export_columns or list(out.columns)
    out = out[[c for c in cols if c in out.columns]]

    DATA_OUT.mkdir(parents=True, exist_ok=True)
    out_path = DATA_OUT / f"{filters.aircraft_make.lower()}_172_individuals_pre{cutoff}.csv"
    out.to_csv(out_path, index=False)

    print("Pipeline complete.")
    print("Output:", out_path)
    print("Row count:", len(out))
    return out_path


def main() -> None:
    filters = load_filters(CONFIG)
    run(filters)


if __name__ == "__main__":
    main()
