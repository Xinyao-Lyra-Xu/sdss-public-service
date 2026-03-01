# src/etl/clean_silver.py
import numpy as np
import pandas as pd
from pathlib import Path


BRONZE_PATH = Path("data/bronze/bronze_shelter_occupancy_raw.csv")
SILVER_PATH = Path("data/silver/silver_shelter_occupancy_clean.csv")

# Business key (defines the "true" grain of the dataset)
BUSINESS_KEYS = [
    "OCCUPANCY_DATE",
    "LOCATION_POSTAL_CODE",
    "SECTOR",
    "OVERNIGHT_SERVICE_TYPE",
    "PROGRAM_MODEL",
    "PROGRAM_AREA",
    "CAPACITY_TYPE",
]

NUMERIC_COLS = [
    "ACTUAL_CAPACITY",
    "OCCUPIED_CAPACITY",
    "UNAVAILABLE_CAPACITY",
    "OCCUPANCY_RATE",
]


def safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    """Vectorized safe division: returns NaN when denom is 0/NaN."""
    denom2 = denom.replace(0, np.nan)
    return numer / denom2


def main() -> None:
    if not BRONZE_PATH.exists():
        raise FileNotFoundError(f"Bronze file not found: {BRONZE_PATH.resolve()}")

    df = pd.read_csv(BRONZE_PATH)

    # -------------------------
    # Type casting / standardize
    # -------------------------
    if "OCCUPANCY_DATE" in df.columns:
        df["OCCUPANCY_DATE"] = pd.to_datetime(df["OCCUPANCY_DATE"], errors="coerce").dt.date
    else:
        raise KeyError("Missing required column: OCCUPANCY_DATE")

    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Standardize string dimensions (trim + keep as string)
    dim_cols = [c for c in BUSINESS_KEYS if c != "OCCUPANCY_DATE"]
    for c in dim_cols:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    # ingestion_time might be string; parse if present for sorting
    if "ingestion_time" in df.columns:
        df["ingestion_time"] = pd.to_datetime(df["ingestion_time"], errors="coerce")
        df = df.sort_values("ingestion_time", ascending=False)
    else:
        # If missing, still proceed but dedupe becomes arbitrary
        df = df.copy()

    # -------------------------
    # Dedup by business key
    # -------------------------
    # mark duplicates BEFORE dropping
    df["is_duplicate"] = df.duplicated(subset=BUSINESS_KEYS, keep="first")
    df = df.drop_duplicates(subset=BUSINESS_KEYS, keep="first").reset_index(drop=True)

    # -------------------------
    # Data quality flags
    # -------------------------
    # Missing/invalid
    df["is_missing_capacity"] = df["ACTUAL_CAPACITY"].isna() | (df["ACTUAL_CAPACITY"] == 0)
    df["is_missing_occupied"] = df["OCCUPIED_CAPACITY"].isna()
    df["is_missing_unavailable"] = df["UNAVAILABLE_CAPACITY"].isna()

    # Negative values
    df["is_negative_any"] = (
        (df["ACTUAL_CAPACITY"] < 0)
        | (df["OCCUPIED_CAPACITY"] < 0)
        | (df["UNAVAILABLE_CAPACITY"] < 0)
    )

    # Over capacity (system pressure or reporting mismatch)
    df["is_over_capacity"] = df["OCCUPIED_CAPACITY"] > df["ACTUAL_CAPACITY"]

    # -------------------------
    # Canonical measures
    # -------------------------
    df["utilization_raw"] = safe_div(df["OCCUPIED_CAPACITY"], df["ACTUAL_CAPACITY"])
    df["utilization_capped"] = df["utilization_raw"].clip(lower=0, upper=1)

    # Compare provided OCCUPANCY_RATE (if exists) to recomputed utilization
    if "OCCUPANCY_RATE" in df.columns:
        df["is_rate_mismatch"] = (df["OCCUPANCY_RATE"] - df["utilization_raw"]).abs() > 0.01
    else:
        df["is_rate_mismatch"] = False

    # Capacity loss (if unavailable exists)
    df["total_capacity"] = df["ACTUAL_CAPACITY"] + df["UNAVAILABLE_CAPACITY"]
    df["capacity_loss_rate"] = safe_div(df["UNAVAILABLE_CAPACITY"], df["total_capacity"])

    # Full flag (>=95% utilization)
    df["full_flag"] = df["utilization_raw"] >= 0.95

    # -------------------------
    # Save silver
    # -------------------------
    SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(SILVER_PATH, index=False)

    # Quick QA printout
    print("Silver table created successfully.")
    print(f"Rows: {len(df)}")
    for c in ["is_duplicate", "is_missing_capacity", "is_negative_any", "is_over_capacity", "is_rate_mismatch"]:
        if c in df.columns:
            print(f"{c}: {df[c].mean():.4f} (share of rows)")


if __name__ == "__main__":
    main()