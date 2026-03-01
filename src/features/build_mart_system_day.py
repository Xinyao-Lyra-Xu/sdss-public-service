import numpy as np
import pandas as pd
from pathlib import Path

SILVER_PATH = Path("data/silver/silver_shelter_occupancy_clean.csv")
GOLD_PATH = Path("data/gold/mart_system_day.csv")

def safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    denom2 = denom.replace(0, np.nan)
    return numer / denom2

def main() -> None:
    df = pd.read_csv(SILVER_PATH)
    df["OCCUPANCY_DATE"] = pd.to_datetime(df["OCCUPANCY_DATE"], errors="coerce")

    df["location_type"] = np.where(
        df["LOCATION_POSTAL_CODE"].isna(),
        "temporary_or_unassigned",
        "fixed_location"
    )

    g = (
        df.groupby(["OCCUPANCY_DATE", "location_type"], as_index=False)
          .agg(
              occupied_sum=("OCCUPIED_CAPACITY", "sum"),
              actual_sum=("ACTUAL_CAPACITY", "sum"),
              unavailable_sum=("UNAVAILABLE_CAPACITY", "sum"),
              n_rows=("OCCUPIED_CAPACITY", "size"),
          )
    )

    g["total_capacity_sum"] = g["actual_sum"] + g["unavailable_sum"]
    g["utilization"] = safe_div(g["occupied_sum"], g["actual_sum"])
    g["capacity_loss_rate"] = safe_div(g["unavailable_sum"], g["total_capacity_sum"])

    GOLD_PATH.parent.mkdir(parents=True, exist_ok=True)
    g.to_csv(GOLD_PATH, index=False)

    print("System-day mart created successfully.")
    print("Rows:", len(g))
    print("Saved to:", GOLD_PATH)

if __name__ == "__main__":
    main()