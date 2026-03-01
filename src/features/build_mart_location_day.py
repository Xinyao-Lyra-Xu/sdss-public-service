import numpy as np
import pandas as pd
from pathlib import Path

SILVER_PATH = Path("data/silver/silver_shelter_occupancy_clean.csv")
GOLD_PATH = Path("data/gold/mart_location_day.csv")

WIN_7 = 7
WIN_28 = 28


def mad(x: pd.Series) -> float:
    x = x.dropna()
    if len(x) == 0:
        return np.nan
    m = np.median(x)
    return np.median(np.abs(x - m))


def safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    denom2 = denom.replace(0, np.nan)
    return numer / denom2


def main() -> None:
    if not SILVER_PATH.exists():
        raise FileNotFoundError(f"Silver file not found: {SILVER_PATH.resolve()}")

    df = pd.read_csv(SILVER_PATH)

    # Parse date
    df["OCCUPANCY_DATE"] = pd.to_datetime(df["OCCUPANCY_DATE"], errors="coerce")

    # Use the actual location column name in SILVER
    location_col = "LOCATION_POSTAL_CODE"
    required = [location_col, "ACTUAL_CAPACITY", "OCCUPIED_CAPACITY", "UNAVAILABLE_CAPACITY"]
    for c in required:
        if c not in df.columns:
            raise KeyError(f"Missing required column in SILVER: {c}. Available: {df.columns.tolist()}")

    for c in ["ACTUAL_CAPACITY", "OCCUPIED_CAPACITY", "UNAVAILABLE_CAPACITY"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # -------------------------
    # Aggregate to location-day (keep keys as columns)
    # -------------------------
    g = (
        df.groupby(["OCCUPANCY_DATE", location_col], as_index=False)
          .agg(
              occupied_sum=("OCCUPIED_CAPACITY", "sum"),
              actual_sum=("ACTUAL_CAPACITY", "sum"),
              unavailable_sum=("UNAVAILABLE_CAPACITY", "sum"),
              n_rows=("OCCUPIED_CAPACITY", "size"),
              n_missing_capacity=("is_missing_capacity", "sum") if "is_missing_capacity" in df.columns else ("OCCUPIED_CAPACITY", "size"),
              n_over_capacity=("is_over_capacity", "sum") if "is_over_capacity" in df.columns else ("OCCUPIED_CAPACITY", "size"),
          )
    )

    if "is_missing_capacity" not in df.columns:
        g["n_missing_capacity"] = np.nan
    if "is_over_capacity" not in df.columns:
        g["n_over_capacity"] = np.nan

    g["total_capacity_sum"] = g["actual_sum"] + g["unavailable_sum"]
    g["utilization"] = safe_div(g["occupied_sum"], g["actual_sum"])
    g["utilization_capped"] = g["utilization"].clip(lower=0, upper=1)
    g["full_flag_95"] = g["utilization"] >= 0.95
    g["capacity_loss_rate"] = safe_div(g["unavailable_sum"], g["total_capacity_sum"])

    # -------------------------
    # Rolling KPIs per location
    # (IMPORTANT: do NOT drop group key)
    # -------------------------
    g = g.sort_values([location_col, "OCCUPANCY_DATE"]).reset_index(drop=True)

    def add_rollups(loc_df: pd.DataFrame) -> pd.DataFrame:
        loc_df = loc_df.copy()
        loc_df["utilization_7d_ma"] = loc_df["utilization"].rolling(WIN_7, min_periods=3).mean()
        loc_df["full_rate_7d"] = loc_df["full_flag_95"].rolling(WIN_7, min_periods=3).mean()
        loc_df["volatility_28d_std"] = loc_df["utilization"].rolling(WIN_28, min_periods=7).std()
        loc_df["volatility_28d_mad"] = loc_df["utilization"].rolling(WIN_28, min_periods=7).apply(mad, raw=False)
        loc_df["p95_utilization_28d"] = loc_df["utilization"].rolling(WIN_28, min_periods=7).quantile(0.95)
        loc_df["capacity_loss_rate_28d_ma"] = loc_df["capacity_loss_rate"].rolling(WIN_28, min_periods=7).mean()
        return loc_df

    # Rolling KPIs per location (ensure group key is preserved)
    out = (
        g.groupby(location_col)
        .apply(add_rollups)
        .reset_index()
    )

    # After reset_index(), pandas may name group key as "level_0"
    if location_col not in out.columns and "level_0" in out.columns:
        out = out.rename(columns={"level_0": location_col})

    # Drop extra helper columns created by apply/reset_index
    for col in ["level_1", "index"]:
        if col in out.columns:
            out = out.drop(columns=[col])

    # Save
    GOLD_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(GOLD_PATH, index=False)

    print("Gold mart created successfully.")
    print("Rows:", len(out))
    print("Saved to:", GOLD_PATH)


if __name__ == "__main__":
    main()