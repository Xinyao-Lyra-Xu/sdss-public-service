import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
Path("outputs").mkdir(exist_ok=True)

GOLD_PATH = Path("data/gold/mart_location_day.csv")

df = pd.read_csv(GOLD_PATH)
df["OCCUPANCY_DATE"] = pd.to_datetime(df["OCCUPANCY_DATE"], errors="coerce")

# Auto-detect location column
location_candidates = [c for c in df.columns if "POSTAL" in c or c.upper() == "LOCATION"]
if not location_candidates:
    raise KeyError(f"No location-like column found. Columns: {df.columns.tolist()}")

location_col = location_candidates[0]
print("Using location column:", location_col)

print("Rows:", len(df))
print("Unique locations:", df[location_col].nunique())
print("Date range:", df["OCCUPANCY_DATE"].min(), "to", df["OCCUPANCY_DATE"].max())

top_pressure = (
    df.groupby(location_col)["p95_utilization_28d"]
      .max()
      .sort_values(ascending=False)
      .head(10)
)

print("\nTop 10 locations by p95 utilization:")
print(top_pressure)

full_locations = (
    df.groupby(location_col)["full_rate_7d"]
      .mean()
      .sort_values(ascending=False)
      .head(10)
)

print("\nTop 10 consistently full locations:")
print(full_locations)

sample_locations = df[location_col].dropna().unique()[:3]

plt.figure(figsize=(10, 6))
for loc in sample_locations:
    temp = df[df[location_col] == loc].sort_values("OCCUPANCY_DATE")
    plt.plot(temp["OCCUPANCY_DATE"], temp["utilization"], label=str(loc))

plt.axhline(0.95, linestyle="--")
plt.title("Utilization Over Time (Sample Locations)")
plt.legend()
plt.tight_layout()
plt.savefig("outputs/qa_utilization_samples.png", dpi=200)
plt.show()

# ---------------------------
# System-level analysis plot
# ---------------------------

SYSTEM_PATH = Path("data/gold/mart_system_day.csv")

if SYSTEM_PATH.exists():
    sys_df = pd.read_csv(SYSTEM_PATH)
    sys_df["OCCUPANCY_DATE"] = pd.to_datetime(sys_df["OCCUPANCY_DATE"])

    pivot_df = (
        sys_df.pivot(
            index="OCCUPANCY_DATE",
            columns="location_type",
            values="utilization"
        )
        .sort_index()
    )

    plt.figure(figsize=(10, 6))

    if "fixed_location" in pivot_df.columns:
        plt.plot(
            pivot_df.index,
            pivot_df["fixed_location"],
            label="Fixed Locations"
        )

    if "temporary_or_unassigned" in pivot_df.columns:
        plt.plot(
            pivot_df.index,
            pivot_df["temporary_or_unassigned"],
            label="Temporary / Unassigned"
        )

    plt.title("System Utilization: Fixed vs Temporary Capacity")
    plt.legend()
    plt.tight_layout()
    plt.savefig("outputs/system_utilization_comparison.png", dpi=200)
    plt.show()