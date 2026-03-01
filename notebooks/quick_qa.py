import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

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
plt.show()