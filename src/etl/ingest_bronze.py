import pandas as pd
from pathlib import Path
from datetime import datetime
import uuid

# =========================
# Paths
# =========================
RAW_PATH = Path("data/raw/public_services_dataset.xlsx")
BRONZE_PATH = Path("data/bronze/bronze_shelter_occupancy_raw.csv")

# =========================
# Load Raw Excel
# =========================
df = pd.read_excel(RAW_PATH)

# =========================
# Add Engineering Columns
# =========================
df["ingestion_time"] = datetime.utcnow()
df["load_id"] = str(uuid.uuid4())
df["source_file"] = RAW_PATH.name

# =========================
# Save Bronze (append-only)
# =========================
BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)

if BRONZE_PATH.exists():
    df.to_csv(BRONZE_PATH, mode="a", header=False, index=False)
else:
    df.to_csv(BRONZE_PATH, index=False)

print("Bronze table created successfully.")
print(f"Rows appended: {len(df)}")