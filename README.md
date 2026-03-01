# Shelter Occupancy & Capacity — Operational Pressure Analysis  
Toronto Shelter & Overnight Services (2024–2025)

---

## Project Overview

This project analyzes Toronto’s shelter and overnight service system using daily operational data from 2024–2025.

The objective is to:

- Assess system pressure and capacity utilization  
- Identify chronically strained locations  
- Measure operational fragility and peak risk  
- Simulate demand scenarios to evaluate resilience  
- Propose data-driven recommendations for improving system stability  

This project is designed to support operational planning and capacity management decisions.

---

## Project Structure

SDSS-PUBLIC-SERVICE/

│  
├── data/  
│   ├── raw/      # Original Excel dataset  
│   ├── bronze/   # Append-only ingestion layer  
│   ├── silver/   # Cleaned & quality-checked dataset  
│   └── gold/     # Analytical marts (location-day & system-day)  
│  
├── notebooks/  
│   └── 00_public_services_analysis.ipynb  # Main analysis notebook  
│  
├── src/  
│   ├── etl/  
│   │   ├── ingest_bronze.py  
│   │   └── clean_silver.py  
│   │  
│   ├── features/  
│   │   ├── build_mart_location_day.py  
│   │   └── build_mart_system_day.py  
│   │  
│   └── analysis/  
│       └── quick_qa.py  
│  
├── outputs/      # Saved figures  
│  
├── requirements.txt  
└── README.md  

---

## Data Pipeline

The project follows a layered analytics engineering approach:

### Bronze Layer
- Append-only ingestion from raw Excel file  
- Adds ingestion metadata (timestamp, load ID)

### Silver Layer
- Data cleaning and standardization  
- Duplicate removal  
- Data quality flags  
- Canonical utilization metrics  
- Capacity loss computation  

### Gold Layer

Two analytical marts are created:

1. Location-Day Mart  
   - Utilization  
   - 7-day and 28-day rolling KPIs  
   - Volatility metrics  
   - Peak (p95) pressure indicators  

2. System-Day Mart  
   - Aggregated fixed vs temporary capacity  
   - System-level utilization trends  

---

## Analytical Approach

### 1. Chronic Pressure Analysis
- Mean utilization by location  
- 7-day near-full rate  
- Rolling 28-day p95 utilization  

### 2. Operational Fragility
- Rolling volatility (standard deviation)  
- Resilience gap (peak vs average utilization)  

### 3. Capacity-Pressure Index (Interpretable Model)

A composite operational pressure model combining:

- Long-run utilization (40%)  
- Peak pressure (30%)  
- Volatility (20%)  
- Capacity loss (10%)  

This index ranks locations by overall operational strain.

### 4. Scenario Simulation

Simulated a +5% demand increase to evaluate:

- Increase in near-full days  
- Increase in over-capacity days  
- Sensitivity of high-risk locations  

This approach assesses how close the system operates to critical thresholds.

---

## How to Reproduce

1. Install dependencies:

pip install -r requirements.txt

2. Open and run:

notebooks/00_public_services_analysis.ipynb

The notebook executes the full pipeline and generates all analytical outputs.

---

## Key Findings

- The shelter system frequently operates near or above 95% utilization.  
- Several locations exhibit structural (chronic) capacity strain.  
- Peak utilization significantly exceeds average levels in fragile locations.  
- A modest +5% demand increase pushes the system closer to critical thresholds.  
- Capacity loss compounds pressure in already strained locations.  

---

## Alignment with Case Objectives

This project directly addresses:

- System pressure assessment  
- Capacity utilization analysis  
- Operational risk detection  
- Scenario-based resilience modeling  

The analysis translates administrative data into actionable planning insights.

---

## Team

Xinyao Xu, Shengyang Zhao

---

## Deliverables

- Fully reproducible Jupyter notebook  
- Modular ETL pipeline  
- Analytical marts for decision support  
- Interpretable operational pressure model  
- Scenario simulation results  
