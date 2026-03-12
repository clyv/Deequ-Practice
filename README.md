# 🚕 Data Quality at Scale: PyDeequ + NYC Yellow Taxi

> Automated data quality validation pipeline built with **PyDeequ (AWS Deequ)** and **PySpark**.  
> Validates millions of rows of real NYC taxi trip data across constraint checking, column profiling, and month-over-month anomaly detection.

---

## 📌 What This Project Does

Most data pipelines fail silently. A fare column goes negative, timestamps get corrupted, passenger counts stop populating — and downstream reports just quietly produce wrong numbers.

This project builds a **systematic data quality layer** using PyDeequ — the same tool Amazon uses internally and open-sourced via AWS Labs. It runs against NYC Yellow Taxi trip records and automatically:

- Validates data against business rules (constraint verification)
- Profiles every column for completeness, distribution, and type anomalies
- Tracks key metrics over time and flags drift between monthly loads
- Generates a human-readable quality report on every run

---

## 📊 Results

| Metric | Value |
|---|---|
| Months analyzed | Sep 2025, Oct 2025, Nov 2025 |
| Total rows validated | ~[FILL IN YOUR TOTAL ROW COUNT] |
| Constraint checks run | 12 |
| ✅ Checks passed | 6 |
| 🚨 Checks failed | 6 |

### Real Data Quality Issues Found

| Issue | Rows Affected | % of Data |
|---|---|---|
| Invalid passenger count (outside 1–6) | [FILL IN] | [FILL IN]% |
| Zero or negative fare amount | [FILL IN] | [FILL IN]% |
| Negative fare amount | [FILL IN] | [FILL IN]% |
| Zero trip distance | [FILL IN] | [FILL IN]% |
| Dropoff recorded before pickup | [FILL IN] | [FILL IN]% |
| Negative tip amount | [FILL IN] | [FILL IN]% |

### Drift Detected Across 3 Months
- Mean fare amount: $[FILL IN] → $[FILL IN] ([FILL IN]% change)
- Mean trip distance: [FILL IN] → [FILL IN] miles ([FILL IN]% change)
- Passenger count completeness: [FILL IN] → [FILL IN]

> **Note:** 2025 data includes the new `cbd_congestion_fee` column introduced by NYC's congestion pricing policy — a real schema change automatically surfaced by the column profiler.

---

## 🏗️ Architecture

```
NYC TLC Public Data (monthly .parquet files)
            │
            ▼
    PySpark DataFrame
            │
            ▼
    ┌───────────────────────────────┐
    │        PyDeequ Pipeline       │
    │                               │
    │  1. Constraint Verification   │  ──► Pass/Fail report per check
    │  2. Column Profiling          │  ──► Stats for every column
    │  3. Metrics Store             │  ──► Persisted JSON per month
    │  4. Anomaly Detection         │  ──► Drift charts across months
    │  5. Quality Report            │  ──► Consolidated summary
    └───────────────────────────────┘
```

**Production equivalent on AWS:** See [`docs/aws-deployment-guide.md`](docs/aws-deployment-guide.md)

---

## 📁 Repository Structure

```
deequ-nyc-taxi-quality/
├── notebooks/
│   └── project1.ipynb          ← Full pipeline (setup → report)
├── docs/
│   └── aws-deployment-guide.md ← Production AWS architecture
├── results/
│   ├── metrics/                ← Persisted Deequ metrics (JSON)
│   └── reports/                ← Exported quality reports (CSV)
└── README.md
```

---

## 🚀 How to Run

### Option 1 — Google Colab (recommended)
1. Open `notebooks/project1.ipynb` in [Google Colab](https://colab.research.google.com)
2. Run all cells from top to bottom
3. Data downloads automatically from the NYC TLC public endpoint

### Option 2 — Local Spark
```bash
# Prerequisites: Java 8+, Python 3.9+
pip install pyspark==3.3.0 pydeequ

# Download data
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-09.parquet
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-10.parquet
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet

# Run the notebook
jupyter notebook notebooks/project1.ipynb
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| [PyDeequ](https://github.com/awslabs/python-deequ) | Data quality checks, profiling, metrics store |
| PySpark 3.5 | Distributed DataFrame processing |
| Amazon Deequ (JVM) | Underlying Scala engine behind PyDeequ |
| Matplotlib | Drift visualization charts |
| NYC TLC Open Data | Source dataset (~3–4M rows/month) |

---

## ☁️ AWS Production Path

This exact pipeline maps directly to AWS Glue for billion-row scale:

```python
# Colab (this repo)
df = spark.read.parquet("sep_2025.parquet")
repository = FileSystemMetricsRepository(spark, "/tmp/metrics.json")

# AWS Glue (production) — same PyDeequ logic, different I/O
df = spark.read.parquet("s3://nyc-tlc/trip data/yellow_tripdata_*.parquet")
repository = FileSystemMetricsRepository(spark, "s3://your-bucket/deequ-metrics/")
```

See the full deployment guide → [`docs/aws-deployment-guide.md`](docs/aws-deployment-guide.md)

---

## 📚 Data Source

NYC Taxi & Limousine Commission — [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
Yellow Taxi Trip Records, September–November 2025 (Parquet format)
