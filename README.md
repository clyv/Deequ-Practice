# 🚕 Data Quality at Scale: PyDeequ + NYC Yellow Taxi

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/clyv/Deequ-Practice/blob/main/notebooks/project1.ipynb)

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
- Traces correlated failures back to a single upstream feed
- Cleans the data and re-runs the identical suite to prove the fix

---

## 📊 Results

| Metric | Value |
|---|---|
| Months analyzed | Sep 2025, Oct 2025, Nov 2025 |
| Total rows validated | **12,861,158** |
| Constraint checks run | 12 |
| ✅ Checks passed | 6 |
| 🚨 Checks failed | 6 |

| Month | Rows |
|---|---|
| Sep 2025 | 4,251,015 |
| Oct 2025 | 4,428,699 |
| Nov 2025 | 4,181,444 |

### Real Data Quality Issues Found

Every percentage below is the share of the 12,861,158-row combined dataset that
violates the corresponding Deequ constraint.

| Issue | Rows Affected | % of Data |
|---|---|---|
| Invalid passenger count (outside 1–6, incl. NULL) | 3,137,285 | 24.39% |
| Zero or negative fare amount | 975,362 | 7.58% |
| Negative fare amount | 969,118 | 7.54% |
| Zero trip distance | 359,502 | 2.80% |
| Dropoff recorded before pickup | 187,267 | 1.46% |
| Negative tip amount | 296 | 0.002% |

**The headline finding — `passenger_count`.** Nearly a quarter of all trips fail
the `1 ≤ passenger_count ≤ 6` constraint, but almost none of them are
out-of-range values:

| Cause | Rows | % of Data |
|---|---|---|
| `passenger_count` is NULL | 3,072,822 | 23.89% |
| `passenger_count` outside 1–6 | 64,463 | 0.50% |

So this is not a validation problem at the edges — it is an upstream collection
failure that stopped populating the field for roughly 1 in 4 trips. A row-count
filter that ignores NULLs would report this as a 0.50% issue and miss it
entirely; Deequ's `satisfies()` treats a NULL predicate as a violation and
surfaces the real 24.39%.

That turned out to be the thread worth pulling — see
[Root cause](#root-cause-four-of-those-six-issues-are-one-upstream-feed) below.

### Drift Detected Across 3 Months

| Metric | Sep 2025 | Nov 2025 | Change |
|---|---|---|---|
| Mean fare amount | $19.20 | $17.12 | -10.86% |
| Mean trip distance | 6.84 mi | 6.53 mi | -4.51% |
| `passenger_count` completeness | 0.7490 | 0.7573 | +0.0084 |

Mean fare falling 10.9% across three consecutive months, while trip
distance falls only 4.5%, is exactly the kind of divergence a metrics
store is built to catch — fare per mile moved, and nothing in a row-count check
would have shown it.

> **Note on schema:** the 2025 files carry a `cbd_congestion_fee` column (20
> columns) that the 2024 files do not (19 columns) — NYC's congestion pricing
> policy landing in the data. It is present in all three months analysed here,
> so it shows up as a year-over-year schema difference in the column profiler
> rather than as drift within this window.

---

### Root cause: four of those six issues are one upstream feed

Deequ reports *what* fails. It does not say whether the failures are
independent — and here they are not.

Five columns are NULL in **exactly the same 3,072,822 rows**. Not correlated:
identical. They are never missing individually.

| Column | NULL rows |
|---|---|
| `passenger_count` | 3,072,822 |
| `congestion_surcharge` | 3,072,822 |
| `RatecodeID` | 3,072,822 |
| `store_and_fwd_flag` | 3,072,822 |
| `Airport_fee` | 3,072,822 |

Those same rows also carry `payment_type = 0` — a value the TLC data dictionary
does not define (it specifies 1–6). That makes `payment_type = 0` a usable
signature for the feed.

Attributing each quality issue back to it:

| Issue | Total | From this feed | Share |
|---|---|---|---|
| Negative fare amount | 969,118 | 774,879 | **80.0%** |
| Zero or negative fare | 975,362 | 776,669 | **79.6%** |
| Zero trip distance | 359,502 | 229,981 | **64.0%** |
| Invalid passenger count | 3,137,285 | 3,072,822 | **97.9%** |
| Dropoff before pickup | 187,267 | 591 | 0.3% |
| Negative tip amount | 296 | 1 | 0.3% |

So this is not six defects to trace separately. It is **one upstream feed
producing roughly a quarter of all records with a block of fields unset**, plus
two genuinely independent problems (timestamp ordering and negative tips) that
need their own fix. That distinction is the difference between six tickets and
one.

---

## 🧹 Cleaning and Revalidation

Detect → quantify → **clean → revalidate**. Which rows to drop is a judgement
call, so the notebook compares two strategies rather than assuming one.

| Strategy | Rows kept | Share |
|---|---|---|
| **Strict** — enforce all 12 constraints | 9,230,244 | 71.77% |
| **Lenient** — trip validity only, keep NULL `passenger_count` | 11,426,085 | 88.84% |

Strict discards **2,195,841 additional rows whose only fault is a missing
`passenger_count`** — a field that says nothing about whether the trip itself
happened. Those rows have a real fare, distance and timestamps. Dropping ~17% of
the dataset to satisfy a constraint about passenger headcount would destroy
usable revenue data to make a dashboard look clean.

The pipeline therefore keeps them, re-runs the **identical** `VerificationSuite`
against the cleaned frame, and prints before/after compliance per constraint.
`passenger_count` is expected to still fail — that is the honest result. Cleaning
fixed the trip-level defects; the missing-field problem belongs upstream, not in
a filter.

---

## 🚦 Congestion Pricing (2025)

NYC began charging for entry to the Manhattan CBD in 2025, and the charge appears
in the trip record as its own `cbd_congestion_fee` column.

| | |
|---|---|
| Trips charged | 9,303,740 (72.34%), flat $0.75 |
| Collected over three months | **$6,977,837** |
| Trips refunded (fee < 0) | 129,908 (1.01%) |

It is distinct from the older `congestion_surcharge` ($2.50, applied to 67.26% of
trips) — a trip can carry both, and conflating them double-counts.

Comparing trips that pay it against trips that do not, **on the cleaned data**:

| Metric | Pays CBD fee | Does not |
|---|---|---|
| Trips | 8,390,450 | 3,035,635 |
| Mean fare | $19.62 | $22.20 |
| Mean distance | 5.36 mi | 8.70 mi |
| Mean tip | $3.30 | $2.78 |
| Median fare per mile | **$7.53** | $6.83 |

Congestion-zone trips are **shorter but cost more per mile**, which is what a
cordon charge is designed to produce. They also tip better, so the higher
per-mile cost is not suppressing gratuities.

Demand peaks at **18:00** (764,176 pickups) and bottoms out at **04:00** (83,572)
— a 9.1× swing across the day.

> The refunded rows matter for the quality story too: 99.3% of trips with a
> negative congestion fee also carry a negative fare. They are voided and
> disputed trips, not corruption — which is why the cleaning layer removes them
> rather than treating them as a data bug to escalate.


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
    │  6. Root Cause Analysis       │  ──► Co-missingness / feed attribution
    │  7. Clean + Revalidate        │  ──► Before/after compliance
    │  8. Congestion Pricing EDA    │  ──► 2025 cbd_congestion_fee analysis
    └───────────────────────────────┘
```

**Production equivalent on AWS:** See [`docs/aws-deployment-guide.md`](docs/aws-deployment-guide.md)

---

## 📁 Repository Structure

```
Deequ-Practice/
├── notebooks/
│   └── project1.ipynb          ← Full pipeline (setup → report)
├── docs/
│   └── aws-deployment-guide.md ← Production AWS architecture
├── results/
│   ├── metrics/                ← Persisted Deequ metrics (taxi_metrics.json)
│   └── reports/                ← Exported quality reports (CSV + README snippet)
├── .gitignore
└── README.md
```

> Source `.parquet` files are **not** committed — they are 50–60 MB each and the
> notebook downloads them on demand from the public TLC endpoint.

---

## 🚀 How to Run

### Option 1 — Google Colab (recommended)

1. Click the **Open in Colab** badge at the top of this README (or open `notebooks/project1.ipynb` in [Google Colab](https://colab.research.google.com))
2. Runtime → Run all
3. **Setup 1 restarts the runtime once, on purpose** — this interrupts "Run all"
4. Runtime → Run all again. Setup 1 detects its sentinel file and skips straight through

The restart is required: Colab ships a PySpark 4.x that has to be removed
before 3.5 will import, and the old modules stay in `sys.modules` until the
kernel restarts. Setup 2 then prints the resolved Java/Python/Spark versions and
names any mismatch before the JVM is started.

Expect a few minutes on the first pass while Java, Spark and the Deequ JAR
download.

### Environment compatibility (read this first)

PyDeequ sits on top of a JVM library, so four versions are coupled. Getting any
one wrong surfaces as `JAVA_GATEWAY_EXITED` or `JavaPackage object is not
callable` — neither of which names the real cause.

| Component | Constraint | Why |
|---|---|---|
| **PyDeequ** | `SPARK_VERSION=3.5` only | `pydeequ/configs.py` raises on any other value |
| **Deequ JAR** | `2.0.21-spark-3.5` | Scala 2.12 build, selected by PyDeequ |
| **Spark** | **3.5.x** | Spark 4.x is Scala 2.13 and cannot load the 2.12 JAR |
| **Java** | 8, 11 or 17 | Spark 4 dropped Java 8; Spark 3.5 supports all three |
| **Python** | 3.8–3.11 | PySpark 3.5.x does not declare 3.12/3.13 support |

The common failure is pairing **Java 8 with PySpark 4.0.0**: Spark 4 requires
Java 17+, so the JVM exits before Py4J can read its port. Raising Java alone
does not fix it either — Spark 4 is Scala 2.13, so the Deequ JAR still will not
load. Pin Spark to 3.5.

### Runtime requirements

**Use the standard CPU runtime — not a GPU one.** Spark and Deequ are CPU/JVM
work; there is no GPU path in this stack without the RAPIDS plugin, so a T4
gives you the same ~12.7 GB of system RAM plus idle VRAM and a spent quota.

What matters is heap. In `local[*]` mode the driver JVM is also the executor, so
`spark.driver.memory` is the whole budget for ~13M rows — and the default is
1 GB, which shows up as a dead kernel rather than a Spark error. Setup 3 raises
it to 6 GB via `PYSPARK_SUBMIT_ARGS` (setting it on `SparkSession.builder` does
nothing in local mode, because the JVM has already launched by then) and prints
the heap it actually got.

If the runtime still dies, drop to two months in the loader cell before reaching
for a bigger machine.

**Two Colab-specific hazards**, both handled in the setup cells:

- **Colab's default `java` is 21.** Spark 3.5 supports 8/11/17 only, and
  `apt install openjdk-11` does *not* repoint `/usr/bin/java` — so following the
  symlink silently selects an unsupported JDK. Setup 2 picks the JDK directory
  directly instead.
- **Colab ships a PySpark 4.x.** Downgrading over it leaves a mixed tree, where
  3.5's `pyspark/pandas/internal.py` imports `get_column_class` from a 4.0
  `pyspark/sql/utils.py` that no longer defines it. Setup 1 deletes the package
  directory and restarts the runtime; Setup 2 detects the condition explicitly
  if it recurs.

> **Python version:** Colab currently runs Python 3.13, ahead of what PySpark
> 3.5 declares (3.8–3.11). This pipeline does all of its work in the JVM — no
> Python UDFs — so it generally runs anyway, and the preflight reports the
> mismatch rather than failing obscurely. If you hit a Python serialization
> error, that is the cause; use an environment with Python 3.11.

### Option 2 — Local Spark

```bash
# Prerequisites: Java 11 (8 or 17 also work), Python 3.8–3.11
pip install "pyspark==3.5.9" "pydeequ==1.6.0"
export SPARK_VERSION=3.5   # PyDeequ reads this to pick its Deequ JAR

# Download data into the repo root
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-09.parquet -O sep_2025.parquet
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-10.parquet -O oct_2025.parquet
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet -O nov_2025.parquet

# Run the notebook
jupyter notebook notebooks/project1.ipynb
```

The notebook resolves its own project root, so it runs correctly whether the
working directory is the repo root (Colab) or `notebooks/` (local Jupyter). If
the 2025 files are absent it falls back to any `yellow_tripdata_2024-0{1,2,3}.parquet`
present in the repo root.

### Outputs

A full run writes:

| Path | Contents |
|---|---|
| `results/metrics/taxi_metrics.json` | Deequ metrics store — one tagged entry per month, appended each run |
| `results/metrics/anomaly_trends.png` | 2×2 drift chart (fare, distance, volume, completeness) |
| `results/reports/constraint_results_*.csv` | Every constraint with pass/fail and compliance ratio |
| `results/reports/column_profile_*.csv` | Per-column completeness, dtype, distinct count, min/max |
| `results/reports/issue_counts_*.csv` | Row counts and percentages per quality issue |
| `results/reports/README_results_snippet.md` | The Results tables above, pre-rendered for pasting |

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| [PyDeequ](https://github.com/awslabs/python-deequ) | Data quality checks, profiling, metrics store |
| PySpark 3.5.9 | Distributed DataFrame processing |
| Amazon Deequ 2.0.21-spark-3.5 (JVM) | Underlying Scala 2.12 engine behind PyDeequ |
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

TLC publishes monthly with roughly a two-month lag. If a download returns a
0-byte file that month is not published yet — step the URLs back one month.
