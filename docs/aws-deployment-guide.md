# ☁️ AWS Production Deployment Guide

> This document describes how the PyDeequ pipeline in this repository maps to a  
> production-grade AWS architecture capable of validating billions of rows at scale.

---

## Overview

The pipeline built in `notebooks/project1.ipynb` runs on Google Colab against 12.9M rows of monthly NYC taxi data (Sep–Nov 2025). The core PyDeequ validation logic is **identical** in production — only the compute environment and data I/O change.

| Layer | This Repo (Dev) | Production (AWS) |
|---|---|---|
| Compute | Google Colab (single node) | AWS Glue (managed Spark cluster) |
| Data source | Local `.parquet` files | S3 bucket (`s3://nyc-tlc/`) |
| Metrics store | Local `/tmp/` filesystem | S3 (`s3://your-bucket/deequ-metrics/`) |
| Reporting | Console + matplotlib | CloudWatch + SNS alerts |
| Orchestration | Manual notebook run | AWS Glue Workflow / EventBridge scheduler |

---

## Production Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                             │
│   S3: s3://nyc-tlc/trip data/yellow_tripdata_*.parquet          │
│   (or any upstream Spark-readable source: Kafka, Redshift, etc) │
└───────────────────────┬─────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                    AWS Glue Job (PySpark)                        │
│                                                                 │
│   glueContext = GlueContext(SparkContext())                      │
│   spark       = glueContext.spark_session                       │
│   df          = spark.read.parquet("s3://nyc-tlc/...")          │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │               PyDeequ Validation Layer                  │   │
│   │                                                         │   │
│   │  ① VerificationSuite  → constraint checks               │   │
│   │  ② ColumnProfilerRunner → column stats                  │   │
│   │  ③ AnalysisRunner + FileSystemMetricsRepository         │   │
│   │     → metrics persisted to S3 per run                   │   │
│   └──────────────────┬──────────────────────────────────────┘   │
└─────────────────────┬┴─────────────────────────────────────────-┘
                      │
          ┌───────────┴────────────┐
          ▼                        ▼
┌─────────────────┐     ┌──────────────────────┐
│   S3 Results    │     │   CloudWatch Metrics  │
│                 │     │                       │
│ /metrics/*.json │     │  - Rows processed     │
│ /reports/*.csv  │     │  - Checks passed/fail │
└────────┬────────┘     └──────────┬───────────┘
         │                         │
         ▼                         ▼
┌─────────────────┐     ┌──────────────────────┐
│  AWS Athena     │     │   SNS Notification    │
│                 │     │                       │
│  Query metrics  │     │  Email/Slack alert    │
│  over time      │     │  on constraint fail   │
└────────┬────────┘     └──────────────────────┘
         │
         ▼
┌─────────────────┐
│  QuickSight     │
│  Dashboard      │
│                 │
│  Drift trends   │
│  Quality scores │
└─────────────────┘
```

---

## Code Mapping: Colab → AWS Glue

The only changes needed to run this pipeline on AWS Glue are the imports and I/O paths. The PyDeequ checks are untouched.

### Spark Session
```python
# ── Colab (this repo) ──────────────────────────────────────────
from pyspark.sql import SparkSession
spark = (SparkSession.builder
    .master("local[*]")
    .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.4-spark-3.5")
    .getOrCreate())

# ── AWS Glue (production) ──────────────────────────────────────
from awsglue.context import GlueContext
from pyspark.context import SparkContext
sc         = SparkContext()
glueContext = GlueContext(sc)
spark      = glueContext.spark_session
# No JAR config needed — Glue bundles Deequ natively
```

### Data Loading
```python
# ── Colab ──────────────────────────────────────────────────────
df = spark.read.parquet("sep_2025.parquet")

# ── AWS Glue ───────────────────────────────────────────────────
df = spark.read.parquet("s3://nyc-tlc/trip data/yellow_tripdata_*.parquet")
# This reads ALL years at once — billions of rows, same API
```

### Metrics Repository
```python
# ── Colab ──────────────────────────────────────────────────────
repository = FileSystemMetricsRepository(spark, "/tmp/deequ_metrics/metrics.json")

# ── AWS Glue ───────────────────────────────────────────────────
repository = FileSystemMetricsRepository(spark, "s3://your-bucket/deequ-metrics/metrics.json")
# Metrics now persist permanently across all runs
```

### Constraint Checks — no change needed
```python
# This block is identical in both environments
check = Check(spark, CheckLevel.Warning, "NYC Taxi Quality Checks")
checkResult = (VerificationSuite(spark)
    .onData(df)
    .addCheck(
        check
        .isComplete("fare_amount")
        .isNonNegative("fare_amount")
        .satisfies("passenger_count >= 1 AND passenger_count <= 6", "Valid passenger count")
        # ... all other checks unchanged
    ).run()
)
```

---

## Adding Alerting with SNS

In production you want automatic alerts when checks fail. Add this after your verification run:

```python
import boto3

def send_quality_alert(failed_checks, total_rows):
    sns = boto3.client("sns", region_name="us-east-1")
    
    message = f"""
    ⚠️ Data Quality Alert — NYC Taxi Pipeline
    
    Run date    : {datetime.now().strftime('%Y-%m-%d %H:%M')}
    Rows scanned: {total_rows:,}
    Failed checks: {len(failed_checks)}
    
    Failed constraints:
    {chr(10).join(f'  - {c}' for c in failed_checks)}
    
    See full report: s3://your-bucket/reports/latest.csv
    """
    
    sns.publish(
        TopicArn="arn:aws:sns:us-east-1:YOUR_ACCOUNT:data-quality-alerts",
        Subject="Data Quality Failure Detected",
        Message=message
    )

# Trigger after your check result
failed = pdf[pdf["constraint_status"] != "Success"]["constraint"].tolist()
if failed:
    send_quality_alert(failed, total_rows)
```

---

## Scheduling with EventBridge

To run this pipeline automatically every time new monthly data lands on S3:

```json
// EventBridge rule — triggers on new S3 object matching the pattern
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": { "name": ["nyc-tlc"] },
    "object": { "key": [{ "prefix": "trip data/yellow_tripdata_" }] }
  }
}
```

This triggers the Glue job automatically — no manual runs, no cron jobs.

---

## Cost Estimate (AWS Free Tier)

| Service | Free Tier | Estimated Usage |
|---|---|---|
| AWS Glue | 1M objects/month free | 1 job/month |
| S3 | 5GB free | ~500MB metrics + reports |
| SNS | 1M notifications free | ~10 alerts/month |
| Athena | 5GB queries free | Dashboard queries |

**For the portfolio:** Running this once manually on a small EC2 instance or a single Glue job costs under $1. You don't need to run it continuously to document the architecture credibly.

---

## Why This Matters at Scale

On a single Colab node, this pipeline processes 12.9M rows in a few minutes.  
On AWS Glue with a 10-node cluster, the **same code** processes the full NYC TLC dataset (~3 billion rows across all years) in roughly the same time — because Spark partitions the work across nodes automatically.

That's the core value of building on PySpark and PyDeequ: the code you write at 13M rows is production-ready at 3B rows without rewriting anything.
