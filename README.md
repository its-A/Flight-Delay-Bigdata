# ✈️ Flight Delay & Root Cause Analytics Engine

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/PySpark-3.5%2B-orange.svg)](https://spark.apache.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-Fast%20SQL-yellow.svg)](https://duckdb.org/)
[![Storage](https://img.shields.io/badge/Format-Apache%20Parquet-green.svg)](https://parquet.apache.org/)
[![BI](https://img.shields.io/badge/Visualization-Tableau-lightblue.svg)](https://public.tableau.com/app/profile/eugenia5101/vizzes)

An end-to-end distributed data engineering and analytics pipeline built to process multi-year Bureau of Transportation Statistics (BTS) airline performance records. 

The system implements a columnar lakehouse architecture, performs multi-dimensional root cause delay attribution (decoupling controllable carrier operations from non-controllable weather/air traffic congestion), and deploys a PySpark machine learning pipeline to forecast high-risk route disruptions.

---

## 🏗️ Architecture & Data Flow

```text
[Raw BTS Airline & Airport Records]
                │
                ▼
  [01_ingest_partition.py (PySpark)]
  - Schema casting & sanitization
  - Out-of-core write to partitioned Parquet
                │
                ▼
   [Curated Lakehouse Storage]
   └── data/curated/flights/Year=YYYY/*.parquet
                │
                ├────────────────────────────────────────┐
                ▼                                        ▼
 [02_root_cause_analysis.py (DuckDB)]    [03_feature_pipeline.py (PySpark ML)]
 - Multi-dimensional aggregations        - Window lag operational baselines
 - Controllable vs. cascade attribution  - StringIndexer & OneHotEncoder
 - Analytical mart generation             - GBT Classifier (High-Risk Routing)
                │                                        │
                ▼                                        ▼
 [data/mart/mart_root_cause_summary]     [data/mart/delay_risk_predictions]
                │                                        │
                └───────────────────┬────────────────────┘
                                    ▼
                      [Executive Tableau Dashboard]
                      - Operational Scorecards
                      - Root Cause Attribution Matrix
                      - Predictive Risk Monitoring

  ## 📊 Executive Dashboard Preview
  **[View Interactive Tableau Dashboard on Tableau Public](https://public.tableau.com/app/profile/eugenia5101/vizzes)**
  ![Tableau Dashboard](docs/dashboard_preview.png)
