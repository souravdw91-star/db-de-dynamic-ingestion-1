# Databricks Dynamic Ingestion & Medallion Architecture

A metadata-driven, schemaless dynamic data ingestion pipeline built for **Databricks** (compatible with **Databricks Community Edition**).

## Overview
This repository implements a production-grade data engineering pipeline following the **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold $\rightarrow$ RPT)**:
- **Bronze Layer**: Raw CSV files downloaded from GitHub, `ingestion_config` metadata driver table, `ingestion_audit` execution log table, and `quarantine_<dataset>` failure isolation tables.
- **Silver Layer**: Clean, Pydantic-validated, type-casted Delta format tables in `silver_db`.
- **Gold Layer**: Star-Schema dimensional data models (`dim_customers`, `dim_products`, `dim_stores`, `dim_supplies`, `fact_orders`, `fact_order_items`) in `gold_db`.
- **RPT Layer**: Aggregated business analytics reporting tables (`rpt_monthly_store_performance`, `rpt_customer_lifetime_value`, `rpt_product_sales_summary`) in `rpt_db`.

---

## Repository Structure

```
├── README.md
├── docs/
│   ├── Databricks_Dynamic_Ingestion_Documentaion.md   # Complete Architecture Documentation
│   └── project_requirement.txt                       # Project specifications
├── ingestion/
│   ├── 00_setup_database_and_config.ipynb            # Database & Metadata initialization
│   ├── ingestion.ipynb                               # Generic Pydantic dynamic ingestion pipeline
│   └── ingestion_orchestration.ipynb                 # Master ingestion orchestrator notebook
└── aggregation/
    ├── 01_gold_dimensions_and_facts.sql              # Gold layer Star Schema DDL & DML
    ├── 02_rpt_reporting_tables.sql                   # RPT layer business analytics DDL & DML
    └── gold_and_rpt_orchestration.ipynb              # Databricks wrapper notebook for Gold & RPT
```

---

## Quickstart Guide for Databricks Community Edition

1. **Import Workspace Repos**:
   - Clone or import this repository into your Databricks Workspace.
2. **Execute Ingestion Setup**:
   - Run `ingestion/00_setup_database_and_config.ipynb` to create databases and initialize the metadata configuration table.
3. **Execute Ingestion Orchestration**:
   - Run `ingestion/ingestion_orchestration.ipynb` to download raw files, validate data with Pydantic, populate `silver_db` Delta tables, and record audit stats.
4. **Execute Gold & RPT Transformations**:
   - Run `aggregation/gold_and_rpt_orchestration.ipynb` (or execute `01_gold_dimensions_and_facts.sql` and `02_rpt_reporting_tables.sql` in Databricks SQL) to create Gold dimensions, facts, and business reporting tables.

For full architecture details, metadata schemas, and SQL validation queries, see [Databricks_Dynamic_Ingestion_Documentaion.md](file:///c:/Sourav/Study/FDE/Assignments/db-de-dynamic-ingestion-1/docs/Databricks_Dynamic_Ingestion_Documentaion.md).
