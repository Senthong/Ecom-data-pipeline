# 🛒 E-Commerce Data Pipeline — End-to-End Analytics Platform

> **Junior Data Engineer Portfolio Project**
> Stack: Python · dbt · Apache Airflow · Docker · AWS (S3, Redshift, Glue)

---

## 📌 Project Overview

A production-grade data pipeline that ingests raw e-commerce data from the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (100k+ real orders), transforms it through a medallion architecture (Bronze → Silver → Gold), and serves analytics-ready tables via a dbt data warehouse on AWS Redshift.

## 🏗️ Architecture

```
[Olist CSV / Kaggle API]
         │
         ▼
[Python Ingestion Scripts]
         │ upload raw CSV
         ▼
[AWS S3 — Raw Bucket]   Bronze Layer
         │
         ▼
[AWS Glue Crawler]  →  Glue Data Catalog
         │
         ▼
[AWS Redshift — Staging Schema]   Silver Layer
         │
         ▼
[dbt Transformations]
         │
         ▼
[AWS Redshift — Analytics Schema]   Gold Layer
         │
         ▼
[Apache Airflow — Orchestration]
```

## 📦 Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python 3.11, boto3, pandas |
| Storage | AWS S3 |
| Warehouse | AWS Redshift Serverless |
| Transformation | dbt-core, dbt-redshift |
| Orchestration | Apache Airflow 2.8 |
| Containerization | Docker, Docker Compose |
| IaC | Terraform |
| CI/CD | GitHub Actions |

## 📊 Dataset: Olist Brazilian E-Commerce

Real-world e-commerce dataset with 100k+ orders from 2016–2018:
- `olist_orders_dataset.csv` — order lifecycle
- `olist_order_items_dataset.csv` — item-level sales
- `olist_customers_dataset.csv` — customer data
- `olist_sellers_dataset.csv` — seller info
- `olist_products_dataset.csv` — product catalog
- `olist_order_payments_dataset.csv` — payment details
- `olist_order_reviews_dataset.csv` — customer reviews
- `olist_geolocation_dataset.csv` — Brazilian zip codes

## 🚀 Quick Start

```bash
git clone https://github.com/senthong/ecom-data-pipeline
cd ecom-data-pipeline
cp .env.example .env  # fill in your AWS credentials
docker compose up -d
```

Then open Airflow UI at http://localhost:8080 and trigger `ecom_full_pipeline_dag`.

## 📁 Project Structure

```
ecom-data-pipeline/
├ ingestion/           # Python scripts to ingest raw data → S3
├ dbt_project/         # dbt models (staging, intermediate, mart)
├ airflow/             # Airflow DAGs and plugins
├ infrastructure/      # Terraform for AWS + Docker Compose
├ scripts/             # Helper scripts
├ .github/workflows/   # CI/CD pipelines
└ README.md
```

## 📈 Key Business Metrics Produced

- **Daily Revenue Report** — GMV by day, week, month
- **Seller Performance** — top sellers by revenue & review score
- **Customer Cohort Analysis** — repeat purchase rate by signup month
- **Product Category Revenue** — top categories by revenue
- **Order Funnel** — conversion from placed → approved → delivered
- **Late Delivery Rate** — % orders delivered after estimated date

## 🎯 Skills Demonstrated

- Designing a multi-layer data warehouse (medallion architecture)
- Writing modular, tested dbt models with ref() and sources
- Orchestrating complex DAGs with Airflow (dependencies, retries, SLA)
- Containerizing the full stack with Docker & Docker Compose
- Managing cloud infrastructure with Terraform (S3, Redshift, IAM)
- Writing data quality tests (not_null, unique, accepted_values, custom)
- Implementing incremental dbt models for large tables
