# 📚 Books Data Pipeline (End-to-End)
## 🚀 Overview
This project is an end-to-end data engineering pipeline built to process and analyze book ratings data.
It demonstrates a modern data stack using:
- Spark for data transformation
- Google Cloud Storage (GCS) for raw and intermediate storage
- BigQuery as a data warehouse
- dbt for analytics modeling
- Kestra for orchestration
- Docker & Terraform for infrastructure
---
## 🏗 Architecture
       +-------------------+
       |   Kaggle Dataset  |
       +---------+---------+
                 |
                 v
       +-------------------+
       |   GCS (Raw Zone)  |
       +---------+---------+
                 |
                 v
       +-------------------+
       |   Spark (ETL)     |
       | Data Cleaning     |
       +---------+---------+
                 |
                 v
       +---------------------------+
       | BigQuery (books_cleaned) |
       +-------------+-------------+
                     |
                     v
       +---------------------------+
       | dbt (Transformations)     |
       | - staging                 |
       | - marts (dim/fact)        |
       +-------------+-------------+
                     |
                     v
       +---------------------------+
       | BigQuery (books_analytics)|
       +-------------+-------------+
                     |
                     v
       +---------------------------+
       | Looker / BI Layer         |
       +---------------------------+
---
## 📂 Project
.
├── dbt_books/         # dbt project (models, tests, macros)
├── flows/             # Kestra flows (pipeline orchestration)
├── logs/              # Pipeline logs
├── spark_jobs/        # PySpark ETL jobs
├── terraform/         # Infrastructure as Code (GCP resources)
├── Users.csv          # Raw dataset example
├── docker-compose.yaml
└── README.md

---
## ⚙️ Tech Stack
- Python / PySpark
- Google Cloud Platform (GCS, BigQuery)
- dbt
- Kestra
- Docker
- Terraform
---
## 🔄 Pipeline Flow
### 1. Extract
- Data is downloaded from Kaggle
- Stored in GCS (raw layer)
### 2. Transform (Spark)
- Data cleaning:
 - Remove invalid ISBNs
 - Normalize authors (e.g., "J.K Rowling" → "J.K. Rowling")
 - Handle missing values
- Output written to:
 - books_cleaned dataset in BigQuery
### 3. Load & Model (dbt)
#### Staging Layer
- stg_books
- stg_users
- stg_ratings
#### Marts Layer
- dim_books
- dim_users
- fct_ratings
#### Example metrics:
- Top-rated books
- Ratings by country
- User activity
---
## 📊 Analytics Use Cases
- Top rated books overall
- Top books by country
- User rating behavior
- Distribution of ratings
---
## ▶️ How to Run
### 1. Start services