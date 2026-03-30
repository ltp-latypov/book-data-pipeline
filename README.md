# 📚 Book Data Pipeline (End-to-End)
## 🚀 Overview
This project is part of a data engineering course and focuses on processing and analyzing books, book ratings, book users data sourced from Kaggle. The dataset contains information about books, users, and their ratings, offering insights into reading preferences, author popularity, and global user behavior. By cleaning and modeling this data, the pipeline enables exploration of trends such as top-rated books, rating distributions across countries, and user engagement patterns
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
```
.
├── dbt_books/         # dbt project (models, tests, macros)
├── flows/             # Kestra flows (pipeline orchestration)     
├── spark_jobs/        # PySpark ETL jobs
├── terraform/         # Infrastructure as Code (GCP resources)
├── docker-compose.yaml
└── README.md
```

---
## ⚙️ Tech Stack
- Python / PySpark
- Google Cloud Platform (GCS, BigQuery)
- dbt
- Kestra
- Docker
- Terraform
---

![Books Data Pipeline Architecture](https://raw.githubusercontent.com/ltp-latypov/book-data-pipeline/refs/heads/main/img/architecture.png)
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
#### Intermediate Layer
- int_books
- int_users
- int_ratings
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

## 🔐 Authentication (IMPORTANT)
To run this project, you need a **Google Cloud service account JSON key and Kaggle credentials**.
### Steps:

1. **Clone Repository**

  ```bash
git clone https://github.com/ltp-latypov/book-data-pipeline.git
cd book-data-pipeline
   ```

2. **Create a service account in GCP with access to:**
  - GCS
  - BigQuery
3. **Download the JSON key and name it as service-account.json**
4. **Place it in the project root directory:**

5. **Download your Kaggle API credentials at https://www.kaggle.com/settings by clicking on the "Generate New Token" button under the "API" section. Update Project Configuration**:
   - Go to **terraform/** directory edit `variables.tf` and specify your GCP project ID in the `project` variable.
   - In **terraform/** directory create `terraform.tfvars` and specify your kaggle credentials .
   ```
    kaggle_username="example_username"
    kaggle_key="XXXXXXXXX"
   ```

6. **Deploy the infrastructure**:
    - Using **Makefile**, type type in terminal window:
   ```bash
   make all
   ```
   Or using **Docker** type in terminal window:
   ```bash
   echo SECRET_GCP_SERVICE_ACCOUNT=$(cat service-account.json | base64 -w 0) >> .env_encoded
   ```

   ``` bash
   docker-compose up --build
   ```

7. **Verify Services & Deployment**:

   ✅ What should be verified:
    - Terraform initialized and applied successfully 
    - Kestra CLI exited with code 0
    - Kestra is healthy and accessible at http://localhost:8080.
    - Spark Master is up (Web UI at http://localhost:38080).
    - Spark Worker is connected.
    - Postgres (Kestra's metadata database) is active.

8. **Initial Flow Execution**:
   - Access the Kestra UI at: [http://localhost:8080/ui/flows/edit/final_project/bigquery_extraction_flow](http://localhost:8080/ui/main/flows/edit/books_pipeline/books_pipeline_main/edit) or find **books_pipeline_main** flow
   - Click "Execute"

9. **Shutdown Procedure**:
- To stop the environment:
    - Using **Makefile**, just type
   ```bash
   make down
   ```
   Or using **Docker**
   ```bash
   docker-compose down -v
   ```

### Partitioning & Clustering
BigQuery tables are optimized using:
```sql
{{ config(
    materialized='table',
    partition_by={
      "field": "release_year",
      "data_type": "int64",
      "range": {
        "start": 1300,
        "end": 2030,
        "interval": 10
      }
    },
    cluster_by=['author', 'book_age_category']
) }}


{{ config(
    materialized='table',
    partition_by={
      "field": "age",
      "data_type": "int64",
      "range": {
        "start": 5,
        "end": 100,
        "interval": 1
      }
    },
    cluster_by=['country', 'age_group']
) }}
```




## DBT Lineage Graph

![Books Data Pipeline Architecture](https://raw.githubusercontent.com/ltp-latypov/book-data-pipeline/refs/heads/main/img/dbt_lineage_graph.png)


## Dashboard Visualization
The processed data is visualized in an interactive [Looker Studio report](https://lookerstudio.google.com/reporting/2f7acc39-9917-446b-a9d9-8b3b8d42287e), showcasing birth trends across demographics.

![Report page 1](https://raw.githubusercontent.com/ltp-latypov/book-data-pipeline/refs/heads/main/img/report_1.png)
![Report page 2](https://raw.githubusercontent.com/ltp-latypov/book-data-pipeline/refs/heads/main/img/report_2.png)


💡 Notes

	•	Ensure service-account.json .env_encoded and terraform.tears is never committed to Git
	•	Ensure it is added to .gitignore:

  ```bash
   service-account.json
   .env_encoded
   ```

