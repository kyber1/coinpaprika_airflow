# **Airflow Crypto Data Pipeline - Overview**

This repository contains a **Dockerized Apache Airflow setup** with **two DAGs** that automate fetching and processing **cryptocurrency OHLCV data** from the **CoinPaprika API**. The setup is designed to **run on a local server** and store data in **Google Cloud Storage (GCS)** for further analysis.

---

## **Repository Structure**

### **1️⃣ Airflow DAGs (Data Pipeline Workflows)**
These Python scripts define how data is fetched and processed:

- **`api_ids.txt`**
  - Contains a **list of cryptocurrency IDs** used for API calls.
  - Extracted manually from CoinPaprika (one-time task).

- **`dag_get_ohlcv_and_upload.py`**
  - Runs **twice daily in batches**, once at one hour and then again the next hour to **stay within CoinPaprika's API limits**.
  - Fetches OHLCV data for the top 100 cryptocurrencies.
  - Saves the raw data as **JSON** in **Google Cloud Storage (GCS)**.

- **`dag_transform_append.py`**
  - Runs **after data ingestion is complete**.
  - Uses **ExternalTaskSensor** to ensure the first DAG has completed before running.
  - Reads the raw JSON files, **cleans the data**, and **appends it to a Parquet file** in **GCS**.

---

### **2️⃣ Docker Setup (Inside `docker_files` Folder)**
This repository is **fully containerized** using **Docker and Docker Compose** to make the setup easy to deploy and manage:

- **`Dockerfile`**
  - Builds a custom **Airflow image** with:
    - Google Cloud SDK
    - Pandas (for data processing)
    - Requests (for API calls)

- **`docker-compose.yaml`**
  - Defines how **Airflow, PostgreSQL (metadata database), and Redis (message broker)** run inside containers.
  - Ensures **DAGs and credentials** are mounted correctly.

---

## **How the Pipeline Works**

1. **`dag_get_ohlcv_and_upload.py`** runs **twice a day in two batches**, fetching OHLCV data from CoinPaprika while respecting API limits.
2. Raw data is **uploaded to GCS** as JSON.
3. **`dag_transform_append.py`** waits for ingestion to finish using **ExternalTaskSensor**, then:
   - Reads the **JSON files**.
   - **Cleans and processes the data** into a Parquet format.
   - Saves the processed file in **GCS**.

The data is now **ready for analysis in BigQuery or Power BI**.

---

## **Future Improvements**

- Automate **updating the top 100 crypto list** instead of manually maintaining `api_ids.txt`.
- Improve **error handling and logging** for failed API calls.
- Optimize **Airflow performance** by experimenting with **KubernetesExecutor** or a managed service like **Cloud Composer**.
- Dive deeper into **Airflow capabilities**, exploring alternative ways to make `dag_transform_append.py` wait for the ingestion DAG, instead of relying on **ExternalTaskSensor**. Possible alternatives include:
  - **Trigger DAGs dynamically** using `TriggerDagRunOperator`.
  - **Use XComs** for cross-DAG communication.
  - **Implement a data-based trigger** (e.g., checking for new files in GCS before starting transformation).

This setup provides a **simple, automated, and scalable way** to collect and process crypto market data while leaving room for enhancements. 🚀
