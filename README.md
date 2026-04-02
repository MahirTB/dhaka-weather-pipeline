## 🏗️ Architecture

This pipeline follows a modular ETL (Extract, Transform, Load) pattern, orchestrated by Prefect Cloud for high reliability.

- **Extraction:**  
  An automated Python script fetches real-time weather data from the Open-Meteo API.

- **Transformation:**  
  Raw data is cleaned using Pandas, localizing timestamps to Asia/Dhaka and converting the dataset into Parquet format for optimized cloud storage and performance.

- **Loading:**  
  The processed, high-performance dataset is pushed to an AWS S3 bucket (Data Lake).

- **Visualization:**  
  A live Streamlit application fetches the Parquet files from S3 and renders interactive insights using Plotly.

---

## 🛠️ Tech Stack

- **Language:** Python (Pandas, Boto3, Requests)  
- **Orchestration:** Prefect Cloud (Hybrid Execution)  
- **Cloud Infrastructure:** AWS S3  
- **Visualization:** Streamlit & Plotly Express  