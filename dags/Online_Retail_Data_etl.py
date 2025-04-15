from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
import os
import shutil
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator


# Dataproc cluster details
PROJECT_ID = "retail-intelligence-platform"
REGION = "europe-west1"
CLUSTER_NAME = "online-retail-data-cluster"

# Google Cloud Storage paths
GCS_BUCKET = "gs://retail_intelligence_bucket"
KAGGLE_JSON_PATH = f"gs://europe-west1-composer-env-bc5c9a49-bucket/scripts/kaggle.json"

# Local paths
LOCAL_TMP_PATH = "/tmp/kaggle.json"
KAGGLE_AUTH_PATH = "/home/airflow/.kaggle/kaggle.json"

# PySpark scripts
EXTRACTION_PYSPARK_URI = "gs://europe-west1-composer-env-bc5c9a49-bucket/Extract_Online_Retail_Data.py" # Path to Extraction scripts in composer bucket
TRANSFORMATION_LOADING_PYSPARK_URI = "gs://europe-west1-composer-env-bc5c9a49-bucket/Transform_Online_Retail_Data.py" # Path to Transform and load scripts in composer bucket

# Define Dataproc job configurations
EXTRACTION_PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": EXTRACTION_PYSPARK_URI,
        "properties": {
            "spark.pyspark.python": "/opt/conda/default/bin/python"  # Force Anaconda's Python
        }
    }
}

TRANSFORMATION_LOADING_PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": TRANSFORMATION_LOADING_PYSPARK_URI,
        "args": [
            "--createDisposition", "CREATE_IF_NEEDED",  # This will create the table if it doesn't exist
            "--writeDisposition", "WRITE_TRUNCATE",  # This will overwrite the existing data (if any)
            "--dataset", "online_retail",  # BigQuery dataset
            "--table", "retail",  # BigQuery table
            "--output_data", "bigquery"
          
        ],
        "properties": {
            "spark.pyspark.python": "/opt/conda/default/bin/python"
        }
    }
}



default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'online_retail_data_etl',
    default_args=default_args,
    description='Extract, Transform, and Load Online Retail Data using Dataproc PySpark jobs',
    schedule_interval='@monthly',
    catchup=False,
) as dag:

   

    # Task 1: Test the Python environment
    test_python_env = DataprocSubmitJobOperator(
    task_id="test_python_env",
    job={
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": "gs://europe-west1-composer-env-bc5c9a49-bucket/Environment_Test.py",  # GCS path to the script
            "properties": {
                "spark.pyspark.python": "/opt/conda/default/bin/python"  # Force using Anaconda Python
            }
        }
    },
    region=REGION,
    project_id=PROJECT_ID
)


    # Task 2: Run Dataproc job to extract data
    extract_data = DataprocSubmitJobOperator(
        task_id='extract_data',
        job=EXTRACTION_PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    # Task 3: Run Dataproc job to transform and load data
    transform_and_load_data = DataprocSubmitJobOperator(
        task_id='load_online_retail_data',
        job=TRANSFORMATION_LOADING_PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    # DAG dependencies
    test_python_env >> extract_data >> transform_and_load_data
