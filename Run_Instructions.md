# Full Setup & Run Instructions for Retail Intelligence Platform

This document provides **step-by-step run instructions** for deploying and executing the batch analytics pipeline for the Online Retail Data project on **Google Cloud Platform (GCP)** using **Terraform**, **Dataproc**, **Composer (Airflow)**, **BigQuery**, and **Looker Studio**.

---

## 1. Project Directory Structure

```
Retail-Intelligence-Platform/
│
├── dags/
│   └── Online_Retail_Data_etl.py
├── diagrams/
│   ├── Pipeline_Architecture.png
│   ├── Visualisation.png
├── scripts/
│   ├── Extract_Online_Retail_Data.py
│   ├── Transform_Online_Retail_Data.py
│   └── Environment_Test.py
├── terraform/
│   ├── /keys/service_keys.json             # Terraform service-account-keys
│   ├── main.tf                             # Provider and terraform block
│   ├── variables.tf                        # Variables declaration and values
│   ├── IAM_roles.ttxt                      # IAM role bindings for Terraform service account
│   ├── services.tf                         # API service enabling
│   ├── network.tf                          # VPC, subnets, firewall  
│   ├── dataproc.tf                         # Dataproc cluster for running Spark jobs
│   ├── bigquery.tf                         # BigQuery dataset to store processed data
│   ├── storage.tf                          # Storage buckets including gcs bucket to store raw data
│   ├── composer.tf                         # Composer environment(Airflow) for workflow orchestration
│   └── outputs.tf                          # Outputs
```

---

## 2. Infrastructure Provisioning (Terraform)

### Prerequisites
- Install [Terraform](https://developer.hashicorp.com/terraform/downloads)  
- Install [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)  
- Setup your project on Google cloud
- Create Terraform service account & authentication (https://cloud.google.com/docs/authentication/getting-started)
- Download terraform service-account-keys (.json) for authetication and store in a location on your machine
- Set environment variable to point to your downloaded GCP keys:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/terraform-service-account-authkeys>.json"
```
- Refresh token/session, and verify authentication:

```bash
gcloud auth application-default login
```

---

### Terraform Infrastructure Deployment

#### Assign Roles to Terraform service account

* Open your terminal
* Copy the Terraform_IAM_roles.txt into your terminal and update the values with your ProjectID and Terraform-service-account and run it

#### Navigate to Terraform Directory

Run the command to navigate to Terraform Directory:
```bash
cd Terraform
```

Before running terraform init and terraform apply, make sure you've updated the **variables**.tf file with correct values specific to your Google Cloud project by following the steps below:

* Steps
- Locate your variables.tf file in the Terraform project folder.
- Update the default values with your project-specific details
- Save the file, then run the following Terraform commands:

```bash
terraform init
terraform plan
terraform apply
```

Confirm with yes when prompted.

This provisions:
- Dataproc cluster
- GCS bucket
- Composer environment
- BigQuery dataset

---

## 3. Generate Kaggle API Credentials

- Go to Kaggle.
- Click on your profile picture (top-right corner) → Account.
- Scroll down to the API section.
- Click Create New API Token. This will download a file called kaggle.json.
- Save kaggle.json on secure path in local machine

---

## 4. Copy Scripts to Airflow GCS Bucket

Update the details of the DAG script in the **dags** folder

Update the Extract_Online_Retail_Data.py and Transform_Online_Retail_Data.py in the **scripts** folder with GCS bucket name and Bigquery details

Ensure the following scripts are copied into Composer’s bucket:

| Script | Purpose |
|--------|---------|
| `kaggle.json` | Kaggle Authentication key|
| `Extract_Online_Retail_Data.py` | Raw data extraction from Kaggle |
| `Transform_Online_Retail_Data.py` | Cleansing, transformations, and loading |
| `Environment_Test.py` | Verifies Python env on Dataproc |
| `Online_Retail_Data_etl.py` | Airflow DAG |

```bash
gsutil cp "/path/to/kaggle.json" gs://<COMPOSER_BUCKET>/scripts/kaggle.json
gsutil cp Extract_Online_Retail_Data.py gs://<COMPOSER_BUCKET>/Extract_Online_Retail_Data.py
gsutil cp Transform_Online_Retail_Data.py" gs://<COMPOSER_BUCKET>/Transform_Online_Retail_Data.py
gsutil cp Environment_Test.py gs://<COMPOSER_BUCKET>/Environment_Test.py
gsutil cp Online_Retail_Data_etl.py" gs://<COMPOSER_BUCKET>/dags/
```


---

## 5. Set Up Kaggle Credentials on Dataproc Nodes

### On local machine:
```bash
gcloud compute scp "/path/to/kaggle.json" <CLUSTER_NAME>-m --zone=<ZONE> ~/
```

Replace the following placeholders:
- `/path/to/kaggle.json`: The local path to the `kaggle.json` file you downloaded from Kaggle.
- `your-cluster-name-m`: The name of your Dataproc cluster master.
- `your-cluster-zone`: The zone in which your Dataproc cluster is running (e.g., `europe-west1-a`)

### SSH into master node and run:
```bash
gcloud compute ssh your-cluster-name-m --zone=your-cluster-zone
```

Create the `.kaggle` directory (if it doesn’t already exist) and move the `kaggle.json` file into it:
```bash
sudo mkdir -p /root/.kaggle
sudo cp ~/kaggle.json /root/.kaggle/kaggle.json
sudo chmod 600 /root/.kaggle/kaggle.json
```

Install `kaggle` if you haven't installed it yet:
```bash
pip install kaggle  
```

Verify the installation path by running:

```bash
export PATH=$HOME/.local/bin:$PATH
which kaggle
```

Make the change permanent:
```bash
echo 'export PATH=$HOME/.local/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

Test the setup on the master cluster node:
```bash
kaggle --version
kaggle datasets list
```

Repeat the whole process for each worker node by replacing `your-cluster-name-m` with the names of the worker cluster nodes, such as `your-cluster-name-w-0` and `your-cluster-name-w-1`.

---

## 6. Airflow Pipeline (Composer DAG)

### Steps:

1. Go to GCP Console → Composer → Select your environment
2. Click **"Airflow UI"** to open the DAG interface
3. Find `Online_Retail_Data_etl` DAG
4. Click **Trigger DAG** ▶️ to start the pipeline
5. Monitor task status in the Graph or Tree view

### DAG Workflow

```text
test_python_env
   ↓
extract_data
   ↓
load_online_retail_data
```

---

## 7. Dataproc Execution

Scripts are run as PySpark jobs via the DAG using DataprocSubmitJobOperator. No need for manual execution unless debugging.

Verify jobs from:
* GCP Console → Dataproc > Jobs
* Logs → Job stdout/stderr

## 8. BigQuery Table Validation

- Go to BigQuery console
- Locate dataset `online_retail`
- Query the `retail` table to verify transformed data

```
Dataset: online_retail  
Table: retail  
Partitioned by: InvoiceDate  
Clustered by: CustomerID
```

---

## 9. Looker Studio Setup

1. Go to Looker Studio → Create Report
2. Select **BigQuery** as the data source
3. Use the `online_retail.retail` table
4. Build visuals for:
   - High-performing products
   - Sales by country 
   - Profitability   
5. Add filters:
   - Year
   - Country
   - Product Category

---

## 10. Validation & Monitoring

* Use Airflow UI logs for DAG run validation
* Validate data with BigQuery preview
* Monitor dashboard and iterate based on feedback

## OPTIONAL - RUN NOTEBOOK

To run the files found in the Notebooks folder, you need to create a new authentication key on your composer service account which is in this format  PROJECT_ID-compute@developer.gserviceaccount.com. Copy and save the newly .json keys to your composer bucket.

Update the the files with your details before running the notebook.
