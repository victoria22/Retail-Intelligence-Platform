import os
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage
from google.api_core.exceptions import Forbidden, NotFound

# Initialize the Kaggle API
api = KaggleApi()
api.authenticate()

# Define dataset and download path
dataset = 'thedevastator/online-retail-transaction-data'
download_path = './raw_uk_online_retail_data/'

# Create the directory if it doesn't exist
os.makedirs(download_path, exist_ok=True)

# Download the dataset
api.dataset_download_files(dataset, path=download_path, unzip=True)

# Initialize Google Cloud Storage client
client = storage.Client()

# Define the GCS bucket
bucket_name = 'YOUR GCS BUCKET NAME'

try:
    bucket = client.get_bucket(bucket_name)
except Forbidden:
    print(f"ERROR: Access denied to bucket '{bucket_name}'.")
    print("Please ensure the Dataproc cluster's service account has at least the 'Storage Object Admin' role.")
    raise
except NotFound:
    print(f"ERROR: Bucket '{bucket_name}' not found.")
    print("Please verify the bucket name and that it exists in your project.")
    raise

# Path to the downloaded files
local_directory = './raw_uk_online_retail_data/'

# Upload files to GCS
for root, dirs, files in os.walk(local_directory):
    for file in files:
        if file.endswith('.csv'):  # upload only CSV files
            local_file_path = os.path.join(root, file)
            gcs_blob_path = f"uk_online_retail_data/{file}"

            # Check if the file already exists in GCS
            blob = bucket.blob(gcs_blob_path)
            if not blob.exists():  # If the file does not exist in GCS
                # Create a blob and upload the file
                blob.upload_from_filename(local_file_path)
                print(f"Uploaded {file} to gs://{bucket_name}/{gcs_blob_path}")
            else:
                print(f"File {file} already exists in GCS, skipping upload.")
