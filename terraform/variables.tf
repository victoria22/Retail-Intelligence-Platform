# The path to your service account key file (JSON)
variable "credentials" {
  description = "Path to your downloaded GCP service account key JSON file"
  default     = "./keys/my-creds.json" # 🔁 Replace with actual path on your machine
}

# Your actual GCP project ID (must match the one in GCP Console)
variable "project_id" {
  description = "The Google Cloud project ID"
  default     = "retail-intelligence-platform" # 🔁 Replace with your project ID, e.g., "genial-upgrade-455023-g5"
}

variable "bucket_name" {
  description = "The name of the storage bucket"
  type        = string
  default     = "retail_intelligence_bucket"  # Provide a default name or leave it empty to require input
}

# Define dataset_id variable
variable "dataset_id" {
  description = "The ID of the BigQuery dataset"
  type        = string
  default     = "online_retail"  # Default dataset ID (you can modify or leave it empty)
}

variable "dataproc_cluster_name" {
  description = "The name of the Dataproc cluster"
  type        = string
  default     = "online-retail-data-cluster"  # You can set a default value if you want
}

variable "user_email" {
  description = "Your goggle email"
  type        = string
  default     = "anaero235@gmail.com"  # Default email address, change it if necessary
}


# Friendly name for your GCP project (can be anything)
variable "project_name" {
  description = "The name of the Google Cloud project"
  default     = "Retail Intelligence Platform" # ✅ Can keep as is or customize
}

# Unique project number from your GCP project info page
variable "project_number" {
  description = "Project number used for Composer Service Agent Role"
  default     = "711112973024" # 🔁 Replace with actual numeric project number, e.g., "25290386376"
}

# Region where your GCP resources will be deployed
variable "region" {
  description = "The Google Cloud region"
  default     = "europe-west1" # ✅ Use your preferred region if different
}

# Email of the Terraform Service Account with required IAM roles
variable "service_account_email" {
  description = "The email of the Terraform service account"
  type        = string
  default     = "terraform@retail-intelligence-platform.iam.gserviceaccount.com" # 🔁 Replace with your service account email
}


