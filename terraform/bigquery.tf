# Grant the BigQuery Admin role to the service account to create datasets
resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"  # This grants permissions to manage BigQuery datasets
  member  = "serviceAccount:${var.service_account_email}"
}

# Create a BigQuery dataset
resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id = var.dataset_id
  project    = var.project_id
  location   = var.region

  depends_on = [google_project_iam_member.bigquery_admin]  # Ensure the IAM role is assigned first
}
