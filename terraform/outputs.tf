# Output the URLs and information for the created resources
output "composer_url" {
  value = google_composer_environment.composer_env.config[0].airflow_uri
}

output "bq_dataset_id" {
  value = google_bigquery_dataset.bq_dataset.dataset_id
}

output "dataproc_cluster_name" {
  value = google_dataproc_cluster.dataproc_cluster.name
}

output "storage_bucket_name" {
  value = google_storage_bucket.storage_bucket.name
}

