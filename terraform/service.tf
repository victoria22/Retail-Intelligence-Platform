# Enable the Service Usage API
resource "google_project_service" "service_usage_api" {
  project            = var.project_id
  service            = "serviceusage.googleapis.com"
  disable_on_destroy = false
}

# Wait for Service Usage API to be enabled
resource "null_resource" "wait_for_service_usage" {
  depends_on = [google_project_service.service_usage_api]

  provisioner "local-exec" {
    command = <<EOT
    echo "Waiting for Service Usage API to be enabled..."
    until gcloud services list --enabled --project=${var.project_id} | grep serviceusage.googleapis.com; do
      echo "Service Usage API not enabled yet. Retrying in 10 seconds..."
      sleep 10
    done
    echo "Service Usage API is now enabled."
    EOT
  }
}

# Enable all APIs (cloudresourcemanager is the priority API)
resource "google_project_service" "enabled_services" {
  for_each = toset([
    "cloudresourcemanager.googleapis.com",  # Make this the first one to ensure it's enabled first
    "compute.googleapis.com",
    "bigquery.googleapis.com",
    "composer.googleapis.com",
    "dataproc.googleapis.com",
    "iam.googleapis.com",
    "servicenetworking.googleapis.com",
    "sqladmin.googleapis.com"
  ])

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
  depends_on         = [null_resource.wait_for_service_usage]
}

# Ensure Cloud Resource Manager API is ready before proceeding
resource "null_resource" "wait_for_cloud_resourcemanager" {
  depends_on = [google_project_service.enabled_services]

  provisioner "local-exec" {
    command = <<EOT
    echo "Waiting for Cloud Resource Manager API to be enabled..."
    until gcloud services list --enabled --project=${var.project_id} | grep cloudresourcemanager.googleapis.com; do
      echo "Cloud Resource Manager API not enabled yet. Retrying in 10 seconds..."
      sleep 10
    done
    echo "Cloud Resource Manager API is now enabled."
    EOT
  }
}

# Cloud APIs enablement (for general use)
resource "google_project_service" "cloudapis" {
  project            = var.project_id
  service            = "cloudapis.googleapis.com"
  disable_on_destroy = false
}
