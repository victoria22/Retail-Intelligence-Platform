# Assign Composer Worker Role
resource "google_project_iam_member" "composer_service_account_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# Enable the Composer API
resource "google_project_service" "composer_api" {
  service = "composer.googleapis.com"
  project = var.project_id
  depends_on = [google_project_service.service_usage_api]
}

# Cloud Composer Environment
resource "google_composer_environment" "composer_env" {
  name   = "composer-env"
  region = var.region

  config {
    software_config {
      image_version = "composer-3-airflow-2.10.2-build.9"
    }

    node_config {
      # Specify the service account explicitly for the nodes
      service_account = "${var.project_number}-compute@developer.gserviceaccount.com"
    }
  }

  depends_on = [google_project_service.composer_api]
}
