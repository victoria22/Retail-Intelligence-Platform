# Ensure the Dataproc service account has necessary permissions
resource "google_project_iam_member" "dataproc_service_account_dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

resource "google_project_iam_member" "dataproc_service_account_storage_object_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

resource "google_project_service" "dataproc_api" {
  project = var.project_id
  service = "dataproc.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_iam_member" "dataproc_service_account_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

resource "google_project_iam_member" "dataproc_service_account_editor" {
  project = var.project_id
  role    = "roles/editor"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# Assign Logging Role
resource "google_project_iam_member" "dataproc_service_account_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# Assign Monitoring Role
resource "google_project_iam_member" "dataproc_service_account_monitoring" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# Assign Storage Admin Role
resource "google_project_iam_member" "dataproc_service_account_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# Assign Storage Object Viewer Role
resource "google_project_iam_member" "dataproc_service_account_storage_object_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# Dataproc IAM for Compute Engine SA
resource "google_project_iam_member" "dataproc_sa_roles" {
  for_each = {
    worker        = "roles/dataproc.worker"
    storage_admin = "roles/storage.admin"
    editor        = "roles/editor"
    logging       = "roles/logging.logWriter"
    monitoring    = "roles/monitoring.metricWriter"
    composer      = "roles/composer.worker"
  }
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# Create a staging bucket (if not existing) - Ensure unique name and no duplicates
resource "google_storage_bucket" "dataproc_staging" {
  name     = "dataproc-staging-${var.project_id}-${var.region}"  # Dynamically using var.project_id and var.region
  location = var.region
}

# IAM binding for the staging bucket
resource "google_storage_bucket_iam_member" "dataproc_staging_bucket_role" {
  bucket = google_storage_bucket.dataproc_staging.name
  role   = "roles/storage.admin"
  member = "user:${var.user_email}"
}

# Dataproc Cluster Configuration (Existing or New)
resource "google_dataproc_cluster" "dataproc_cluster" {
  name   = var.dataproc_cluster_name
  region = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
      disk_config {
        boot_disk_type  = "pd-balanced"
        boot_disk_size_gb = 100
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-2"
      disk_config {
        boot_disk_type  = "pd-balanced"
        boot_disk_size_gb = 100
      }
    }

    software_config {
      image_version = "2.2-debian12"
    }

    gce_cluster_config {
      subnetwork       = google_compute_subnetwork.private_subnet.id
      internal_ip_only = false  # Allow external IPs
      tags             = ["dataproc"]
      service_account = "${var.project_number}-compute@developer.gserviceaccount.com"
    }
  }

  depends_on = [
    google_compute_network.vpc_network,
    google_compute_subnetwork.private_subnet,
    google_project_service.dataproc_api
  ]
}
