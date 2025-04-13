# Create storage bucket for online retail data
resource "google_storage_bucket" "storage_bucket" {
  name          = var.bucket_name
  location      = var.region
  storage_class = "STANDARD"
}

# IAM role assignment for Dataproc/Composer's default Compute Engine service account
resource "google_storage_bucket_iam_member" "dataproc_bucket_role" {
  bucket = google_storage_bucket.storage_bucket.name
  role   = "roles/storage.objectAdmin"

  # Assign access to the Compute Engine default SA (used by Dataproc/Composer)
  member = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# Assign objectAdmin role to Compute Engine default SA
resource "google_storage_bucket_iam_member" "object_admin_binding" {
  bucket = google_storage_bucket.storage_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}
