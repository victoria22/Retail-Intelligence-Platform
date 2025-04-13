resource "google_compute_network" "vpc_network" {
  name = "private-network"
}

resource "google_compute_subnetwork" "private_subnet" {
  name                     = "private-subnet"
  region                   = var.region
  network                  = google_compute_network.vpc_network.id
  private_ip_google_access = true
  ip_cidr_range            = "10.0.0.0/24"
}

# Allow internal communication between Dataproc nodes
resource "google_compute_firewall" "dataproc_internal" {
  name    = "dataproc-internal-communication"
  network = google_compute_network.vpc_network.name

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "icmp"
  }

  source_ranges = ["10.0.0.0/24"]
  target_tags   = ["dataproc"]
}

# Allow SSH, RDP, and ICMP from the internet 
resource "google_compute_firewall" "dataproc_external" {
  name    = "dataproc-external-access"
  network = google_compute_network.vpc_network.name

  allow {
    protocol = "tcp"
    ports    = ["22"]  # Only allow SSH
  }
  allow {
    protocol = "icmp"
  }

  source_ranges = ["51.191.3.75/32"]  # a single IP
  target_tags   = ["dataproc"]
}
