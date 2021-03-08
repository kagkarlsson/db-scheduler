
resource "google_compute_instance" "default" {
  count = 4
  name         = "test${count.index}"
  machine_type = "n1-standard-4"
  zone         = local.compute_zone
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-9"
    }
  }

  // Local SSD disk
  scratch_disk {
    interface = "SCSI"
  }

  network_interface {
    network = "default"

    access_config {
      // Ephemeral IP
    }
  }

  metadata = {
    ssh-keys = "gustavkarlsson:${file("~/.ssh/id_rsa.pub")}"
  }

  metadata_startup_script = "apt-get install -yq openjdk-11-jdk"

  service_account {
    scopes = ["userinfo-email", "compute-ro", "storage-ro"]
  }
}
