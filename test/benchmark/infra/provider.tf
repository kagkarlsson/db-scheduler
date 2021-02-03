locals {
  region = "europe-west1"
  compute_zone = "europe-west1-b"
}
provider "google" {
  credentials = file("~/db-scheduler-benchmark-account.json")
  project     = "db-scheduler-benchmarking"
  region      = local.region
}
