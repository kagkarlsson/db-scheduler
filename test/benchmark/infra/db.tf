

resource "random_id" "db_name_suffix" {
  byte_length = 4
}

resource "random_password" "user_password" {
  length = 20
  special = false
}

data "http" "myip" {
  url = "http://ipv4.icanhazip.com"
}

resource "google_sql_database_instance" "master" {
  name = "db-scheduler-${random_id.db_name_suffix.hex}"
  region = local.region
  database_version = "POSTGRES_11"

  settings {
    tier = "db-f1-micro"

    ip_configuration {
      authorized_networks {
        name = "home"
        value = "${chomp(data.http.myip.body)}/32"
      }
      dynamic "authorized_networks" {
        for_each = toset(local.vm_ips)
        iterator = ip
        content {
          name = "vm${ip.key}"
          value = "${ip.key}/32"
        }
      }
    }
  }
}

resource "google_sql_user" "users" {
  name     = "gustavkarlsson"
  instance = google_sql_database_instance.master.name
  password = random_password.user_password.result
}

resource "google_sql_database" "database" {
  name     = "bench"
  instance = google_sql_database_instance.master.name
}
