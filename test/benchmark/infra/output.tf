locals {
  vm_ips = [for vm in google_compute_instance.default : vm.network_interface[0].access_config[0].nat_ip]
}

output "vm_public_ips" {
  value = local.vm_ips
}

output "postgres_public_ip" {
  value = google_sql_database_instance.master.public_ip_address
}

output "postgres_password" {
  value = random_password.user_password.result
}
