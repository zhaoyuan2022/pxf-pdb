output "ssh_config" {
  value = templatefile("${path.module}/templates/ssh_config.tpl", {
    cluster_name = var.env_name
    ipa = google_compute_instance.ipa
    namenode = google_compute_instance.namenode
    datanode = google_compute_instance.datanode
  })

}
output "ansible_inventory" {
  value = templatefile("${path.module}/templates/inventory.ini.tpl", {
    ipa = google_compute_instance.ipa
    namenode = google_compute_instance.namenode
    datanode = google_compute_instance.datanode
  })
}

output "etc_hosts" {
  value = templatefile("${path.module}/templates/etc_hosts.tpl", {
    project_id = var.gcp_project
    ipa = google_compute_instance.ipa
    namenode = google_compute_instance.namenode
    datanode = google_compute_instance.datanode
  })
}

output "ansible_variables" {
  value = <<-DOC
  # Ansible vars_file containing variable values from Terraform.
  # Generated by Terraform mgmt configuration.
  cluster_name: ${var.env_name}
  gcp_storage_service_account_email: ${data.google_compute_default_service_account.default.email}
  DOC
}

output "private_key" {
  value = tls_private_key.keypair_gen.private_key_pem
  sensitive = true

}

output "namenode_tls_private_key" {
  value     = tls_private_key.namenode
  sensitive = true
}

output "namenode_tls_cert" {
  value     = tls_self_signed_cert.namenode
  sensitive = true
}

output "datanode_tls_private_key" {
  value = tls_private_key.datanode
  sensitive = true
}

output "datanode_tls_cert" {
  value = tls_self_signed_cert.datanode
  sensitive = true
}

output "project" {
  value = var.gcp_project
  description = "the terraform state must include the GCP project as an output to allow for deleting VMs from failed CI tasks."
}

output "zone" {
  value = var.gcp_zone
  description = "the terraform state must include the GCP zone as an output to allow for deleting VMs from failed CI tasks."
}