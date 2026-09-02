# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

variable "kubeconfig" {
  type        = string
  description = "KUBECONFIG used for Terraform execution"
}

variable "project" {
  type        = string
  description = "GDCH project name"
}

variable "service-account-name" {
  type        = string
  description = "Project service account name"
}

variable "gdch-domain-surfix" {
  type        = string
  description = "GDCH domain suffix"
}

variable "ca-cert-path" {
  type        = string
  description = "Local CA cert data file path"
}

variable "key-json-file" {
  type        = string
  description = "Generated private key file name for the service account"
}

variable "output-kubeconfig-file" {
  type        = string
  description = "Generated KUBECONFIG file name for the service account"
}

variable "cluster-name" {
  type        = string
  description = "GDCH cluster name the service account will interact with"
}

provider "kubernetes" {
  config_path = var.kubeconfig
}

resource "kubernetes_manifest" "project" {
  manifest = {
    "apiVersion" = "resourcemanager.gdc.goog/v1"
    "kind"       = "Project"
    "metadata" = {
      "name"      = "${var.project}"
      "namespace" = "platform"

      "labels" = {
        "atat.config.google.com/task-order-number" = "TO5678"
        "atat.config.google.com/clin-number"       = "5678"
      }
    }
  }

  wait {
    fields = {
      "status.propagatedName" = "${var.project}"
    }
  }
}

resource "tls_private_key" "ecdsa" {
  algorithm   = "ECDSA"
  ecdsa_curve = "P256"
}

resource "random_uuid" "uuid" {}

resource "kubernetes_manifest" "project_service_account" {
  depends_on = [
    tls_private_key.ecdsa,
  ]

  manifest = {
    "apiVersion" = "resourcemanager.gdc.goog/v1"
    "kind"       = "ProjectServiceAccount"
    "metadata" = {
      "name"      = "${var.service-account-name}"
      "namespace" = "${var.project}"
    }
    "spec" = {
      "keys" = [
        {
          "algorithm"   = "ES256"
          "id"          = random_uuid.uuid.result
          "key"         = base64encode(tls_private_key.ecdsa.public_key_pem)
          "validAfter"  = timestamp()
          "validBefore" = timeadd(timestamp(), "240h")
        }
      ]
    }
  }

  wait {
    fields = {
      "status.propagatedName" = "${var.service-account-name}"
    }
  }
}

resource "local_file" "key_json" {
  depends_on = [kubernetes_manifest.project_service_account]
  content = jsonencode({
    format_version = "1"
    ca_cert_path   = var.ca-cert-path
    name           = var.service-account-name
    private_key    = replace(tls_private_key.ecdsa.private_key_pem, "\n", "\n")
    private_key_id = random_uuid.uuid.result
    project        = var.project
    token_uri      = format("https://service-accounts.%s/authenticate", var.gdch-domain-surfix)
    type           = "gdch_service_account"
  })
  filename        = var.key-json-file
  file_permission = 644
}

resource "local_file" "kubeconfig" {
  depends_on      = [local_file.key_json]
  filename        = var.output-kubeconfig-file
  file_permission = 644
  content         = <<-EOF
apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority-data: ${filebase64(var.ca-cert-path)}
    server: https://${var.cluster-name}-kube.${var.gdch-domain-surfix}
  name: ${var.cluster-name}
contexts:
- context:
    cluster: ${var.cluster-name}
    user: user-gdch-sa
  name: context-${var.cluster-name}
current-context: context-${var.cluster-name}
users:
- name: user-gdch-sa
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      command: gdch-sa-auth-plugin
      args:
      - --audience=${var.cluster-name}
      - --key-file=${local_file.key_json.filename}
EOF
}
