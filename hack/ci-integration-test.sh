#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Google LLC
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

COMMIT_HASH="${COMMIT_HASH:-$(git rev-parse --short HEAD)}"
REGION="${GDC_REGION:-us-west6}"
ZONE="${GDC_ZONE:-us-west6-a}"
AVAILABLE_ZONES="${GDC_AVAILABLE_ZONES:-us-west6-a,us-west6-b,us-west6-c}"
GDCH_PROJECT="${GDC_PROJECT:-sapbtp}"
VUC="${GDC_VUC:-gardener-github-ci}"
ORG="${GDC_ORG:-gdc1}"
LAB_URL="${GDC_LAB_URL:-staging.gpcdemolabs.com}"
MANAGED_DNS_ZONE="${GDC_MANAGED_DNS_ZONE:-sap.gardener.gpcdemolabs.com}"
GDCLOUD_VERSION="${GDCLOUD_VERSION:-1.16.2}"
VCLUSTER_VERSION="${VCLUSTER_VERSION:-0.32.1}"
VCLUSTER_K8S_TAG="${VCLUSTER_K8S_TAG:-v1.35.0}"
IMAGE_REPOSITORY_PROVIDER="${IMAGE_REPOSITORY_PROVIDER:-ghcr.io/gardener/gardener-extension-provider-gdc/gardener-extension-provider-gdch}"
IMAGE_TAG="${IMAGE_TAG:-pr-${COMMIT_HASH}}"
IMAGE_WITHTAG="${IMAGE_REPOSITORY_PROVIDER}:${IMAGE_TAG}"

WORK_DIR="$(mktemp -d)"
CA_FILE="${WORK_DIR}/cafile"
SA_FILE="${GDC_SERVICE_ACCOUNT_FILE:-${WORK_DIR}/gdc_service_account.json}"
TOKEN_HELPER_DIR="${WORK_DIR}/token-helper"
MGMT_URL="https://management-kube.apiserver.${ORG}.${ZONE}.${LAB_URL}"

cleanup() {
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT INT TERM

if [[ -n "${GDC_SERVICE_ACCOUNT_KEY:-}" ]]; then
  umask 077
  printf '%s' "${GDC_SERVICE_ACCOUNT_KEY}" > "${SA_FILE}"
fi

if [[ ! -s "${SA_FILE}" ]]; then
  echo "Error: GDC Service Account key is required (set GDC_SERVICE_ACCOUNT_KEY or GDC_SERVICE_ACCOUNT_FILE)." >&2
  exit 1
fi

determine_controllers() {
  if [[ -n "${CONTROLLERS:-}" ]]; then
    echo "${CONTROLLERS}"
    return 0
  fi

  if [[ -z "${BASE_REF:-}" ]] || ! git rev-parse --verify "${BASE_REF}" >/dev/null 2>&1; then
    echo ""
    return 0
  fi

  local changed_files
  changed_files=$(git diff --name-only "${BASE_REF}...HEAD" || true)
  if [[ -z "${changed_files}" ]]; then
    echo ""
    return 0
  fi

  local run_all=false
  local -A affected=()

  while IFS= read -r file; do
    [[ -z "${file}" ]] && continue
    case "${file}" in
      pkg/controller/worker/*|integration/presubmit/extension-provider/worker_test.go)
        affected["WorkerController"]=1
        ;;
      pkg/controller/bastion/*|integration/presubmit/extension-provider/bastion_test.go)
        affected["BastionController"]=1
        ;;
      pkg/controller/infrastructure/*|integration/presubmit/extension-provider/infra_test.go)
        affected["InfraController"]=1
        ;;
      pkg/controller/backupbucket/*|pkg/controller/backupentry/*|integration/presubmit/extension-provider/backup_test.go)
        affected["BackupController"]=1
        ;;
      pkg/controller/controlplane/*|integration/presubmit/extension-provider/controlplane_test.go)
        affected["ControlplaneController"]=1
        ;;
      pkg/controller/dnsrecord/*|integration/presubmit/extension-provider/dnsrecord_test.go)
        affected["DNSRecordController"]=1
        ;;
      pkg/webhook/*|integration/presubmit/extension-provider/provider_webhook_test.go)
        affected["ExtensionProviderWebhook"]=1
        ;;
      *.md)
        ;;
      *)
        run_all=true
        break
        ;;
    esac
  done <<< "${changed_files}"

  if [[ "${run_all}" == "true" ]]; then
    echo ""
    return 0
  fi

  if [[ "${#affected[@]}" -eq 0 ]]; then
    echo "NONE"
    return 0
  fi

  local IFS=","
  echo "${!affected[*]}"
}

CONTROLLERS_TO_RUN="$(determine_controllers)"
if [[ "${CONTROLLERS_TO_RUN}" == "NONE" ]]; then
  echo "Only documentation files changed. Skipping integration test execution."
  exit 0
fi

if [[ -n "${CONTROLLERS_TO_RUN}" ]]; then
  echo "Selected controllers to run: ${CONTROLLERS_TO_RUN}"
else
  echo "Running all extension-provider controllers and webhooks."
fi

echo "Fetching GDC Root CA from console.${ORG}.${ZONE}.${LAB_URL}..."
wget "https://console.${ORG}.${ZONE}.${LAB_URL}/.well-known/certificate-authority" \
  --no-check-certificate -q -O "${CA_FILE}"

install_gdcloud_cli() {
  local gdcloud_root="/usr/local/bin/google-distributed-cloud-hosted-cli"
  if [[ -x "${gdcloud_root}/bin/gdcloud" && -x "${gdcloud_root}/bin/gdcloud-k8s-auth-plugin" ]]; then
    echo "gdcloud CLI already installed at ${gdcloud_root}/bin/gdcloud"
    export PATH="${gdcloud_root}/bin:${PATH}"
    export GDCLOUD_PATH="${gdcloud_root}/bin/gdcloud"
    return 0
  fi

  echo "Minting STS token using GDC ServiceAccount to query CLIBundleMetadata..."
  mkdir -p "${TOKEN_HELPER_DIR}"
  cat << 'EOF' > "${TOKEN_HELPER_DIR}/main.go"
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/gardener/gardener-extension-provider-gdc/gdc/pkg/auth"
)

func main() {
	saBytes, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var sa auth.ServiceAccount
	if err := json.Unmarshal(saBytes, &sa); err != nil {
		panic(err)
	}
	caBytes, err := os.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}
	ts := auth.NewSTSTokenSource(os.Args[3], &sa, auth.WithCACert(caBytes))
	tok, err := ts.Token()
	if err != nil {
		panic(err)
	}
	fmt.Print(tok.AccessToken)
}
EOF

  local sts_token
  sts_token=$(go run "${TOKEN_HELPER_DIR}/main.go" "${SA_FILE}" "${CA_FILE}" "${MGMT_URL}")

  echo "Resolving gdcloud CLI bundle URL (version: ${GDCLOUD_VERSION}) from ${MGMT_URL}..."
  local serving_url
  serving_url=$(curl -ksSL -H "Authorization: Bearer ${sts_token}" \
    "${MGMT_URL}/apis/artifactview.private.gdc.goog/v1alpha1/namespaces/ui-system/clibundlemetadata" | \
    jq -r --arg ver "${GDCLOUD_VERSION}" '
      [.items[]
       | select(.platform.os == "linux" and .platform.architecture == "amd64")
       | select($ver == "" or (.commonMetadata.artifactVersion | contains($ver)))
      ]
      | sort_by(.metadata.creationTimestamp)
      | last
      | .commonMetadata.servingURL // empty
    ')

  if [[ -z "${serving_url}" ]]; then
    echo "Error: Failed to resolve gdcloud CLI servingURL for version '${GDCLOUD_VERSION}'." >&2
    exit 1
  fi

  local tarball="/tmp/gdcloud_cli_linux.tar.gz"
  echo "Downloading gdcloud CLI from ${serving_url}..."
  curl -kL --fail --retry 3 "${serving_url}?uncompressed=false" -o "${tarball}"

  local sudo_cmd=""
  if [[ "${EUID:-$(id -u)}" -ne 0 ]] && command -v sudo >/dev/null 2>&1; then
    sudo_cmd="sudo"
  fi

  echo "Extracting gdcloud CLI to /usr/local/bin and installing gdcloud-k8s-auth-plugin..."
  ${sudo_cmd} tar -xzf "${tarball}" -C /usr/local/bin/
  ${sudo_cmd} "${gdcloud_root}/bin/gdcloud" components install gdcloud-k8s-auth-plugin
  rm -f "${tarball}"

  export PATH="${gdcloud_root}/bin:${PATH}"
  export GDCLOUD_PATH="${gdcloud_root}/bin/gdcloud"
  "${GDCLOUD_PATH}" version
}

install_gdcloud_cli

echo "Packaging extension-provider Helm chart..."
CHART_VERSION=$(yq '.version' charts/extension-provider/Chart.yaml)
helm package charts/extension-provider --destination "${WORK_DIR}"
CHART_PACKAGE="${WORK_DIR}/gardener-extension-provider-gdch-${CHART_VERSION}.tgz"

echo "Pulling vcluster Helm chart (version: ${VCLUSTER_VERSION})..."
helm pull vcluster --repo https://charts.loft.sh --version "${VCLUSTER_VERSION}" --destination "${WORK_DIR}"
VCLUSTER_CHART_PACKAGE="${WORK_DIR}/vcluster-${VCLUSTER_VERSION}.tgz"

echo "Building and pushing provider image ${IMAGE_WITHTAG}..."
make docker-image-provider IMAGE_REPOSITORY_PROVIDER="${IMAGE_REPOSITORY_PROVIDER}" IMAGE_TAG="${IMAGE_TAG}"
docker push "${IMAGE_WITHTAG}"

echo "Running extension-provider presubmit integration test..."
CGO_ENABLED=0 go test -v -timeout=45m ./integration/presubmit/extension-provider \
  -args \
  --commit_hash="${COMMIT_HASH}" \
  --zone="${ZONE}" \
  --region="${REGION}" \
  --available_zones="${AVAILABLE_ZONES}" \
  --project="${GDCH_PROJECT}" \
  --vuc="${VUC}" \
  --org="${ORG}" \
  --lab_url="${LAB_URL}" \
  --cafile="${CA_FILE}" \
  --service_account="${SA_FILE}" \
  --image_tag="${IMAGE_WITHTAG}" \
  --chart_package="${CHART_PACKAGE}" \
  --vcluster_chart="${VCLUSTER_CHART_PACKAGE}" \
  --vcluster_k8s_tag="${VCLUSTER_K8S_TAG}" \
  --managed_dns_zone="${MANAGED_DNS_ZONE}" \
  --controllers="${CONTROLLERS_TO_RUN}"
