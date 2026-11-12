#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VALUES_FILE="${VALUES_FILE:-examples/helm-values-cephfs-dynamic.yaml}"
NAMESPACE="${NAMESPACE:-kube-system}"
RELEASE_NAME="${RELEASE_NAME:-opennebula-csi}"
CSI_IMAGE_REPOSITORY="${CSI_IMAGE_REPOSITORY:-}"
CSI_IMAGE_TAG="${CSI_IMAGE_TAG:-}"

require() {
  local var_name="$1"
  if [[ -z "${!var_name:-}" ]]; then
    echo "missing required environment variable: ${var_name}" >&2
    exit 1
  fi
}

[[ "${ISOLATED_DISPOSABLE_CLUSTER_APPROVAL:-}" == "run invasive isolated lab suite" ]] || { echo "This legacy installer is restricted to an approved isolated cluster; use the 7.4 qualification runbook on hpldb." >&2; exit 1; }
require LAB_CLUSTER_UID
require CSI_IMAGE_DIGEST
require KUBECONFIG
[[ "$(kubectl get ns kube-system -o jsonpath='{.metadata.uid}')" == "$LAB_CLUSTER_UID" ]] || { echo "wrong cluster UID" >&2; exit 1; }
require ONE_XMLRPC
require ONE_AUTH
require CEPHFS_ADMIN_ID
require CEPHFS_ADMIN_KEY
require CEPHFS_NODE_USER_ID
require CEPHFS_NODE_USER_KEY

cd "${ROOT_DIR}"

go test ./...
helm lint helm/opennebula-csi --values "${VALUES_FILE}"
helm template opennebula-csi helm/opennebula-csi --values "${VALUES_FILE}" >/tmp/opennebula-csi-staging-render.yaml

kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 || kubectl create namespace "${NAMESPACE}"

kubectl -n "${NAMESPACE}" create secret generic opennebula-csi-auth \
  --from-literal=credentials="${ONE_AUTH}" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "${NAMESPACE}" create secret generic cephfs-provisioner \
  --from-literal=adminID="${CEPHFS_ADMIN_ID}" \
  --from-literal=adminKey="${CEPHFS_ADMIN_KEY}" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "${NAMESPACE}" create secret generic cephfs-node-stage \
  --from-literal=userID="${CEPHFS_NODE_USER_ID}" \
  --from-literal=userKey="${CEPHFS_NODE_USER_KEY}" \
  --dry-run=client -o yaml | kubectl apply -f -

helm_args=(
  upgrade --install "${RELEASE_NAME}" helm/opennebula-csi
  --namespace "${NAMESPACE}"
  --create-namespace
  --wait
  --wait-for-jobs
  --timeout 150m
  --set "image.digest=${CSI_IMAGE_DIGEST}"
  --set "oneApiEndpoint=${ONE_XMLRPC}"
  --values "${VALUES_FILE}"
)

if [[ -n "${CSI_IMAGE_REPOSITORY}" ]]; then
  helm_args+=(--set "image.repository=${CSI_IMAGE_REPOSITORY}")
fi
if [[ -n "${CSI_IMAGE_TAG}" ]]; then
  helm_args+=(--set "image.tag=${CSI_IMAGE_TAG}")
fi

helm "${helm_args[@]}"

kubectl -n "${NAMESPACE}" rollout status statefulset/"${RELEASE_NAME}"-opennebula-csi-controller --timeout=5m
kubectl -n "${NAMESPACE}" rollout status daemonset/"${RELEASE_NAME}"-opennebula-csi-node --timeout=5m

kubectl apply -f examples/demo-busybox-cephfs-rwx.yaml
kubectl rollout status deployment/demo-cephfs-rwx --timeout=5m

echo "staging CephFS validation deployment is ready"
