#!/usr/bin/env bash
set -euo pipefail

CHART_DIR="${CHART_DIR:-helm/opennebula-csi}"
export STORAGECLASS_RECONCILE_CHART_DIR
STORAGECLASS_RECONCILE_CHART_DIR="$(cd "${CHART_DIR}" && pwd)"
go test ./pkg/csi/driver -run '^TestStorageClassReconcileRenderContract$' -count=1
