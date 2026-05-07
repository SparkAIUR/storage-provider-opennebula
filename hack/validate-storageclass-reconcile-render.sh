#!/usr/bin/env bash
set -euo pipefail

CHART_DIR="${CHART_DIR:-helm/opennebula-csi}"

common_args=(
  --set credentials.existingSecret.name=dummy
  --set credentials.existingSecret.userKey=user
  --set credentials.existingSecret.passwordKey=pass
  --set 'storageClasses[0].name=opennebula-local'
  --set 'storageClasses[0].parameters.datastoreIDs=100'
)

default_render="$(mktemp)"
enabled_render="$(mktemp)"
trap 'rm -f "${default_render}" "${enabled_render}"' EXIT

helm template reconcile-default "${CHART_DIR}" "${common_args[@]}" >"${default_render}"
if grep -q 'app.kubernetes.io/component: storageclass-reconcile' "${default_render}"; then
	echo "storageClassReconcile hook rendered even though storageClassReconcile.enabled is unset" >&2
	exit 1
fi

helm template reconcile-enabled "${CHART_DIR}" "${common_args[@]}" \
  --set storageClassReconcile.enabled=true >"${enabled_render}"

if ! grep -q 'app.kubernetes.io/component: storageclass-reconcile' "${enabled_render}"; then
  echo "storageClassReconcile hook labels were not rendered when explicitly enabled" >&2
  exit 1
fi
if ! grep -q '^kind: Job$' "${enabled_render}"; then
  echo "storageClassReconcile Job was not rendered when explicitly enabled" >&2
  exit 1
fi
if ! grep -q '^kind: ConfigMap$' "${enabled_render}"; then
  echo "storageClassReconcile ConfigMap was not rendered when explicitly enabled" >&2
  exit 1
fi

echo "storageClassReconcile render validation OK"
