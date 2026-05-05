package driver

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	inventoryv1alpha1 "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/apis/storageprovider/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	placementSourcePVRequiredNode          = "pv_required_node"
	placementSourcePVCRequiredNode         = "pvc_required_node"
	placementSourcePVPreferredNode         = "pv_preferred_node"
	placementSourcePVCPreferredNode        = "pvc_preferred_node"
	placementSourceLegacyPreferredLastNode = "legacy_preferred_last_node"
	placementSourceLastAttachedNode        = "last_attached_node"

	placementDecisionNone        = "none"
	placementDecisionRequired    = "required"
	placementDecisionPreferred   = "preferred"
	placementDecisionIgnored     = "ignored"
	placementDecisionInvalid     = "invalid"
	placementDecisionConflicting = "conflicting"
)

type explicitNodePlacement struct {
	RequiredNode           string
	RequiredNodeSource     string
	RequiredNodeUntil      *time.Time
	RequiredNodeExpired    bool
	PreferredNode          string
	PreferredNodeSource    string
	PlacementReason        string
	LegacyPreferredNode    string
	Warnings               []string
	RequiredNodeParseError error
	ExplicitPreferredNode  string
}

func hasManualNodeTargetingAnnotations(runtimeCtx *VolumeRuntimeContext) bool {
	if runtimeCtx == nil {
		return false
	}
	for _, annotations := range []map[string]string{runtimeCtx.PVAnnotations, runtimeCtx.PVCAnnotations} {
		if strings.TrimSpace(annotations[annotationRequiredNode]) != "" {
			return true
		}
		if strings.TrimSpace(annotations[annotationPreferredNode]) != "" {
			return true
		}
		if strings.TrimSpace(annotations[annotationRequiredNodeUntil]) != "" {
			return true
		}
		if strings.TrimSpace(annotations[annotationPlacementReason]) != "" {
			return true
		}
	}
	return false
}

func explicitNodePlacementForRuntimeContext(runtimeCtx *VolumeRuntimeContext) explicitNodePlacement {
	placement := explicitNodePlacement{
		PlacementReason: strings.TrimSpace(firstNonEmpty(
			annotationValue(runtimeCtx, annotationPlacementReason, "pv"),
			annotationValue(runtimeCtx, annotationPlacementReason, "pvc"),
		)),
	}
	if runtimeCtx == nil {
		return placement
	}

	pvRequired := annotationValue(runtimeCtx, annotationRequiredNode, "pv")
	pvcRequired := annotationValue(runtimeCtx, annotationRequiredNode, "pvc")
	switch {
	case pvRequired != "":
		placement.RequiredNode = pvRequired
		placement.RequiredNodeSource = placementSourcePVRequiredNode
		if pvcRequired != "" && pvcRequired != pvRequired {
			placement.Warnings = append(placement.Warnings, fmt.Sprintf("PVC required-node %q is ignored because PV required-node %q takes precedence", pvcRequired, pvRequired))
		}
	case pvcRequired != "":
		placement.RequiredNode = pvcRequired
		placement.RequiredNodeSource = placementSourcePVCRequiredNode
	}

	if placement.RequiredNode != "" {
		rawUntil := strings.TrimSpace(firstNonEmpty(
			requiredNodeUntilForScope(runtimeCtx, placement.RequiredNodeSource),
			annotationValue(runtimeCtx, annotationRequiredNodeUntil, "pv"),
			annotationValue(runtimeCtx, annotationRequiredNodeUntil, "pvc"),
		))
		if rawUntil != "" {
			until, err := time.Parse(time.RFC3339, rawUntil)
			if err != nil {
				placement.RequiredNodeParseError = fmt.Errorf("invalid %s annotation %q: %w", annotationRequiredNodeUntil, rawUntil, err)
				return placement
			}
			expiry := until.UTC()
			placement.RequiredNodeUntil = &expiry
			if expiry.Before(time.Now().UTC()) {
				placement.RequiredNodeExpired = true
			}
		}
	}

	pvPreferred := annotationValue(runtimeCtx, annotationPreferredNode, "pv")
	pvcPreferred := annotationValue(runtimeCtx, annotationPreferredNode, "pvc")
	switch {
	case pvPreferred != "":
		placement.PreferredNode = pvPreferred
		placement.ExplicitPreferredNode = pvPreferred
		placement.PreferredNodeSource = placementSourcePVPreferredNode
		if pvcPreferred != "" && pvcPreferred != pvPreferred {
			placement.Warnings = append(placement.Warnings, fmt.Sprintf("PVC preferred-node %q is ignored because PV preferred-node %q takes precedence", pvcPreferred, pvPreferred))
		}
	case pvcPreferred != "":
		placement.PreferredNode = pvcPreferred
		placement.ExplicitPreferredNode = pvcPreferred
		placement.PreferredNodeSource = placementSourcePVCPreferredNode
	}

	if placement.PreferredNode == "" {
		legacyPreferred := annotationValue(runtimeCtx, annotationPreferredLastNode, "pv")
		if legacyPreferred != "" {
			placement.LegacyPreferredNode = legacyPreferred
			placement.PreferredNode = legacyPreferred
			placement.PreferredNodeSource = placementSourceLegacyPreferredLastNode
			placement.Warnings = append(placement.Warnings, fmt.Sprintf("PV annotation %s is deprecated and will be ignored in v0.6.0; use %s instead", annotationPreferredLastNode, annotationPreferredNode))
		}
	}

	return placement
}

func requiredNodeUntilForScope(runtimeCtx *VolumeRuntimeContext, source string) string {
	switch source {
	case placementSourcePVRequiredNode:
		return annotationValue(runtimeCtx, annotationRequiredNodeUntil, "pv")
	case placementSourcePVCRequiredNode:
		return annotationValue(runtimeCtx, annotationRequiredNodeUntil, "pvc")
	default:
		return ""
	}
}

func annotationValue(runtimeCtx *VolumeRuntimeContext, key, scope string) string {
	if runtimeCtx == nil {
		return ""
	}
	switch scope {
	case "pv":
		return strings.TrimSpace(runtimeCtx.PVAnnotations[key])
	case "pvc":
		return strings.TrimSpace(runtimeCtx.PVCAnnotations[key])
	default:
		return ""
	}
}

func volumeRuntimeContextFromPVAndPVC(pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim) *VolumeRuntimeContext {
	if pv == nil {
		return nil
	}
	runtimeCtx := &VolumeRuntimeContext{
		PVName:        pv.Name,
		PVAnnotations: cloneStringMap(pv.Annotations),
		AccessModes:   append([]corev1.PersistentVolumeAccessMode(nil), pv.Spec.AccessModes...),
	}
	runtimeCtx.Backend = strings.TrimSpace(runtimeCtx.PVAnnotations[annotationBackend])
	if rawDatastoreID := strings.TrimSpace(runtimeCtx.PVAnnotations[annotationDatastoreID]); rawDatastoreID != "" {
		if parsed, err := strconv.Atoi(rawDatastoreID); err == nil {
			runtimeCtx.DatastoreID = parsed
		}
	}
	runtimeCtx.RestartMode = strings.TrimSpace(runtimeCtx.PVAnnotations[annotationRestartOpt])
	if rawGrace := strings.TrimSpace(runtimeCtx.PVAnnotations[annotationDetachGrace]); rawGrace != "" {
		if parsed, err := strconv.Atoi(rawGrace); err == nil {
			runtimeCtx.DetachGraceHint = parsed
		}
	}
	if pvc != nil {
		runtimeCtx.PVCNamespace = pvc.Namespace
		runtimeCtx.PVCName = pvc.Name
		runtimeCtx.PVCAnnotations = cloneStringMap(pvc.Annotations)
		if runtimeCtx.RestartMode == "" {
			runtimeCtx.RestartMode = strings.TrimSpace(runtimeCtx.PVCAnnotations[annotationRestartOpt])
		}
		if runtimeCtx.DetachGraceHint == 0 {
			if rawGrace := strings.TrimSpace(runtimeCtx.PVCAnnotations[annotationDetachGrace]); rawGrace != "" {
				if parsed, err := strconv.Atoi(rawGrace); err == nil {
					runtimeCtx.DetachGraceHint = parsed
				}
			}
		}
	} else if pv.Spec.ClaimRef != nil {
		runtimeCtx.PVCNamespace = pv.Spec.ClaimRef.Namespace
		runtimeCtx.PVCName = pv.Spec.ClaimRef.Name
	}
	return runtimeCtx
}

func (r *KubeRuntime) GetNode(ctx context.Context, nodeName string) (*corev1.Node, error) {
	if r == nil || !r.enabled {
		return nil, fmt.Errorf("kubernetes runtime is not enabled")
	}
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return nil, fmt.Errorf("node name is required")
	}
	return r.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
}

func (r *KubeRuntime) CompatibleSystemDatastoresForRuntimeContext(ctx context.Context, runtimeCtx *VolumeRuntimeContext) ([]string, string, error) {
	if runtimeCtx == nil {
		return nil, "", nil
	}
	if !strings.EqualFold(strings.TrimSpace(runtimeCtx.Backend), "local") {
		return nil, "non-local backend is not topology restricted", nil
	}
	if runtimeCtx.DatastoreID <= 0 {
		return nil, "", fmt.Errorf("missing datastore-id annotation")
	}
	return r.compatibleSystemDatastoresForDatastore(ctx, runtimeCtx.DatastoreID)
}

func (r *KubeRuntime) compatibleSystemDatastoresForDatastore(ctx context.Context, datastoreID int) ([]string, string, error) {
	if r == nil || !r.enabled {
		return nil, "", fmt.Errorf("kubernetes runtime is not enabled")
	}
	if r.inventoryClient == nil {
		return nil, "", fmt.Errorf("inventory runtime is not enabled")
	}
	if datastoreID <= 0 {
		return nil, "", fmt.Errorf("datastore id is required")
	}
	datastore, err := r.OpenNebulaDatastoreByID(ctx, datastoreID)
	if err != nil {
		return nil, "", err
	}
	if strings.EqualFold(strings.TrimSpace(datastore.Status.Backend), "cephfs") {
		return nil, "shared filesystem datastores are node-agnostic", nil
	}
	if len(datastore.Status.OpenNebula.CompatibleSystemDatastores) == 0 {
		return nil, fmt.Sprintf("datastore %d does not report compatible system datastores", datastoreID), nil
	}
	values := make([]string, 0, len(datastore.Status.OpenNebula.CompatibleSystemDatastores))
	for _, id := range datastore.Status.OpenNebula.CompatibleSystemDatastores {
		if id <= 0 {
			continue
		}
		values = append(values, strconv.Itoa(id))
	}
	sort.Strings(values)
	return values, "", nil
}

func (r *KubeRuntime) NodeSystemDatastore(ctx context.Context, nodeName string) (string, error) {
	if r == nil || !r.enabled {
		return "", fmt.Errorf("kubernetes runtime is not enabled")
	}
	node, err := r.GetNode(ctx, nodeName)
	if err != nil {
		return "", err
	}
	if value := strings.TrimSpace(node.Labels[topologySystemDSLabel]); value != "" {
		return value, nil
	}
	if r.inventoryClient == nil {
		return "", nil
	}
	openNebulaNode, err := r.OpenNebulaNode(ctx, nodeName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	if value := strings.TrimSpace(openNebulaNode.Status.Storage.TopologySystemDatastore); value != "" {
		return value, nil
	}
	if openNebulaNode.Status.OpenNebula.SystemDatastoreID > 0 {
		return strconv.Itoa(openNebulaNode.Status.OpenNebula.SystemDatastoreID), nil
	}
	return "", nil
}

func (r *KubeRuntime) ValidateManualNodeTarget(ctx context.Context, runtimeCtx *VolumeRuntimeContext, nodeName, targetKind string) error {
	if r == nil || !r.enabled {
		return fmt.Errorf("kubernetes runtime is not enabled")
	}
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return fmt.Errorf("%s is empty", targetKind)
	}
	if _, err := r.GetNode(ctx, nodeName); err != nil {
		return fmt.Errorf("%s %s does not exist: %w", targetKind, nodeName, err)
	}
	if r.inventoryClient != nil {
		openNebulaNode, err := r.OpenNebulaNode(ctx, nodeName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("%s %s is missing from OpenNebula inventory", targetKind, nodeName)
			}
			return fmt.Errorf("failed to look up %s %s in OpenNebula inventory: %w", targetKind, nodeName, err)
		}
		if strings.EqualFold(strings.TrimSpace(openNebulaNode.Status.Phase), inventoryv1alpha1.NodePhaseNotFound) {
			return fmt.Errorf("%s %s is tombstoned in OpenNebula inventory", targetKind, nodeName)
		}
	}
	compatibleSystemDatastores, reason, err := r.CompatibleSystemDatastoresForRuntimeContext(ctx, runtimeCtx)
	if err != nil {
		return fmt.Errorf("failed to resolve compatible system datastores for %s %s: %w", targetKind, nodeName, err)
	}
	if len(compatibleSystemDatastores) == 0 {
		return nil
	}
	nodeSystemDatastore, err := r.NodeSystemDatastore(ctx, nodeName)
	if err != nil {
		return fmt.Errorf("failed to resolve system datastore for %s %s: %w", targetKind, nodeName, err)
	}
	if strings.TrimSpace(nodeSystemDatastore) == "" {
		return fmt.Errorf("%s %s does not report topology system datastore compatibility (%s)", targetKind, nodeName, reason)
	}
	if !containsString(compatibleSystemDatastores, nodeSystemDatastore) {
		return fmt.Errorf("%s %s is incompatible with compatible system datastores %v (node system datastore=%s)", targetKind, nodeName, compatibleSystemDatastores, nodeSystemDatastore)
	}
	return nil
}

func containsString(values []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, value := range values {
		if strings.TrimSpace(value) == target {
			return true
		}
	}
	return false
}
