package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const lastNodePreferenceWebhookPath = "/mutate-v1-pod-last-node-preference"

type jsonPatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value,omitempty"`
}

type LastNodePreferenceWebhook struct {
	driver *Driver
}

type podNodePlacementDecision struct {
	TargetNode        string
	Required          bool
	Reason            string
	Source            string
	PlacementDecision string
}

func NewLastNodePreferenceWebhook(driver *Driver) *LastNodePreferenceWebhook {
	return &LastNodePreferenceWebhook{driver: driver}
}

func (w *LastNodePreferenceWebhook) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		rw.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var review admissionv1.AdmissionReview
	if err := json.NewDecoder(req.Body).Decode(&review); err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(rw).Encode(admissionv1.AdmissionReview{
			Response: &admissionv1.AdmissionResponse{
				UID:     review.Request.UID,
				Allowed: false,
				Result:  &metav1.Status{Message: err.Error()},
			},
		})
		return
	}

	response := &admissionv1.AdmissionResponse{
		UID:     review.Request.UID,
		Allowed: true,
	}
	if review.Request == nil || review.Request.Kind.Kind != "Pod" {
		review.Response = response
		_ = json.NewEncoder(rw).Encode(review)
		return
	}

	var pod corev1.Pod
	if err := json.Unmarshal(review.Request.Object.Raw, &pod); err != nil {
		response.Allowed = false
		response.Result = &metav1.Status{Message: err.Error()}
		review.Response = response
		_ = json.NewEncoder(rw).Encode(review)
		return
	}

	patch, warnings, err := w.buildPatch(req.Context(), &pod)
	if err != nil {
		response.Allowed = false
		response.Result = &metav1.Status{Message: err.Error()}
		review.Response = response
		_ = json.NewEncoder(rw).Encode(review)
		return
	}
	if len(warnings) > 0 {
		response.Warnings = warnings
	}
	if len(patch) > 0 {
		rawPatch, err := json.Marshal(patch)
		if err != nil {
			response.Allowed = false
			response.Result = &metav1.Status{Message: err.Error()}
			review.Response = response
			_ = json.NewEncoder(rw).Encode(review)
			return
		}
		patchType := admissionv1.PatchTypeJSONPatch
		response.PatchType = &patchType
		response.Patch = rawPatch
	}
	review.Response = response
	_ = json.NewEncoder(rw).Encode(review)
}

func (w *LastNodePreferenceWebhook) buildPatch(ctx context.Context, pod *corev1.Pod) ([]jsonPatchOperation, []string, error) {
	if w == nil || w.driver == nil || w.driver.kubeRuntime == nil || !w.driver.kubeRuntime.enabled {
		return nil, nil, nil
	}
	if pod == nil || strings.TrimSpace(pod.Spec.NodeName) != "" {
		return nil, nil, nil
	}

	annotations := map[string]string{}
	var affinity corev1.Affinity
	affinityInitialized := false
	affinityChanged := false
	ensureAffinity := func() *corev1.Affinity {
		if !affinityInitialized {
			if pod.Spec.Affinity != nil {
				affinity = *pod.Spec.Affinity.DeepCopy()
			}
			affinityInitialized = true
		}
		return &affinity
	}

	systemDatastores, systemWarnings, err := w.resolveCompatibleSystemDatastores(ctx, pod)
	warnings := append([]string{}, systemWarnings...)
	if err != nil {
		w.driver.metrics.RecordLastNodePreference("error", "system_ds_conflict")
		return nil, warnings, err
	}
	if len(systemDatastores) > 0 {
		applyRequiredNodeSelector(ensureAffinity(), corev1.NodeSelectorRequirement{
			Key:      topologySystemDSLabel,
			Operator: corev1.NodeSelectorOpIn,
			Values:   systemDatastores,
		})
		annotations[annotationSystemDSAffinity] = strings.Join(systemDatastores, ",")
		annotations[annotationSystemDSInjected] = "true"
		affinityChanged = true
	}

	placement, placementWarnings, err := w.resolvePlacementNode(ctx, pod)
	warnings = append(warnings, placementWarnings...)
	if err != nil {
		w.driver.metrics.RecordLastNodePreference("error", "resolution_failed")
		return nil, warnings, err
	}
	if strings.TrimSpace(placement.TargetNode) == "" {
		w.driver.metrics.RecordLastNodePreference("skipped", placement.Reason)
		return buildPodMutationPatch(pod, annotations, affinityChanged, ensureAffinity), warnings, nil
	}
	if placement.Required {
		node, err := w.driver.kubeRuntime.GetNode(ctx, placement.TargetNode)
		if err != nil {
			w.driver.metrics.RecordLastNodePreference("error", "node_lookup_failed")
			return nil, warnings, err
		}
		hostname := strings.TrimSpace(node.Labels[corev1.LabelHostname])
		if hostname == "" {
			hostname = placement.TargetNode
		}
		if allowed, reason := podAllowsNodeTarget(pod, node); !allowed {
			w.driver.metrics.RecordLastNodePreference("error", "pod_constraint_conflict")
			return nil, warnings, fmt.Errorf("pod %s/%s already has scheduling constraints that exclude required node %s: %s", pod.Namespace, pod.Name, placement.TargetNode, reason)
		}
		applyRequiredNodeSelector(ensureAffinity(), corev1.NodeSelectorRequirement{
			Key:      corev1.LabelHostname,
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{hostname},
		})
		annotations[annotationRequiredNodeInjected] = placement.TargetNode
		annotations[annotationPlacementSource] = firstNonEmpty(placement.Source, protectionSourceExplicitRequiredNode)
		annotations[annotationPlacementDecision] = placementDecisionRequired
		affinityChanged = true
		w.driver.metrics.RecordLastNodePreference("injected", firstNonEmpty(placement.Source, "protected_required_hostname"))
		return buildPodMutationPatch(pod, annotations, affinityChanged, ensureAffinity), warnings, nil
	}
	if softPlacementRequiresFeatureGate(placement.Source) && !w.lastNodePreferenceEnabled() {
		w.driver.metrics.RecordLastNodePreference("skipped", "feature_disabled")
		return buildPodMutationPatch(pod, annotations, affinityChanged, ensureAffinity), warnings, nil
	}
	if softPlacementRequiresFeatureGate(placement.Source) && strings.EqualFold(strings.TrimSpace(pod.Annotations[annotationLastNodePref]), lastNodePreferenceDisabledValue) {
		w.driver.metrics.RecordLastNodePreference("skipped", "pod_opt_out")
		return buildPodMutationPatch(pod, annotations, affinityChanged, ensureAffinity), warnings, nil
	}
	node, err := w.driver.kubeRuntime.GetNode(ctx, placement.TargetNode)
	if err != nil {
		validation := nodeCandidateValidation{
			Node:   strings.TrimSpace(placement.TargetNode),
			Status: nodeCandidateValidationKubernetesLookupFailed,
			Message: fmt.Sprintf(
				"failed to look up %s %s in Kubernetes: %v",
				placementSourceTargetKind(placement.Source),
				strings.TrimSpace(placement.TargetNode),
				err,
			),
		}
		if apierrors.IsNotFound(err) {
			validation.Status = nodeCandidateValidationMissingKubernetesNode
			validation.Message = fmt.Sprintf("%s %s does not exist in Kubernetes", placementSourceTargetKind(placement.Source), strings.TrimSpace(placement.TargetNode))
		}
		warnings = append(warnings, softPlacementWarning(placement.Source, validation))
		w.driver.metrics.RecordLastNodePreference("skipped", softPlacementMetricReason(validation.Status))
		return buildPodMutationPatch(pod, annotations, affinityChanged, ensureAffinity), warnings, nil
	}
	hostname := strings.TrimSpace(node.Labels[corev1.LabelHostname])
	if hostname == "" {
		hostname = strings.TrimSpace(placement.TargetNode)
	}
	if hasPreferredHostnameAffinity(pod, hostname) {
		w.driver.metrics.RecordLastNodePreference("skipped", "already_present")
		return buildPodMutationPatch(pod, annotations, affinityChanged, ensureAffinity), warnings, nil
	}
	appendPreferredHostname(ensureAffinity(), hostname)
	annotations[annotationPreferredNodeInjected] = placement.TargetNode
	annotations[annotationPlacementSource] = firstNonEmpty(placement.Source, placementSourceLastAttachedNode)
	annotations[annotationPlacementDecision] = placementDecisionPreferred
	affinityChanged = true
	w.driver.metrics.RecordLastNodePreference("injected", firstNonEmpty(placement.Source, "preferred_hostname"))
	return buildPodMutationPatch(pod, annotations, affinityChanged, ensureAffinity), warnings, nil
}

func (w *LastNodePreferenceWebhook) resolvePlacementNode(ctx context.Context, pod *corev1.Pod) (podNodePlacementDecision, []string, error) {
	decision := podNodePlacementDecision{Reason: "no_eligible_volume", PlacementDecision: placementDecisionNone}
	requiredDecisions := map[string]podNodePlacementDecision{}
	preferredDecisions := map[string]podNodePlacementDecision{}
	warnings := []string{}
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		pvc, err := w.driver.kubeRuntime.client.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, volume.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("skipping PVC %s: %v", volume.PersistentVolumeClaim.ClaimName, err))
			continue
		}
		if strings.TrimSpace(pvc.Spec.VolumeName) == "" {
			continue
		}
		pv, err := w.driver.kubeRuntime.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("skipping PV %s: %v", pvc.Spec.VolumeName, err))
			continue
		}
		if pv.Spec.CSI == nil || strings.TrimSpace(pv.Spec.CSI.Driver) != DefaultDriverName {
			continue
		}
		protection, err := localRWOProtectionDecisionForDriver(ctx, w.driver, pv.Spec.CSI.VolumeHandle, "")
		if err != nil {
			return decision, warnings, err
		}
		warnings = append(warnings, protection.Warnings...)
		if protection.ExplicitRequiredNodeExpired && protection.ExplicitRequiredNodeUntil != nil {
			warnings = append(warnings, fmt.Sprintf("ignoring expired required-node %s on PV %s because it expired at %s", protection.ExplicitRequiredNode, pv.Name, protection.ExplicitRequiredNodeUntil.Format(time.RFC3339)))
		}
		if protection.Invalid {
			if strings.TrimSpace(protection.Message) != "" {
				warnings = append(warnings, protection.Message)
			}
			return decision, warnings, fmt.Errorf("%s", protection.Message)
		}
		if protection.PlacementDecision == placementDecisionIgnored && strings.TrimSpace(protection.Message) != "" {
			warnings = append(warnings, protection.Message)
		}
		if !eligibleForLastNodePreference(pv) {
			continue
		}
		if protection.Protected && strings.TrimSpace(protection.RequiredNode) != "" {
			requiredDecisions[strings.TrimSpace(protection.RequiredNode)] = podNodePlacementDecision{
				TargetNode:        strings.TrimSpace(protection.RequiredNode),
				Required:          true,
				Reason:            firstNonEmpty(protection.Reason, "protected_required_node"),
				Source:            firstNonEmpty(protection.PlacementSource, protection.Source),
				PlacementDecision: placementDecisionRequired,
			}
			continue
		}
		preferredNode := strings.TrimSpace(protection.PreferredNode)
		if preferredNode == "" {
			continue
		}
		preferredDecisions[preferredNode] = podNodePlacementDecision{
			TargetNode:        preferredNode,
			Reason:            firstNonEmpty(protection.Reason, "eligible"),
			Source:            firstNonEmpty(protection.PlacementSource, protection.Source),
			PlacementDecision: placementDecisionPreferred,
		}
	}
	switch len(requiredDecisions) {
	case 0:
	case 1:
		for _, resolved := range requiredDecisions {
			decision = resolved
			return decision, warnings, nil
		}
	default:
		warnings = append(warnings, "conflicting local RWO volumes require different manual or protected nodes")
		return decision, warnings, fmt.Errorf("pod references local RWO volumes that require different nodes")
	}
	switch len(preferredDecisions) {
	case 0:
		return decision, warnings, nil
	case 1:
		for _, resolved := range preferredDecisions {
			decision = resolved
			return decision, warnings, nil
		}
	default:
		warnings = append(warnings, "skipping soft last-node preference because referenced PVCs disagree on preferred node")
		decision.Reason = "conflicting_nodes"
		return decision, warnings, nil
	}
	return decision, warnings, nil
}

func (w *LastNodePreferenceWebhook) resolveCompatibleSystemDatastores(ctx context.Context, pod *corev1.Pod) ([]string, []string, error) {
	if w == nil || w.driver == nil || w.driver.kubeRuntime == nil || pod == nil {
		return nil, nil, nil
	}
	if w.driver.kubeRuntime.inventoryClient == nil {
		return nil, nil, nil
	}
	warnings := []string{}
	var intersection map[string]struct{}
	seenVolume := false

	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		pvcName := strings.TrimSpace(volume.PersistentVolumeClaim.ClaimName)
		if pvcName == "" {
			continue
		}
		pvc, err := w.driver.kubeRuntime.client.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("skipping PVC %s for system datastore affinity: %v", pvcName, err))
			continue
		}
		if strings.TrimSpace(pvc.Spec.VolumeName) == "" {
			continue
		}
		pv, err := w.driver.kubeRuntime.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("skipping PV %s for system datastore affinity: %v", pvc.Spec.VolumeName, err))
			continue
		}
		values, reason, err := w.compatibleSystemDatastoresForPV(ctx, pv)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("skipping PV %s for system datastore affinity: %v", pv.Name, err))
			continue
		}
		if len(values) == 0 {
			if reason != "" {
				warnings = append(warnings, fmt.Sprintf("skipping PV %s for system datastore affinity: %s", pv.Name, reason))
			}
			continue
		}
		current := stringSet(values)
		if !seenVolume {
			intersection = current
			seenVolume = true
			continue
		}
		intersection = intersectStringSets(intersection, current)
		if len(intersection) == 0 {
			return nil, warnings, fmt.Errorf("OpenNebula CSI PVCs on pod %s/%s require incompatible system datastore sets", pod.Namespace, pod.Name)
		}
	}

	if len(intersection) == 0 {
		return nil, warnings, nil
	}
	values := sortedSetValues(intersection)
	return values, warnings, nil
}

func (w *LastNodePreferenceWebhook) compatibleSystemDatastoresForPV(ctx context.Context, pv *corev1.PersistentVolume) ([]string, string, error) {
	if pv == nil || pv.Spec.CSI == nil {
		return nil, "", nil
	}
	if strings.TrimSpace(pv.Spec.CSI.Driver) != DefaultDriverName {
		return nil, "", nil
	}
	if opennebula.IsSharedFilesystemVolumeID(pv.Spec.CSI.VolumeHandle) || strings.EqualFold(strings.TrimSpace(pv.Annotations[annotationBackend]), "cephfs") {
		return nil, "shared filesystem volumes are node-agnostic", nil
	}
	rawDatastoreID := strings.TrimSpace(pv.Annotations[annotationDatastoreID])
	if rawDatastoreID == "" {
		return nil, "missing datastore-id annotation", nil
	}
	datastoreID, err := strconv.Atoi(rawDatastoreID)
	if err != nil || datastoreID <= 0 {
		return nil, "", fmt.Errorf("invalid datastore-id annotation %q", rawDatastoreID)
	}
	datastore, err := w.driver.kubeRuntime.OpenNebulaDatastoreByID(ctx, datastoreID)
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

func (w *LastNodePreferenceWebhook) lastNodePreferenceEnabled() bool {
	if w == nil || w.driver == nil {
		return false
	}
	enabled, ok := w.driver.PluginConfig.GetBool(config.LastNodePreferenceEnabledVar)
	if !ok {
		return true
	}
	return enabled
}

func eligibleForLastNodePreference(pv *corev1.PersistentVolume) bool {
	if pv == nil || pv.Spec.CSI == nil {
		return false
	}
	if strings.TrimSpace(pv.Spec.CSI.Driver) != DefaultDriverName {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(pv.Annotations[annotationBackend]), "local") {
		return false
	}
	if !hasSingleWriterAccessMode(pv.Spec.AccessModes) {
		return false
	}
	if opennebula.IsSharedFilesystemVolumeID(pv.Spec.CSI.VolumeHandle) {
		return false
	}
	return true
}

func hasPreferredHostnameAffinity(pod *corev1.Pod, hostname string) bool {
	if pod == nil || pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return false
	}
	for _, term := range pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
		for _, expr := range term.Preference.MatchExpressions {
			if expr.Key != corev1.LabelHostname || expr.Operator != corev1.NodeSelectorOpIn {
				continue
			}
			for _, value := range expr.Values {
				if value == hostname {
					return true
				}
			}
		}
	}
	return false
}

func softPlacementRequiresFeatureGate(source string) bool {
	switch strings.TrimSpace(source) {
	case placementSourcePVPreferredNode, placementSourcePVCPreferredNode, placementSourceLegacyPreferredLastNode:
		return false
	default:
		return true
	}
}

func podAllowsNodeTarget(pod *corev1.Pod, node *corev1.Node) (bool, string) {
	if pod == nil || node == nil {
		return false, "pod or node is missing"
	}
	for key, value := range pod.Spec.NodeSelector {
		if strings.TrimSpace(node.Labels[key]) != value {
			return false, fmt.Sprintf("nodeSelector %s=%s excludes node %s", key, value, node.Name)
		}
	}
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil || pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return true, ""
	}
	required := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(required.NodeSelectorTerms) == 0 {
		return true, ""
	}
	for _, term := range required.NodeSelectorTerms {
		if nodeSelectorTermMatchesNode(term, node) {
			return true, ""
		}
	}
	return false, "existing required node affinity excludes the target node"
}

func nodeSelectorTermMatchesNode(term corev1.NodeSelectorTerm, node *corev1.Node) bool {
	if node == nil {
		return false
	}
	for _, expr := range term.MatchExpressions {
		if !nodeSelectorRequirementMatchesNode(expr, node) {
			return false
		}
	}
	for _, expr := range term.MatchFields {
		if !nodeFieldSelectorRequirementMatchesNode(expr, node) {
			return false
		}
	}
	return true
}

func nodeSelectorRequirementMatchesNode(expr corev1.NodeSelectorRequirement, node *corev1.Node) bool {
	value, present := node.Labels[expr.Key]
	switch expr.Operator {
	case corev1.NodeSelectorOpIn:
		return present && containsString(expr.Values, value)
	case corev1.NodeSelectorOpNotIn:
		return !present || !containsString(expr.Values, value)
	case corev1.NodeSelectorOpExists:
		return present
	case corev1.NodeSelectorOpDoesNotExist:
		return !present
	case corev1.NodeSelectorOpGt:
		return nodeSelectorNumericCompare(value, expr.Values, func(left, right int) bool { return left > right })
	case corev1.NodeSelectorOpLt:
		return nodeSelectorNumericCompare(value, expr.Values, func(left, right int) bool { return left < right })
	default:
		return false
	}
}

func nodeFieldSelectorRequirementMatchesNode(expr corev1.NodeSelectorRequirement, node *corev1.Node) bool {
	var value string
	var present bool
	switch expr.Key {
	case "metadata.name":
		value = node.Name
		present = strings.TrimSpace(node.Name) != ""
	default:
		return false
	}
	switch expr.Operator {
	case corev1.NodeSelectorOpIn:
		return present && containsString(expr.Values, value)
	case corev1.NodeSelectorOpNotIn:
		return !present || !containsString(expr.Values, value)
	case corev1.NodeSelectorOpExists:
		return present
	case corev1.NodeSelectorOpDoesNotExist:
		return !present
	case corev1.NodeSelectorOpGt:
		return nodeSelectorNumericCompare(value, expr.Values, func(left, right int) bool { return left > right })
	case corev1.NodeSelectorOpLt:
		return nodeSelectorNumericCompare(value, expr.Values, func(left, right int) bool { return left < right })
	default:
		return false
	}
}

func nodeSelectorNumericCompare(leftRaw string, rightValues []string, compare func(int, int) bool) bool {
	if len(rightValues) == 0 {
		return false
	}
	left, err := strconv.Atoi(strings.TrimSpace(leftRaw))
	if err != nil {
		return false
	}
	right, err := strconv.Atoi(strings.TrimSpace(rightValues[0]))
	if err != nil {
		return false
	}
	return compare(left, right)
}

func buildPodMutationPatch(pod *corev1.Pod, annotations map[string]string, affinityChanged bool, affinityFn func() *corev1.Affinity) []jsonPatchOperation {
	patch := make([]jsonPatchOperation, 0, len(annotations)+2)
	if len(annotations) > 0 {
		if pod.Annotations == nil {
			patch = append(patch, jsonPatchOperation{Op: "add", Path: "/metadata/annotations", Value: map[string]string{}})
		}
		keys := make([]string, 0, len(annotations))
		for key := range annotations {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			patch = append(patch, jsonPatchOperation{Op: "add", Path: "/metadata/annotations/" + jsonPointerEscape(key), Value: annotations[key]})
		}
	}
	if affinityChanged {
		op := "add"
		if pod.Spec.Affinity != nil {
			op = "replace"
		}
		patch = append(patch, jsonPatchOperation{
			Op:    op,
			Path:  "/spec/affinity",
			Value: *affinityFn(),
		})
	}
	return patch
}

func appendPreferredHostname(affinity *corev1.Affinity, hostname string) {
	if affinity == nil || strings.TrimSpace(hostname) == "" {
		return
	}
	if affinity.NodeAffinity == nil {
		affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, corev1.PreferredSchedulingTerm{
		Weight: 100,
		Preference: corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      corev1.LabelHostname,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{hostname},
			}},
		},
	})
}

func applyRequiredNodeSelector(affinity *corev1.Affinity, requirement corev1.NodeSelectorRequirement) {
	if affinity == nil || strings.TrimSpace(requirement.Key) == "" || len(requirement.Values) == 0 {
		return
	}
	if affinity.NodeAffinity == nil {
		affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	required := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if required == nil || len(required.NodeSelectorTerms) == 0 {
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{requirement},
			}},
		}
		return
	}
	for i := range required.NodeSelectorTerms {
		if !nodeSelectorTermHasRequirement(required.NodeSelectorTerms[i], requirement) {
			required.NodeSelectorTerms[i].MatchExpressions = append(required.NodeSelectorTerms[i].MatchExpressions, requirement)
		}
	}
	affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = required
}

func nodeSelectorTermHasRequirement(term corev1.NodeSelectorTerm, requirement corev1.NodeSelectorRequirement) bool {
	for _, expr := range term.MatchExpressions {
		if expr.Key != requirement.Key || expr.Operator != requirement.Operator {
			continue
		}
		if equalStringSets(expr.Values, requirement.Values) {
			return true
		}
	}
	return false
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			set[trimmed] = struct{}{}
		}
	}
	return set
}

func intersectStringSets(left, right map[string]struct{}) map[string]struct{} {
	out := map[string]struct{}{}
	for value := range left {
		if _, ok := right[value]; ok {
			out[value] = struct{}{}
		}
	}
	return out
}

func sortedSetValues(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func equalStringSets(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftSet := stringSet(left)
	for _, value := range right {
		if _, ok := leftSet[strings.TrimSpace(value)]; !ok {
			return false
		}
	}
	return true
}

func jsonPointerEscape(value string) string {
	value = strings.ReplaceAll(value, "~", "~0")
	value = strings.ReplaceAll(value, "/", "~1")
	return value
}
