package driver

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type KubernetesNodeIdentity struct {
	Name          string
	UID           string
	Exists        bool
	Ready         bool
	Unschedulable bool
}

type VolumeDesiredState struct {
	Desired           bool
	NodeName          string
	Reason            string
	DemandSource      string
	PVName            string
	PVCNamespace      string
	PVCName           string
	PVDeleting        bool
	PVPhase           corev1.PersistentVolumePhase
	PVCDeleting       bool
	MultipleNodes     bool
	ReferencedNodes   []string
	AttachmentResidue bool
	AttachmentNames   []string
	AttachmentNodes   []string
}

const (
	volumeDemandSourceActivePod                    = "active_pod"
	volumeDemandSourceMultipleActivePods           = "multiple_active_pods"
	volumeDemandSourceTransitionalVolumeAttachment = "transitional_volume_attachment"
	volumeDemandSourceStaleVolumeAttachment        = "stale_volume_attachment"
	volumeDemandSourceVolumeParked                 = "volume_parked"
)

func (r *KubeRuntime) NodeIdentity(ctx context.Context, nodeName string) (KubernetesNodeIdentity, error) {
	if r == nil || !r.enabled {
		return KubernetesNodeIdentity{}, fmt.Errorf("kubernetes runtime is not enabled")
	}
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return KubernetesNodeIdentity{}, fmt.Errorf("node name is required")
	}
	node, err := r.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return KubernetesNodeIdentity{Name: nodeName}, nil
		}
		return KubernetesNodeIdentity{}, err
	}
	identity := KubernetesNodeIdentity{
		Name:          node.Name,
		UID:           string(node.UID),
		Exists:        true,
		Unschedulable: node.Spec.Unschedulable,
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			identity.Ready = condition.Status == corev1.ConditionTrue
			break
		}
	}
	return identity, nil
}

func (r *KubeRuntime) DesiredNodeForVolume(ctx context.Context, volumeHandle string) (VolumeDesiredState, error) {
	if r == nil || !r.enabled {
		return VolumeDesiredState{}, fmt.Errorf("kubernetes runtime is not enabled")
	}
	runtimeCtx, err := r.ResolveVolumeRuntimeContext(ctx, volumeHandle)
	if err != nil {
		return VolumeDesiredState{}, err
	}
	state := VolumeDesiredState{
		PVName:       runtimeCtx.PVName,
		PVCNamespace: runtimeCtx.PVCNamespace,
		PVCName:      runtimeCtx.PVCName,
	}

	pv, err := r.client.CoreV1().PersistentVolumes().Get(ctx, runtimeCtx.PVName, metav1.GetOptions{})
	if err != nil {
		return state, err
	}
	state.PVPhase = pv.Status.Phase
	state.PVDeleting = !pv.DeletionTimestamp.IsZero()
	if state.PVDeleting {
		state.Reason = "persistent_volume_deleting"
		return state, nil
	}
	if pv.Status.Phase == corev1.VolumeReleased || pv.Status.Phase == corev1.VolumeFailed {
		state.Reason = "persistent_volume_" + strings.ToLower(string(pv.Status.Phase))
		return state, nil
	}

	if runtimeCtx.PVCNamespace == "" || runtimeCtx.PVCName == "" {
		state.Reason = "unbound_volume"
		return state, nil
	}
	pvc, err := r.client.CoreV1().PersistentVolumeClaims(runtimeCtx.PVCNamespace).Get(ctx, runtimeCtx.PVCName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			state.Reason = "persistent_volume_claim_not_found"
			return state, nil
		}
		return state, err
	}
	state.PVCDeleting = pvc.DeletionTimestamp != nil
	if state.PVCDeleting {
		state.Reason = "persistent_volume_claim_deleting"
		return state, nil
	}

	nodes := map[string]struct{}{}
	pods, err := r.client.CoreV1().Pods(runtimeCtx.PVCNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return state, err
	}
	for idx := range pods.Items {
		pod := &pods.Items[idx]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		referencesPVC := false
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim == nil {
				continue
			}
			if strings.TrimSpace(volume.PersistentVolumeClaim.ClaimName) == runtimeCtx.PVCName {
				referencesPVC = true
				break
			}
		}
		if !referencesPVC {
			continue
		}
		if strings.TrimSpace(pod.Spec.NodeName) != "" {
			nodes[strings.TrimSpace(pod.Spec.NodeName)] = struct{}{}
		}
	}
	if len(nodes) == 1 {
		for node := range nodes {
			state.Desired = true
			state.NodeName = node
			state.Reason = volumeDemandSourceActivePod
			state.DemandSource = volumeDemandSourceActivePod
			state.ReferencedNodes = []string{node}
			return state, nil
		}
	}
	if len(nodes) > 1 {
		state.Desired = true
		state.MultipleNodes = true
		state.Reason = volumeDemandSourceMultipleActivePods
		state.DemandSource = volumeDemandSourceMultipleActivePods
		for node := range nodes {
			state.ReferencedNodes = append(state.ReferencedNodes, node)
		}
		sort.Strings(state.ReferencedNodes)
		return state, nil
	}

	vas, err := r.client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return state, err
	}
	attachmentNames := []string{}
	attachmentNodes := []string{}
	transitionalNodes := map[string]struct{}{}
	now := time.Now().UTC()
	for idx := range vas.Items {
		va := &vas.Items[idx]
		if !matchesVolumeAttachment(runtimeCtx.PVName, va) {
			continue
		}
		if strings.TrimSpace(va.Spec.NodeName) == "" || !va.DeletionTimestamp.IsZero() {
			continue
		}
		state.AttachmentResidue = true
		attachmentNames = append(attachmentNames, strings.TrimSpace(va.Name))
		attachmentNodes = append(attachmentNodes, strings.TrimSpace(va.Spec.NodeName))
		if volumeAttachmentIsTransitional(va, now, r.staleVolumeAttachmentGrace()) {
			transitionalNodes[strings.TrimSpace(va.Spec.NodeName)] = struct{}{}
		}
	}
	if len(attachmentNames) > 0 {
		sort.Strings(attachmentNames)
		sort.Strings(attachmentNodes)
		state.AttachmentNames = attachmentNames
		state.AttachmentNodes = attachmentNodes
	}
	if len(transitionalNodes) == 1 {
		for node := range transitionalNodes {
			state.Desired = true
			state.NodeName = node
			state.Reason = volumeDemandSourceTransitionalVolumeAttachment
			state.DemandSource = volumeDemandSourceTransitionalVolumeAttachment
			state.ReferencedNodes = []string{node}
			return state, nil
		}
	}
	if len(transitionalNodes) > 1 {
		state.Desired = true
		state.MultipleNodes = true
		state.Reason = volumeDemandSourceTransitionalVolumeAttachment
		state.DemandSource = volumeDemandSourceTransitionalVolumeAttachment
		for node := range transitionalNodes {
			state.ReferencedNodes = append(state.ReferencedNodes, node)
		}
		sort.Strings(state.ReferencedNodes)
		return state, nil
	}
	if state.AttachmentResidue {
		state.Reason = volumeDemandSourceVolumeParked
		state.DemandSource = volumeDemandSourceStaleVolumeAttachment
		return state, nil
	}

	state.Reason = volumeDemandSourceVolumeParked
	state.DemandSource = volumeDemandSourceVolumeParked
	return state, nil
}

func matchesVolumeAttachment(pvName string, va *storagev1.VolumeAttachment) bool {
	if va == nil || va.Spec.Source.PersistentVolumeName == nil {
		return false
	}
	return strings.TrimSpace(*va.Spec.Source.PersistentVolumeName) == strings.TrimSpace(pvName)
}

func (s VolumeDesiredState) DemandSourceOrReason() string {
	if strings.TrimSpace(s.DemandSource) != "" {
		return strings.TrimSpace(s.DemandSource)
	}
	return strings.TrimSpace(s.Reason)
}

func loadStaleVolumeAttachmentGrace(cfg config.CSIPluginConfig) time.Duration {
	seconds, ok := cfg.GetInt(config.StuckAttachmentStaleVAGraceSecondsVar)
	if !ok || seconds <= 0 {
		seconds = 90
	}
	return time.Duration(seconds) * time.Second
}

func volumeAttachmentIsTransitional(va *storagev1.VolumeAttachment, now time.Time, grace time.Duration) bool {
	if va == nil {
		return false
	}
	if grace <= 0 {
		grace = 90 * time.Second
	}
	createdAt := va.CreationTimestamp.Time.UTC()
	if createdAt.IsZero() {
		return false
	}
	return now.Sub(createdAt) < grace
}
