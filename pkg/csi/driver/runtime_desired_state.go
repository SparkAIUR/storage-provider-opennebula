package driver

import (
	"context"
	"fmt"
	"strings"

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
	Desired         bool
	NodeName        string
	Reason          string
	PVName          string
	PVCNamespace    string
	PVCName         string
	PVDeleting      bool
	PVPhase         corev1.PersistentVolumePhase
	PVCDeleting     bool
	MultipleNodes   bool
	ReferencedNodes []string
}

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
			state.Reason = "active_pod"
			state.ReferencedNodes = []string{node}
			return state, nil
		}
	}
	if len(nodes) > 1 {
		state.Desired = true
		state.MultipleNodes = true
		state.Reason = "multiple_active_pods"
		for node := range nodes {
			state.ReferencedNodes = append(state.ReferencedNodes, node)
		}
		return state, nil
	}

	vas, err := r.client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return state, err
	}
	for idx := range vas.Items {
		va := &vas.Items[idx]
		if !matchesVolumeAttachment(runtimeCtx.PVName, va) {
			continue
		}
		if strings.TrimSpace(va.Spec.NodeName) == "" || !va.DeletionTimestamp.IsZero() {
			continue
		}
		state.Desired = true
		state.NodeName = strings.TrimSpace(va.Spec.NodeName)
		state.Reason = "volume_attachment"
		state.ReferencedNodes = []string{state.NodeName}
		return state, nil
	}

	state.Reason = "volume_parked"
	return state, nil
}

func matchesVolumeAttachment(pvName string, va *storagev1.VolumeAttachment) bool {
	if va == nil || va.Spec.Source.PersistentVolumeName == nil {
		return false
	}
	return strings.TrimSpace(*va.Spec.Source.PersistentVolumeName) == strings.TrimSpace(pvName)
}
