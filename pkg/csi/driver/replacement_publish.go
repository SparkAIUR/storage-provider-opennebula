package driver

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

const (
	replacementAttachPrepared = "prepared"
	replacementAttachArmed    = "dispatch-possible"
	replacementAttachDetached = "detach-completed"
)

type replacementDetachCompletion struct {
	VAName string `json:"vaName"`
	VAUID  string `json:"vaUID"`
	DiskID string `json:"diskID"`
	Target string `json:"target"`
	Serial string `json:"serial"`
}

type replacementAttachAttempt struct {
	Version           int                         `json:"version"`
	Phase             string                      `json:"phase,omitempty"`
	ID                string                      `json:"id"`
	PVUID             string                      `json:"pvUID"`
	PVCUID            string                      `json:"pvcUID"`
	VAName            string                      `json:"vaName,omitempty"`
	VAUID             string                      `json:"vaUID,omitempty"`
	Driver            string                      `json:"driver"`
	VolumeID          string                      `json:"volumeID"`
	Node              string                      `json:"node"`
	OriginalNodeUID   string                      `json:"originalNodeUID"`
	ReplacementUID    string                      `json:"replacementUID"`
	HistoryGeneration string                      `json:"historyGeneration"`
	ImageID           int                         `json:"imageID"`
	VMID              int                         `json:"vmID"`
	OriginalVMID      int                         `json:"originalVMID,omitempty"`
	OriginalVMName    string                      `json:"originalVMName,omitempty"`
	DetachCompletion  replacementDetachCompletion `json:"detachCompletion,omitzero"`
}

func replacementHistoryGeneration(history VolumeHistoryRecord) string {
	payload, _ := json.Marshal(struct {
		Version                                                                             int
		Volume, PV, ClaimNamespace, Claim, Backend, Evidence, Node, NodeUID, Target, Serial string
		VM, Image                                                                           int
		Slot                                                                                *int
		Published, Staged                                                                   time.Time
	}{
		Version: history.Version, Volume: history.VolumeID, PV: history.PVName, ClaimNamespace: history.PVCNamespace,
		Claim: history.PVCName, Backend: history.Backend, Evidence: history.EvidenceSource,
		Node: history.LastSuccessfulNodeName, NodeUID: history.LastSuccessfulNodeUID,
		Target: history.LastSuccessfulTarget, Serial: history.LastSuccessfulDeviceSerial,
		VM: history.LastSuccessfulOpenNebulaVMID, Image: history.LastSuccessfulImageID, Slot: history.LastSuccessfulDiskID,
		Published: history.LastSuccessfulPublishTime, Staged: history.LastSuccessfulStageTime,
	})
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func (attempt replacementAttachAttempt) prepared() bool {
	return attempt.Version == 2 && attempt.Phase == replacementAttachPrepared
}

func (attempt replacementAttachAttempt) dispatchPossible() bool {
	return attempt.Version == 2 && attempt.Phase == replacementAttachArmed
}

func (attempt replacementAttachAttempt) detached() bool {
	return attempt.Version == 2 && attempt.Phase == replacementAttachDetached
}

func (attempt replacementAttachAttempt) matchesHistory(history VolumeHistoryRecord) bool {
	originalVM := attempt.VMID
	if attempt.OriginalVMID != 0 || attempt.OriginalVMName != "" {
		if attempt.OriginalVMID <= 0 || attempt.OriginalVMID == attempt.VMID {
			return false
		}
		originalVM = attempt.OriginalVMID
	}
	completion := attempt.DetachCompletion
	diskID, diskErr := strconv.Atoi(completion.DiskID)
	validPhase := (attempt.prepared() || attempt.dispatchPossible()) && completion == (replacementDetachCompletion{}) ||
		attempt.detached() && completion.VAName != "" && completion.VAUID != "" && diskErr == nil && diskID >= 0 &&
			completion.Target != "" && completion.Serial == fmt.Sprintf("onecsi-%d", attempt.ImageID)
	validVA := attempt.VAName != "" && attempt.VAUID != "" &&
		(!attempt.detached() || (completion.VAName == attempt.VAName && completion.VAUID == attempt.VAUID))
	token, err := hex.DecodeString(attempt.ID)
	return err == nil && len(token) == 32 && validPhase && validVA && attempt.PVUID != "" && attempt.PVCUID != "" &&
		attempt.Driver != "" && attempt.VolumeID == history.VolumeID && attempt.Node != "" &&
		attempt.Node == history.LastSuccessfulNodeName && attempt.OriginalNodeUID != "" && attempt.ReplacementUID != "" &&
		attempt.OriginalNodeUID != attempt.ReplacementUID && attempt.OriginalNodeUID == history.LastSuccessfulNodeUID &&
		attempt.ImageID > 0 && attempt.ImageID == history.LastSuccessfulImageID &&
		attempt.VMID > 0 && originalVM == history.LastSuccessfulOpenNebulaVMID &&
		attempt.HistoryGeneration == replacementHistoryGeneration(history) && cephHistoryHasObservedAuthority(history)
}

func (s *ControllerServer) retireCompletedReplacement(ctx context.Context, history VolumeHistoryRecord) (bool, error) {
	attempt := history.PendingReplacementAttach
	if attempt == nil || !attempt.detached() || !attempt.matchesHistory(history) || attempt.Driver != s.driver.name {
		return false, nil
	}
	runtime := s.driver.kubeRuntime
	if runtime == nil || !runtime.enabled || runtime.client == nil {
		return false, attachmentEvidenceUnavailable("completed replacement VolumeAttachment identity is unavailable", nil)
	}
	completion := attempt.DetachCompletion
	va, err := runtime.client.StorageV1().VolumeAttachments().Get(ctx, completion.VAName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, attachmentEvidenceUnavailable("completed replacement VolumeAttachment identity is unverified", err)
	}
	if err == nil && (va.UID == "" || string(va.UID) == completion.VAUID) {
		return false, nil
	}
	if err := s.retireVerifiedReplacement(ctx, *attempt); err != nil {
		return false, err
	}
	return true, nil
}

func (s *ControllerServer) replacementPublishHistory(ctx context.Context, req *csi.ControllerPublishVolumeRequest, runtimeCtx *VolumeRuntimeContext, node KubernetesNodeIdentity) (VolumeHistoryRecord, bool, error) {
	if s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled || s.driver.volumeHistory == nil {
		return VolumeHistoryRecord{}, false, nil
	}
	history, err := s.readAttachmentHistory(ctx, req.VolumeId)
	if err != nil {
		return history, false, attachmentEvidenceUnavailable("publish attachment history is unverified", err)
	}
	retired, err := s.retireCompletedReplacement(ctx, history)
	if err != nil {
		return history, false, err
	}
	if retired {
		history, err = s.readAttachmentHistory(ctx, req.VolumeId)
		if err != nil {
			return history, false, attachmentEvidenceUnavailable("publish attachment history is unverified", err)
		}
	}
	if history.PendingReplacementAttach != nil {
		return history, true, nil
	}
	backend, _ := runtimeCtx.verifiedAttachmentBackend()
	replacement := (isCephBackend(history.Backend) || isCephBackend(backend)) && volumeHistoryHasObservedSuccess(history) &&
		history.LastSuccessfulNodeName == req.NodeId && history.LastSuccessfulNodeUID != "" && history.LastSuccessfulNodeUID != node.UID
	return history, replacement, nil
}

func (s *ControllerServer) verifyReplacementBinding(ctx context.Context, req *csi.ControllerPublishVolumeRequest, history VolumeHistoryRecord, attempt replacementAttachAttempt, diskID int) (replacementAttachAttempt, error) {
	unavailable := func() (replacementAttachAttempt, error) {
		return attempt, status.Error(codes.Unavailable, "replacement Ceph attach release, binding or demand is unverified")
	}
	client := s.driver.kubeRuntime.client.CoreV1()
	pv, err := s.ownedDeletionPV(ctx, req.VolumeId)
	if err != nil || pv == nil || pv.DeletionTimestamp != nil || pv.Status.Phase != corev1.VolumeBound ||
		pv.Name != history.PVName || !cephRetentionAccessMode(pv.Spec.AccessModes) {
		return unavailable()
	}
	backend, err := persistentVolumeBackend(pv)
	if err != nil || !isCephBackend(backend) {
		return unavailable()
	}
	pvc, replaced, err := s.driver.kubeRuntime.attachmentClaim(ctx, pv)
	if err != nil || replaced || pvc == nil || pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound ||
		!slices.Equal(pv.Spec.AccessModes, pvc.Spec.AccessModes) || pvc.Namespace != history.PVCNamespace || pvc.Name != history.PVCName {
		return unavailable()
	}
	pinned := attempt.PVUID != ""
	if pinned && (attempt.PVUID != string(pv.UID) || attempt.PVCUID != string(pvc.UID)) {
		return unavailable()
	}
	attempt.PVUID, attempt.PVCUID = string(pv.UID), string(pvc.UID)
	mode := req.GetVolumeCapability().GetAccessMode().GetMode()
	if req.Readonly || (mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER && mode != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER) {
		return unavailable()
	}
	node, err := client.Nodes().Get(ctx, req.NodeId, metav1.GetOptions{})
	if err != nil || string(node.UID) != attempt.ReplacementUID || node.DeletionTimestamp != nil || node.CreationTimestamp.IsZero() ||
		!slices.ContainsFunc(node.Status.Conditions, func(c corev1.NodeCondition) bool {
			return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
		}) {
		return unavailable()
	}
	attachments, err := s.detachVolumeAttachments(ctx, pv.Name, "")
	if err != nil || len(attachments) != 1 {
		return unavailable()
	}
	for name, attachment := range attachments {
		if name == "" || attachment.attacher != attempt.Driver || attachment.node != attempt.Node || attachment.uid == "" ||
			attachment.deleting || attachment.attached || !attachment.created.After(node.CreationTimestamp.Time) || validateAttachmentImageAssertions(attachment.metadata, attempt.ImageID) != nil ||
			(pinned && (attempt.VAName != name || attempt.VAUID != string(attachment.uid))) {
			return unavailable()
		}
		attempt.VAName, attempt.VAUID = name, string(attachment.uid)
	}
	runtimeCtx := &VolumeRuntimeContext{PVName: pv.Name, PVCNamespace: pvc.Namespace, PVCName: pvc.Name, PVCUID: pvc.UID,
		verifiedClaimBinding: true, PVAnnotations: pv.Annotations, PVCAnnotations: pvc.Annotations, Backend: backend, AccessModes: pv.Spec.AccessModes}
	control := resolvedRecoveryControlState(req.VolumeId, runtimeCtx, VolumeRecoveryControlState{})
	if control.Invalid || control.Active() {
		return attempt, status.Error(codes.FailedPrecondition, "replacement Ceph attach is blocked by recovery controls")
	}
	protection, err := s.localRWOProtectionProjection(ctx, req.VolumeId, req.NodeId, runtimeCtx, false, true)
	if err != nil || protection.Invalid || (protection.Protected && protection.RequiredNode != "" && protection.RequiredNode != req.NodeId && !protection.OverrideUsed) {
		return unavailable()
	}
	target := opennebula.HostArtifactAttachmentTarget{NodeName: attempt.Node, VMID: attempt.VMID, ImageID: attempt.ImageID, DiskID: diskID}
	if err := s.recheckAttachmentAuthority(ctx, req.VolumeId, runtimeCtx, target, pv, pvc, false); err != nil {
		return attempt, err
	}
	current, err := client.Nodes().Get(ctx, req.NodeId, metav1.GetOptions{})
	if err != nil || current.UID != node.UID || !current.CreationTimestamp.Equal(&node.CreationTimestamp) ||
		current.DeletionTimestamp != nil ||
		!slices.ContainsFunc(current.Status.Conditions, func(c corev1.NodeCondition) bool {
			return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
		}) {
		return unavailable()
	}
	if waiting, err := s.replacementWaitingConsumers(ctx, pv, pvc, current); err != nil || !waiting {
		return unavailable()
	}
	if err := s.recheckDetachVolumeAttachments(ctx, pv.Name, "", attachments); err != nil {
		return attempt, err
	}
	return attempt, ctx.Err()
}

func (s *ControllerServer) replacementWaitingConsumers(ctx context.Context, pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim, node *corev1.Node) (bool, error) {
	pods, err := s.driver.kubeRuntime.client.CoreV1().Pods(pv.Spec.ClaimRef.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, attachmentEvidenceUnavailable("replacement consumer state is unreadable", err)
	}
	consumers := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		references, err := podReferencesClaim(pod, pv.Spec.ClaimRef.Name, pvc)
		if err != nil {
			return false, attachmentEvidenceUnavailable("replacement consumer claim is unverified", err)
		}
		if !references || terminalPodMayBeReleased(pod) {
			continue
		}
		if pod.DeletionTimestamp != nil || pod.UID == "" || pod.Spec.NodeName != node.Name ||
			node.CreationTimestamp.IsZero() || !pod.CreationTimestamp.After(node.CreationTimestamp.Time) ||
			!replacementPodPendingBeforeMount(pod) {
			return false, attachmentEvidenceUnavailable("replacement consumer may already be using the volume", nil)
		}
		consumers++
	}
	if consumers > 1 && pv.Spec.AccessModes[0] == corev1.ReadWriteOncePod {
		return false, attachmentEvidenceUnavailable("replacement volume has multiple consumers", nil)
	}
	return consumers > 0, ctx.Err()
}

func replacementPodPendingBeforeMount(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodPending || pod.Status.PodIP != "" || len(pod.Status.PodIPs) != 0 ||
		slices.ContainsFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool {
			return c.Status != corev1.ConditionFalse && (c.Type == corev1.PodReadyToStartContainers || c.Type == corev1.PodReady || c.Type == corev1.ContainersReady)
		}) {
		return false
	}
	if start := pod.Status.StartTime; start != nil &&
		(start.IsZero() || start.Before(&pod.CreationTimestamp) || start.After(time.Now())) {
		return false
	}
	return premountContainerStatuses(pod.Status.ContainerStatuses, func(name string) bool {
		return slices.ContainsFunc(pod.Spec.Containers, func(c corev1.Container) bool { return c.Name == name })
	}) && premountContainerStatuses(pod.Status.InitContainerStatuses, func(name string) bool {
		return slices.ContainsFunc(pod.Spec.InitContainers, func(c corev1.Container) bool { return c.Name == name })
	}) && premountContainerStatuses(pod.Status.EphemeralContainerStatuses, func(name string) bool {
		return slices.ContainsFunc(pod.Spec.EphemeralContainers, func(c corev1.EphemeralContainer) bool { return c.Name == name })
	})
}

func premountContainerStatuses(statuses []corev1.ContainerStatus, declared func(string) bool) bool {
	seen := make(map[string]bool, len(statuses))
	for _, container := range statuses {
		waiting := container.State.Waiting
		if container.Name == "" || seen[container.Name] || !declared(container.Name) ||
			container.Ready || (container.Started != nil && *container.Started) || container.RestartCount != 0 ||
			container.ContainerID != "" || container.ImageID != "" ||
			container.LastTerminationState != (corev1.ContainerState{}) ||
			container.State.Running != nil || container.State.Terminated != nil ||
			waiting == nil || waiting.Message != "" || (waiting.Reason != "ContainerCreating" && waiting.Reason != "PodInitializing") {
			return false
		}
		seen[container.Name] = true
	}
	return true
}

func (s *ControllerServer) verifyReplacementAttempt(ctx context.Context, req *csi.ControllerPublishVolumeRequest, attempt replacementAttachAttempt, attached bool) (*opennebula.VolumeDiskRecord, error) {
	if attempt.Driver != s.driver.name || attempt.VolumeID != req.VolumeId || attempt.Node != req.NodeId {
		return nil, attachmentEvidenceUnavailable("replacement Ceph attach scope changed", nil)
	}
	if !attempt.dispatchPossible() {
		return nil, attachmentEvidenceUnavailable("replacement Ceph attach was not authorized to dispatch", nil)
	}
	disk, err := s.inspectReplacementDisk(ctx, attempt, attached)
	if err != nil {
		return nil, err
	}
	history, err := s.readAttachmentHistory(ctx, req.VolumeId)
	if err != nil || history.PendingReplacementAttach == nil || *history.PendingReplacementAttach != attempt || !attempt.matchesHistory(history) {
		return nil, attachmentEvidenceUnavailable("replacement Ceph attach history changed", err)
	}
	diskID := -1
	if disk != nil {
		diskID = disk.DiskID
	}
	_, err = s.verifyReplacementBinding(ctx, req, history, attempt, diskID)
	return disk, err
}

func (s *ControllerServer) inspectReplacementDisk(ctx context.Context, attempt replacementAttachAttempt, attached bool) (*opennebula.VolumeDiskRecord, error) {
	inspector, ok := s.volumeProvider.(opennebula.ReplacementAttachInspector)
	if !ok {
		return nil, attachmentEvidenceUnavailable("replacement Ceph attach inspection is unavailable", nil)
	}
	disk, err := inspector.InspectReplacementAttach(ctx, attempt.VolumeID, attempt.Node, attempt.ImageID, attempt.VMID, attempt.ID)
	if err != nil || (disk != nil) != attached {
		return nil, attachmentEvidenceUnavailable("replacement Ceph attach outcome is unverified", err)
	}
	if attempt.OriginalVMID != 0 {
		if name, err := inspector.InspectReplacementRelease(ctx, attempt.ImageID, attempt.OriginalVMID, attempt.OriginalVMName); err != nil || (name != "" && name != attempt.OriginalVMName) {
			return nil, attachmentEvidenceUnavailable("original Ceph attachment release is unverified", err)
		}
	}
	return disk, nil
}

func (s *ControllerServer) retireIdleReplacementAttach(ctx context.Context, attempt replacementAttachAttempt) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if err := retryOnConflict(retry.DefaultRetry, func() error {
		return s.retireReplacementAttach(ctx, attempt)
	}); err != nil {
		klog.V(2).InfoS("Could not retire idle replacement Ceph attach", "volumeID", attempt.VolumeID, "err", err)
	}
}

func (s *ControllerServer) retireReplacementAttach(ctx context.Context, attempt replacementAttachAttempt) error {
	_, err := s.driver.volumeHistory.updateHistory(ctx, attempt.VolumeID, true, func(current *VolumeHistoryRecord) error {
		if current.PendingReplacementAttach == nil || *current.PendingReplacementAttach != attempt || !attempt.matchesHistory(*current) {
			return fmt.Errorf("replacement Ceph attach retirement does not match current history")
		}
		current.PendingReplacementAttach = nil
		return nil
	})
	return err
}

func (s *ControllerServer) beginReplacementAttach(ctx context.Context, req *csi.ControllerPublishVolumeRequest, history VolumeHistoryRecord, imageID, vmID int, node KubernetesNodeIdentity) (*replacementAttachAttempt, error) {
	inspector, ok := s.volumeProvider.(opennebula.ReplacementAttachInspector)
	if !ok {
		return nil, attachmentEvidenceUnavailable("replacement Ceph attach inspection is unavailable", nil)
	}
	attempt := replacementAttachAttempt{Version: 2, Phase: replacementAttachPrepared, Driver: s.driver.name, VolumeID: req.VolumeId, Node: req.NodeId,
		OriginalNodeUID: history.LastSuccessfulNodeUID, ReplacementUID: node.UID, ImageID: imageID, VMID: vmID,
		HistoryGeneration: replacementHistoryGeneration(history)}
	existing := history.PendingReplacementAttach
	if existing != nil {
		if !existing.prepared() || !existing.matchesHistory(history) || existing.VolumeID != attempt.VolumeID ||
			existing.Node != attempt.Node || existing.Driver != attempt.Driver || existing.ReplacementUID != attempt.ReplacementUID ||
			existing.ImageID != imageID || existing.VMID != vmID {
			return nil, attachmentEvidenceUnavailable("prepared replacement Ceph attach scope changed", nil)
		}
		attempt = *existing
	} else {
		token := make([]byte, 32)
		if _, err := rand.Read(token); err != nil {
			return nil, attachmentEvidenceUnavailable("replacement Ceph attach identifier is unavailable", err)
		}
		attempt.ID = hex.EncodeToString(token)
	}
	disk, err := inspector.InspectReplacementAttach(ctx, req.VolumeId, req.NodeId, imageID, vmID, "")
	if err != nil || disk != nil {
		return nil, attachmentEvidenceUnavailable("replacement Ceph attach requires settled provider absence", err)
	}
	if vmID != history.LastSuccessfulOpenNebulaVMID {
		attempt.OriginalVMID = history.LastSuccessfulOpenNebulaVMID
		name, err := inspector.InspectReplacementRelease(ctx, imageID, attempt.OriginalVMID, attempt.OriginalVMName)
		if err != nil || (existing != nil && name != "" && name != attempt.OriginalVMName) {
			return nil, attachmentEvidenceUnavailable("original Ceph attachment release is unverified", err)
		}
		if existing == nil {
			attempt.OriginalVMName = name
		}
	}
	attempt, err = s.verifyReplacementBinding(ctx, req, history, attempt, -1)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		_, err = s.driver.volumeHistory.updateHistory(ctx, req.VolumeId, true, func(current *VolumeHistoryRecord) error {
			if current.PendingReplacementAttach != nil || !attempt.matchesHistory(*current) {
				return fmt.Errorf("replacement Ceph attach history changed")
			}
			current.PendingReplacementAttach = &attempt
			return nil
		})
		if err != nil {
			s.retireIdleReplacementAttach(ctx, attempt)
			return nil, attachmentEvidenceUnavailable("replacement Ceph attach admission could not be persisted", err)
		}
	}
	if err := s.checkReplacementAttemptReadback(ctx, attempt); err != nil {
		s.retireIdleReplacementAttach(ctx, attempt)
		return nil, err
	}
	armed := attempt
	armed.Phase = replacementAttachArmed
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return nil, attachmentEvidenceUnavailable("replacement Ceph dispatch identifier is unavailable", err)
	}
	armed.ID = hex.EncodeToString(token)
	_, err = s.driver.volumeHistory.updateHistory(ctx, req.VolumeId, true, func(current *VolumeHistoryRecord) error {
		if current.PendingReplacementAttach == nil || *current.PendingReplacementAttach != attempt || !attempt.matchesHistory(*current) {
			return fmt.Errorf("prepared replacement Ceph attach changed")
		}
		current.PendingReplacementAttach = &armed
		return nil
	})
	if err != nil {
		s.retireIdleReplacementAttach(ctx, armed)
		return nil, attachmentEvidenceUnavailable("replacement Ceph dispatch authorization is unverified", err)
	}
	if err := s.checkReplacementAttemptReadback(ctx, armed); err != nil {
		s.retireIdleReplacementAttach(ctx, armed)
		return nil, err
	}
	return &armed, nil
}

func (s *ControllerServer) checkReplacementAttemptReadback(ctx context.Context, attempt replacementAttachAttempt) error {
	observed, err := s.readAttachmentHistory(ctx, attempt.VolumeID)
	if err != nil || observed.PendingReplacementAttach == nil || *observed.PendingReplacementAttach != attempt || !attempt.matchesHistory(observed) {
		return attachmentEvidenceUnavailable("replacement Ceph attach admission readback failed", err)
	}
	return nil
}

func (s *ControllerServer) completeReplacementAttach(ctx context.Context, req *csi.ControllerPublishVolumeRequest, attempt replacementAttachAttempt, node KubernetesNodeIdentity, runtimeCtx *VolumeRuntimeContext, protection *LocalRWOProtectionDecision) (successfulAttachmentObservation, error) {
	var observation successfulAttachmentObservation
	if attempt.Driver != s.driver.name || attempt.VolumeID != req.VolumeId || attempt.Node != req.NodeId || attempt.ReplacementUID != node.UID {
		return observation, attachmentEvidenceUnavailable("replacement Ceph attach scope changed", nil)
	}
	disk, err := s.verifyReplacementAttempt(ctx, req, attempt, true)
	if err != nil {
		return observation, err
	}
	restoreCtx := opennebula.WithPublishMutationAuthorization(ctx, req.VolumeId, req.NodeId, attempt.VMID, attempt.ImageID, func(ctx context.Context) error {
		current, err := s.verifyReplacementAttempt(ctx, req, attempt, true)
		if err == nil && *current != *disk {
			return attachmentEvidenceUnavailable("replacement Ceph attach disk changed", nil)
		}
		return err
	})
	if err := s.restoreVolumeCapacity(restoreCtx, req.VolumeId, attempt.VMID, attempt.ImageID); err != nil {
		return observation, err
	}
	if err := s.rejectIfLocalDeviceReportBlocksAttachSuccess(ctx, req.VolumeId, req.NodeId, disk.Target, req.GetVolumeContext(), runtimeCtx); err != nil {
		return observation, err
	}
	observation, err = s.verifyPublishedAttachment(ctx, req.VolumeId, req.NodeId, attempt.ImageID, attempt.VMID, disk.Target, "", node, true)
	if err != nil {
		return observation, err
	}
	current, err := s.verifyReplacementAttempt(ctx, req, attempt, true)
	if err != nil || *current != *disk || observation.diskID != disk.DiskID || observation.serial != disk.Serial {
		return observation, attachmentEvidenceUnavailable("replacement Ceph attach disk changed", err)
	}
	observation.replacementAttempt = &attempt
	return observation, s.recordSuccessfulLocalVolumePublish(ctx, req.VolumeId, observation, req.GetVolumeContext(), runtimeCtx, protection)
}
