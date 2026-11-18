package driver

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const (
	replacementAttachPrepared = "prepared"
	replacementAttachArmed    = "dispatch-possible"
)

type replacementAttachAttempt struct {
	Version           int    `json:"version"`
	Phase             string `json:"phase,omitempty"`
	ID                string `json:"id"`
	PVUID             string `json:"pvUID"`
	PVCUID            string `json:"pvcUID"`
	Driver            string `json:"driver"`
	VolumeID          string `json:"volumeID"`
	Node              string `json:"node"`
	OriginalNodeUID   string `json:"originalNodeUID"`
	ReplacementUID    string `json:"replacementUID"`
	HistoryGeneration string `json:"historyGeneration"`
	ImageID           int    `json:"imageID"`
	VMID              int    `json:"vmID"`
	OriginalVMID      int    `json:"originalVMID,omitempty"`
	OriginalVMName    string `json:"originalVMName,omitempty"`
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
	return attempt.Version == 1 && attempt.Phase == "" || attempt.Version == 2 && attempt.Phase == replacementAttachArmed
}

func (attempt replacementAttachAttempt) matchesHistory(history VolumeHistoryRecord) bool {
	originalVM := attempt.VMID
	if attempt.OriginalVMID != 0 || attempt.OriginalVMName != "" {
		if attempt.OriginalVMID <= 0 || attempt.OriginalVMID == attempt.VMID || (attempt.Version == 1 && attempt.OriginalVMName == "") {
			return false
		}
		originalVM = attempt.OriginalVMID
	}
	token, err := hex.DecodeString(attempt.ID)
	return err == nil && len(token) == 32 && (attempt.prepared() || attempt.dispatchPossible()) && attempt.PVUID != "" && attempt.PVCUID != "" &&
		attempt.Driver != "" && attempt.VolumeID == history.VolumeID && attempt.Node != "" &&
		attempt.Node == history.LastSuccessfulNodeName && attempt.OriginalNodeUID != "" && attempt.ReplacementUID != "" &&
		attempt.OriginalNodeUID != attempt.ReplacementUID && attempt.OriginalNodeUID == history.LastSuccessfulNodeUID &&
		attempt.ImageID > 0 && attempt.ImageID == history.LastSuccessfulImageID &&
		attempt.VMID > 0 && originalVM == history.LastSuccessfulOpenNebulaVMID &&
		attempt.HistoryGeneration == replacementHistoryGeneration(history) && cephHistoryHasObservedAuthority(history)
}

func (s *ControllerServer) replacementPublishHistory(ctx context.Context, req *csi.ControllerPublishVolumeRequest, runtimeCtx *VolumeRuntimeContext, node KubernetesNodeIdentity) (VolumeHistoryRecord, bool, error) {
	if s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled || s.driver.volumeHistory == nil {
		return VolumeHistoryRecord{}, false, nil
	}
	history, err := s.readAttachmentHistory(ctx, req.VolumeId)
	if err != nil {
		return history, false, attachmentEvidenceUnavailable("publish attachment history is unverified", err)
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
	if attempt.PVUID != "" && (attempt.PVUID != string(pv.UID) || attempt.PVCUID != string(pvc.UID)) {
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
	volume := corev1.UniqueVolumeName(fmt.Sprintf("kubernetes.io/csi/%s^%s", attempt.Driver, attempt.VolumeID))
	if slices.Contains(node.Status.VolumesInUse, volume) {
		return unavailable()
	}
	attachments, err := s.detachVolumeAttachments(ctx, pv.Name, "")
	if err != nil || len(attachments) > 1 {
		return unavailable()
	}
	for _, attachment := range attachments {
		if attachment.attacher != attempt.Driver || attachment.node != attempt.Node || attachment.uid == "" || attachment.deleting || attachment.attached {
			return unavailable()
		}
	}
	pods, err := client.Pods(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return unavailable()
	}
	consumers := 0
	for i := range pods.Items {
		pod := &pods.Items[i]
		references, err := podReferencesClaim(pod, pvc.Name, pvc)
		if err != nil {
			return unavailable()
		}
		if !references || terminalPodMayBeReleased(pod) {
			continue
		}
		if pod.DeletionTimestamp != nil || pod.UID == "" || pod.Spec.NodeName != req.NodeId {
			return unavailable()
		}
		if start := pod.Status.StartTime; start != nil {
			if !start.After(node.CreationTimestamp.Time) {
				return unavailable()
			}
		} else if pod.Status.Phase != corev1.PodPending || len(pod.Status.ContainerStatuses) != 0 ||
			len(pod.Status.InitContainerStatuses) != 0 || len(pod.Status.EphemeralContainerStatuses) != 0 {
			return unavailable()
		}
		consumers++
	}
	if consumers == 0 || (pv.Spec.AccessModes[0] == corev1.ReadWriteOncePod && consumers != 1) {
		return unavailable()
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
	if err := s.recheckDetachVolumeAttachments(ctx, pv.Name, "", attachments); err != nil {
		return attempt, err
	}
	current, err := client.Nodes().Get(ctx, req.NodeId, metav1.GetOptions{})
	if err != nil || current.UID != node.UID || !current.CreationTimestamp.Equal(&node.CreationTimestamp) ||
		current.DeletionTimestamp != nil || slices.Contains(current.Status.VolumesInUse, volume) ||
		!slices.ContainsFunc(current.Status.Conditions, func(c corev1.NodeCondition) bool {
			return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
		}) {
		return unavailable()
	}
	return attempt, ctx.Err()
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
	err := s.retireReplacementAttach(ctx, attempt)
	if err != nil {
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
	_, err = s.driver.volumeHistory.updateHistory(ctx, req.VolumeId, true, func(current *VolumeHistoryRecord) error {
		if current.PendingReplacementAttach == nil || *current.PendingReplacementAttach != attempt || !attempt.matchesHistory(*current) {
			return fmt.Errorf("prepared replacement Ceph attach changed")
		}
		current.PendingReplacementAttach = &armed
		return nil
	})
	if err != nil {
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
