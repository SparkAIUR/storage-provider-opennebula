package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// cephRetentionAuthority contains only identity and authority facts that must
// remain unchanged across the two bounded provider/Kubernetes observations.
// Resource versions and node heartbeats are deliberately not authority tokens.
type cephRetentionAuthority struct {
	PVName, PVCNamespace, PVCName, VAName string
	PVUID, PVCUID, NodeUID, VAUID         types.UID
	Capacity                              int64
	RequestedCapacity                     string
	ConsumerUIDs                          []string
	History                               VolumeHistoryRecord
	AttachmentMetadata                    map[string]string
}

func cephRetentionUnavailable(message string, err error) error {
	return attachmentEvidenceUnavailable("cannot safely retain Ceph attachment: "+message, err)
}

// rescueCephSameNodeUnpublish completes only logical deletion of a stale VA.
// The disk stays owned by the same VM; no physical detach is asserted or recorded.
// This is separate from local-only maintenance and sticky-grace eligibility.
func (s *ControllerServer) rescueCephSameNodeUnpublish(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest, imageID, nodeID int, runtimeCtx *VolumeRuntimeContext) (handled bool, response *csi.ControllerUnpublishVolumeResponse, resultErr error) {
	defer func() {
		if handled && ctx.Err() != nil {
			response = nil
			resultErr = status.FromContextError(ctx.Err()).Err()
		}
	}()
	if s == nil || s.driver == nil || req == nil || s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
		return false, nil, nil
	}
	if s.driver.operationLocks == nil {
		return true, nil, cephRetentionUnavailable("operation locks are unavailable", nil)
	}
	nodeRelease := s.driver.operationLocks.Acquire(controllerNodeLockKey(req.NodeId))
	defer nodeRelease()
	release := s.driver.operationLocks.Acquire(controllerVolumeLockKey(req.VolumeId))
	defer release()
	if err := ctx.Err(); err != nil {
		return true, nil, status.FromContextError(err).Err()
	}
	desired, err := s.driver.kubeRuntime.DesiredNodeForVolume(ctx, req.VolumeId)
	if err != nil {
		return true, nil, cephRetentionUnavailable("desired-node lookup failed", err)
	}
	if desired.MultipleNodes {
		return true, nil, cephRetentionUnavailable("volume has consumers on multiple nodes", nil)
	}
	if !desired.Desired || desired.NodeName != req.NodeId {
		return false, nil, nil
	}
	if desired.DemandSource != volumeDemandSourceActivePod {
		return true, nil, cephRetentionUnavailable("current active Pod demand is unverified", nil)
	}
	if enabled, known := s.driver.PluginConfig.GetBool(config.StuckAttachmentReconcilerEnabledVar); known && !enabled {
		return true, nil, status.Error(codes.FailedPrecondition, "Ceph same-node retention requires the attachment reconciler for durable orphan cleanup")
	}
	first, err := s.cephRetentionAuthority(ctx, req, runtimeCtx, nodeID, -1)
	if err != nil {
		return true, nil, err
	}
	inspector, ok := s.volumeProvider.(opennebula.SameNodeRetentionInspector)
	if !ok {
		return true, nil, cephRetentionUnavailable("provider cannot verify stable same-node retention", nil)
	}
	firstDisk, err := inspector.InspectSameNodeRetention(ctx, req.VolumeId, req.NodeId)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return true, nil, status.FromContextError(ctxErr).Err()
	}
	if err != nil {
		return true, nil, cephRetentionUnavailable("provider observation failed", err)
	}
	if err := validateCephRetentionDisk(req, imageID, nodeID, first, firstDisk); err != nil {
		return true, nil, err
	}
	// A second owner census and fresh VM observation bounds external changes;
	// controller locks serialize our own operations but are not provider fencing.
	firstObservation := *firstDisk
	lastDisk, err := inspector.InspectSameNodeRetention(ctx, req.VolumeId, req.NodeId)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return true, nil, status.FromContextError(ctxErr).Err()
	}
	if err != nil {
		return true, nil, cephRetentionUnavailable("provider recheck failed", err)
	}
	if lastDisk == nil || firstObservation != *lastDisk {
		return true, nil, cephRetentionUnavailable("provider identity or capacity changed during retention checks", nil)
	}
	lastRuntime, err := s.driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, req.VolumeId)
	if err != nil {
		return true, nil, cephRetentionUnavailable("final volume runtime lookup failed", err)
	}
	last, err := s.cephRetentionAuthority(ctx, req, lastRuntime, nodeID, lastDisk.DiskID)
	if err != nil {
		return true, nil, err
	}
	if !reflect.DeepEqual(first, last) {
		return true, nil, cephRetentionUnavailable("Kubernetes identity or demand changed during retention checks", nil)
	}
	if err := ctx.Err(); err != nil {
		return true, nil, status.FromContextError(err).Err()
	}
	s.driver.metrics.RecordSameNodeRestartReuse("disk")
	if recorder := s.driver.kubeRuntime.recorder; recorder != nil {
		recorder.Event(&corev1.ObjectReference{
			APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: last.PVCNamespace, Name: last.PVCName, UID: last.PVCUID,
		}, corev1.EventTypeNormal, eventReasonSameNodeDetachRescued, "retained the Ceph disk on the same node while completing logical attachment deletion")
	}
	return true, &csi.ControllerUnpublishVolumeResponse{}, nil
}

func validateCephRetentionDisk(req *csi.ControllerUnpublishVolumeRequest, imageID, nodeID int, authority *cephRetentionAuthority, disk *opennebula.SameNodeRetentionEvidence) error {
	if disk == nil || disk.VolumeID != req.VolumeId || disk.ImageID != imageID || disk.NodeID != nodeID || disk.DiskID < 0 || disk.Target == "" || disk.DeviceSerial == "" || disk.SizeBytes != authority.Capacity {
		return cephRetentionUnavailable("provider proof does not match the bound volume, VM or capacity", nil)
	}
	if err := validateAttachmentImageAssertions(authority.AttachmentMetadata, disk.ImageID); err != nil {
		return cephRetentionUnavailable("VolumeAttachment device identity contradicts the pinned image", err)
	}
	if observed, present := authority.AttachmentMetadata[publishContextDeviceSerial]; present && strings.TrimSpace(observed) != disk.DeviceSerial {
		return cephRetentionUnavailable("VolumeAttachment serial contradicts the current device", nil)
	}
	if target, present := authority.AttachmentMetadata["volumeName"]; present && strings.TrimPrefix(strings.TrimSpace(target), "/dev/") != disk.Target {
		return cephRetentionUnavailable("VolumeAttachment target contradicts the current device", nil)
	}
	history := authority.History
	if !cephHistoryHasObservedAuthority(history) {
		return cephRetentionUnavailable("history has no observed success authority", nil)
	}
	for key, expected := range map[string]string{paramPVName: authority.PVName, paramPVCName: authority.PVCName, paramPVCNamespace: authority.PVCNamespace} {
		if observed, present := authority.AttachmentMetadata[key]; present && strings.TrimSpace(observed) != expected {
			return cephRetentionUnavailable("VolumeAttachment metadata contradicts the current binding", nil)
		}
	}
	if history.LastSuccessfulImageID != disk.ImageID || history.LastSuccessfulNodeName != req.NodeId || history.LastSuccessfulNodeUID != string(authority.NodeUID) {
		return cephRetentionUnavailable("observed volume history does not match image and node identity", nil)
	}
	if (history.LastSuccessfulOpenNebulaVMID > 0 && history.LastSuccessfulOpenNebulaVMID != disk.NodeID) || (history.LastSuccessfulDiskID > 0 && history.LastSuccessfulDiskID != disk.DiskID) || (history.LastSuccessfulTarget != "" && history.LastSuccessfulTarget != disk.Target) || (history.LastSuccessfulDeviceSerial != "" && history.LastSuccessfulDeviceSerial != disk.DeviceSerial) {
		return cephRetentionUnavailable("current device identity contradicts observed history", nil)
	}
	if disk.DeviceSerial != fmt.Sprintf("onecsi-%d", disk.ImageID) {
		return cephRetentionUnavailable("device serial does not identify the pinned image", nil)
	}
	return nil
}

func cephRetentionAccessMode(modes []corev1.PersistentVolumeAccessMode) bool {
	return len(modes) == 1 && hasSingleWriterAccessMode(modes)
}

func (s *ControllerServer) cephRetentionAuthority(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest, runtimeCtx *VolumeRuntimeContext, nodeID, diskID int) (*cephRetentionAuthority, error) {
	backend, err := runtimeCtx.verifiedAttachmentBackend()
	if err != nil || !isCephBackend(backend) {
		return nil, cephRetentionUnavailable("Ceph backend identity changed", err)
	}
	runtime := s.driver.kubeRuntime
	pv, err := runtime.client.CoreV1().PersistentVolumes().Get(ctx, runtimeCtx.PVName, metav1.GetOptions{})
	if err != nil {
		return nil, cephRetentionUnavailable("PV lookup failed", err)
	}
	currentBackend, backendErr := persistentVolumeBackend(pv)
	if pv.UID == "" || pv.DeletionTimestamp != nil || pv.Status.Phase != corev1.VolumeBound || pv.Spec.CSI == nil || pv.Spec.CSI.Driver != s.driver.name || pv.Spec.CSI.VolumeHandle != req.VolumeId || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID == "" || backendErr != nil || !isCephBackend(currentBackend) {
		return nil, cephRetentionUnavailable("bound PV identity is incomplete or inconsistent", nil)
	}
	pvc, err := runtime.client.CoreV1().PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(ctx, pv.Spec.ClaimRef.Name, metav1.GetOptions{})
	if err != nil {
		return nil, cephRetentionUnavailable("PVC lookup failed", err)
	}
	if pvc.UID != pv.Spec.ClaimRef.UID || pvc.UID == "" || pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName != pv.Name {
		return nil, cephRetentionUnavailable("bound PVC identity changed or is incomplete", nil)
	}
	if !cephRetentionAccessMode(pv.Spec.AccessModes) || !slices.Equal(pv.Spec.AccessModes, pvc.Spec.AccessModes) {
		return nil, status.Error(codes.FailedPrecondition, "Ceph same-node retention requires matching RWO or RWOP access modes")
	}
	completedCapacity := pv.Spec.Capacity.Storage()
	capacity := completedCapacity.Value()
	requestedCapacity := pvc.Spec.Resources.Requests.Storage()
	if requestedCapacity.Sign() <= 0 || requestedCapacity.Cmp(*completedCapacity) > 0 || capacity <= 0 || pvc.Status.Capacity.Storage().Cmp(*completedCapacity) != 0 || len(pvc.Status.Conditions) > 0 || len(pvc.Status.AllocatedResourceStatuses) > 0 {
		return nil, cephRetentionUnavailable("PVC capacity is pending or inconsistent", nil)
	}
	identity, err := runtime.NodeIdentity(ctx, req.NodeId)
	if err != nil {
		return nil, cephRetentionUnavailable("node identity lookup failed", err)
	}
	if !identity.Exists || !identity.Ready || identity.Deleting || identity.UID == "" {
		return nil, cephRetentionUnavailable("node identity is unknown or not Ready", nil)
	}
	authority := &cephRetentionAuthority{PVName: pv.Name, PVUID: pv.UID, PVCNamespace: pvc.Namespace, PVCName: pvc.Name, PVCUID: pvc.UID, NodeUID: types.UID(identity.UID), Capacity: capacity, RequestedCapacity: requestedCapacity.String()}
	attachments, err := s.detachVolumeAttachments(ctx, pv.Name, "")
	if err != nil {
		return nil, cephRetentionUnavailable("VolumeAttachment lookup failed", err)
	}
	for name, va := range attachments {
		if authority.VAUID != "" || va.attacher != s.driver.name || va.node != req.NodeId || va.uid == "" || !va.deleting || !va.attached {
			return nil, cephRetentionUnavailable("deleting VolumeAttachment ownership is ambiguous or unverified", nil)
		}
		authority.VAName = name
		authority.VAUID = va.uid
		authority.AttachmentMetadata = va.metadata
	}
	if authority.VAName == "" {
		return nil, cephRetentionUnavailable("owned deleting VolumeAttachment was not observed", nil)
	}
	if backend, present := authority.AttachmentMetadata[annotationBackend]; present && !isCephBackend(backend) {
		return nil, cephRetentionUnavailable("VolumeAttachment backend contradicts Ceph binding", nil)
	}
	currentRuntime := &VolumeRuntimeContext{PVName: pv.Name, PVCNamespace: pvc.Namespace, PVCName: pvc.Name, PVCUID: pvc.UID, PVAnnotations: cloneStringMap(pv.Annotations), PVCAnnotations: cloneStringMap(pvc.Annotations), Backend: backend, AccessModes: append([]corev1.PersistentVolumeAccessMode(nil), pv.Spec.AccessModes...)}
	control := resolvedRecoveryControlState(req.VolumeId, currentRuntime, VolumeRecoveryControlState{})
	if control.Invalid || control.Active() {
		return nil, status.Error(codes.FailedPrecondition, "Ceph same-node retention is blocked by active or invalid recovery controls")
	}
	history, err := s.readAttachmentHistory(ctx, req.VolumeId)
	if err != nil {
		return nil, cephRetentionUnavailable("volume history read failed", err)
	}
	if history.VolumeID != req.VolumeId || !cephHistoryHasObservedAuthority(history) {
		return nil, cephRetentionUnavailable("observed volume history is unavailable", nil)
	}
	target := opennebula.HostArtifactAttachmentTarget{
		NodeName: req.NodeId, VMID: nodeID, DiskID: diskID, ImageID: history.LastSuccessfulImageID,
	}
	if err := s.attachmentProtectionChecks(ctx, req.VolumeId, currentRuntime, target); err != nil {
		return nil, err
	}
	authority.History = history
	if err := s.recheckAttachmentAuthority(ctx, req.VolumeId, currentRuntime, target, pv, pvc, false); err != nil {
		return nil, err
	}
	pods, err := runtime.client.CoreV1().Pods(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, cephRetentionUnavailable("current Pod demand lookup failed", err)
	}
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		referencesPVC, err := podReferencesClaim(&pod, pvc.Name, pvc)
		if err != nil {
			return nil, cephRetentionUnavailable("current Pod claim is unverified", err)
		}
		if !referencesPVC {
			continue
		}
		if pod.UID == "" || pod.Spec.NodeName != req.NodeId {
			return nil, cephRetentionUnavailable("current Pod demand is ambiguous or cross-node", nil)
		}
		authority.ConsumerUIDs = append(authority.ConsumerUIDs, string(pod.UID))
	}
	if len(authority.ConsumerUIDs) == 0 {
		return nil, cephRetentionUnavailable("current same-node Pod demand disappeared", nil)
	}
	if pv.Spec.AccessModes[0] == corev1.ReadWriteOncePod && len(authority.ConsumerUIDs) != 1 {
		return nil, cephRetentionUnavailable("RWOP volume has multiple consumers", nil)
	}
	sort.Strings(authority.ConsumerUIDs)
	currentNode, err := runtime.NodeIdentity(ctx, req.NodeId)
	if err != nil || !currentNode.Exists || !currentNode.Ready || currentNode.Deleting || currentNode.UID != identity.UID {
		return nil, cephRetentionUnavailable("node identity or readiness changed during retention checks", err)
	}
	if err := s.recheckDetachVolumeAttachments(ctx, pv.Name, "", attachments); err != nil {
		return nil, err
	}
	return authority, nil
}

func (s *ControllerServer) attachmentProtectionChecks(ctx context.Context, volumeID string, runtimeCtx *VolumeRuntimeContext, target opennebula.HostArtifactAttachmentTarget) error {
	if s.driver.volumeRepairState == nil || s.driver.volumeQuarantine == nil || s.driver.hostArtifactQuarantine == nil {
		return attachmentEvidenceUnavailable("protection state managers are unavailable", nil)
	}
	repair, active, err := s.driver.volumeRepairState.GetCurrent(ctx, volumeID)
	if err != nil {
		return attachmentEvidenceUnavailable("cannot verify current volume repair state", err)
	}
	if active {
		const message = "volume repair is required before attachment changes"
		s.recordPVCWarningFromRuntimeContext(ctx, runtimeCtx, eventReasonVolumeRepairRequired, message)
		s.driver.metrics.RecordVolumeRepairState(repair.Classification, "active")
		return status.Error(codes.FailedPrecondition, message)
	}
	quarantine := s.driver.volumeQuarantine
	volumeCM, err := quarantine.runtime.GetConfigMap(ctx, quarantine.namespace, volumeQuarantineStateConfigMapName)
	if err != nil && !apierrors.IsNotFound(err) {
		return attachmentEvidenceUnavailable("volume quarantine lookup failed", err)
	}
	var rawVolumeQuarantine string
	var volumeQuarantinePresent bool
	if err == nil {
		rawVolumeQuarantine, volumeQuarantinePresent = volumeCM.Data[volumeID]
	}
	if volumeQuarantinePresent {
		var state VolumeQuarantineState
		if decodeErr := decodeAttachmentQuarantine(rawVolumeQuarantine, &state); decodeErr != nil {
			return attachmentEvidenceUnavailable("volume quarantine is malformed", decodeErr)
		}
		if state.VolumeID != volumeID || (state.Reason == "" && state.Classification == "") || !validQuarantineObservation(state.FailureCount, state.FirstObservedAt, state.LastObservedAt, state.ExpiresAt) {
			return attachmentEvidenceUnavailable("volume quarantine identity or classification is unknown", nil)
		}
		if state.ExpiresAt.After(time.Now().UTC()) {
			return status.Error(codes.FailedPrecondition, "attachment change is blocked by volume quarantine")
		}
	}
	// RefreshVolume's legacy decoder skips malformed entries. Read and validate
	// this safety boundary directly so corrupt or unreadable state cannot permit ACK.
	manager := s.driver.hostArtifactQuarantine
	cm, err := manager.runtime.GetConfigMap(ctx, manager.namespace, hostArtifactStateConfigMapName)
	if err != nil && !apierrors.IsNotFound(err) {
		return attachmentEvidenceUnavailable("host artifact quarantine lookup failed", err)
	}
	targetKeys := hostArtifactTargetQuarantineKeys(target)
	if err == nil {
		for key, raw := range cm.Data {
			var state HostArtifactQuarantineState
			if decodeErr := decodeAttachmentQuarantine(raw, &state); decodeErr != nil {
				return attachmentEvidenceUnavailable("host artifact quarantine is malformed", decodeErr)
			}
			recordTarget := opennebula.HostArtifactAttachmentTarget{VMID: state.VMID, NodeName: state.NodeName, DiskID: state.DiskID}
			expectedKey := firstNonEmpty(recordTarget.Key(), state.VolumeID)
			if state.Key != key || state.Key != expectedKey || strings.TrimSpace(state.VolumeID) == "" ||
				!validQuarantineObservation(state.FailureCount, state.FirstObservedAt, state.LastObservedAt, state.ExpiresAt) {
				return attachmentEvidenceUnavailable("host artifact quarantine identity or expiry is unknown", nil)
			}
			if state.ImageID > 0 && target.ImageID > 0 && (state.VolumeID == volumeID) != (state.ImageID == target.ImageID) {
				return attachmentEvidenceUnavailable("host artifact quarantine contradicts volume ownership", nil)
			}
			if state.ExpiresAt.After(time.Now().UTC()) && (state.VolumeID == volumeID || slices.Contains(targetKeys, key)) {
				return status.Error(codes.FailedPrecondition, "attachment change is blocked by host artifact quarantine")
			}
		}
	}
	return nil
}

func attachmentEvidenceUnavailable(message string, err error) error {
	if err != nil {
		klog.V(2).InfoS("Attachment authority read failed", "check", message, "err", err)
	}
	return status.Error(codes.Unavailable, message)
}

func decodeAttachmentQuarantine(raw string, state any) error {
	if err := decodeCephRetentionProtection(raw, state); err != nil {
		return err
	}
	var fields struct {
		ExpiresAt *time.Time `json:"expiresAt"`
	}
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return err
	}
	if fields.ExpiresAt == nil {
		return fmt.Errorf("quarantine expiry is missing")
	}
	return nil
}

func validQuarantineObservation(count int, first, last, expiry time.Time) bool {
	return count > 0 && !first.IsZero() && !last.IsZero() && !last.Before(first) && (expiry.IsZero() || expiry.After(last))
}

// Unknown future fields are not evidence of an expired protection record.
func decodeCephRetentionProtection(raw string, target any) error {
	if strings.TrimSpace(raw) == "null" {
		return fmt.Errorf("protection record must be an object")
	}
	if err := requireUniqueAuthorityKeys(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return err
		}
		return fmt.Errorf("protection record contains trailing JSON")
	}
	return nil
}

func cephHistoryHasObservedAuthority(history VolumeHistoryRecord) bool {
	return history.Version == stateObjectVersion && isCephBackend(history.Backend) &&
		volumeHistoryHasObservedSuccess(history) && !history.Bootstrapped &&
		(history.EvidenceSource == "" || history.EvidenceSource == volumeHistoryEvidenceSourceObserved)
}

var errAttachmentBackendUndeclared = errors.New("persistent volume backend is undeclared")

func verifiedAttachmentBackend(annotation, attribute string) (string, error) {
	annotation = strings.ToLower(strings.TrimSpace(annotation))
	attribute = strings.ToLower(strings.TrimSpace(attribute))
	if annotation != "" && attribute != "" && annotation != attribute {
		return "", fmt.Errorf("persistent volume backend assertions disagree")
	}
	backend := firstNonEmpty(annotation, attribute)
	switch backend {
	case "":
		return "", errAttachmentBackendUndeclared
	case "ceph", "cephfs", "local":
		return backend, nil
	default:
		return "", fmt.Errorf("persistent volume backend is unknown")
	}
}

func persistentVolumeBackend(pv *corev1.PersistentVolume) (string, error) {
	var attribute string
	if pv.Spec.CSI != nil {
		attribute = pv.Spec.CSI.VolumeAttributes[annotationBackend]
	}
	return verifiedAttachmentBackend(pv.Annotations[annotationBackend], attribute)
}

func (runtimeCtx *VolumeRuntimeContext) verifiedAttachmentBackend() (string, error) {
	if runtimeCtx == nil {
		return "", fmt.Errorf("volume runtime backend is unavailable")
	}
	return verifiedAttachmentBackend(runtimeCtx.Backend, runtimeCtx.backendAttribute)
}

func isCephBackend(backend string) bool {
	return strings.EqualFold(strings.TrimSpace(backend), "ceph")
}

func validateAttachmentImageAssertions(metadata map[string]string, imageID int) error {
	if raw, present := metadata[publishContextOpenNebulaImageID]; present {
		observed, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || observed <= 0 || observed != imageID {
			return fmt.Errorf("VolumeAttachment image identity does not match")
		}
	}
	if raw, present := metadata[publishContextDeviceSerial]; present && strings.TrimSpace(raw) != fmt.Sprintf("onecsi-%d", imageID) {
		return fmt.Errorf("VolumeAttachment device serial does not match the image")
	}
	return nil
}

func (s *ControllerServer) readAttachmentHistory(ctx context.Context, volumeID string) (VolumeHistoryRecord, error) {
	manager := s.driver.volumeHistory
	if manager == nil {
		return VolumeHistoryRecord{}, fmt.Errorf("volume history manager is unavailable")
	}
	cm, err := manager.runtime.GetConfigMap(ctx, manager.namespace, volumeHistoryStateConfigMapName)
	if apierrors.IsNotFound(err) {
		return VolumeHistoryRecord{}, nil
	}
	if err != nil {
		return VolumeHistoryRecord{}, err
	}
	raw, present := cm.Data[volumeID]
	if !present {
		return VolumeHistoryRecord{}, nil
	}
	return decodeAttachmentHistory(raw, volumeID)
}

func decodeAttachmentHistory(raw, volumeID string) (VolumeHistoryRecord, error) {
	var history VolumeHistoryRecord
	if err := decodeCephRetentionProtection(raw, &history); err != nil {
		return VolumeHistoryRecord{}, err
	}
	if history.VolumeID != volumeID || (history.Version != stateObjectVersion && (history.Version != 0 || isCephBackend(history.Backend))) ||
		history.LastSuccessfulImageID < 0 || history.LastSuccessfulOpenNebulaVMID < 0 || history.LastSuccessfulDiskID < 0 {
		return VolumeHistoryRecord{}, fmt.Errorf("volume history identity or schema is invalid")
	}
	return history, nil
}
