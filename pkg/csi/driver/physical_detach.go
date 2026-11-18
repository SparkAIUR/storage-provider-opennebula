package driver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

var errReplacementDetachPending = errors.New("replacement attachment requires verified absence and retirement")

type physicalDetachIdentity struct {
	pvUID, nodeUID        types.UID
	imageID, vmID, diskID int
	background, ceph      bool
	backendInferred       bool
	cephSingleWriter      bool
	backend               string
	attachments           map[string]detachAttachmentAuthority
	replacement           *replacementAttachAttempt
	replacementDisk       opennebula.VolumeDiskRecord
	replacementRetired    bool
}

type detachAttachmentAuthority struct {
	uid            types.UID
	created        time.Time
	attacher, node string
	deleting       bool
	attached       bool
	metadata       map[string]string
}

func (s *ControllerServer) detachVolumeAttachments(ctx context.Context, pvName, nodeName string) (map[string]detachAttachmentAuthority, error) {
	vas, err := s.driver.kubeRuntime.client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, attachmentEvidenceUnavailable("cannot verify attachment deletion authority", err)
	}
	attachments := make(map[string]detachAttachmentAuthority)
	for _, va := range vas.Items {
		if matchesVolumeAttachment(pvName, &va) && (nodeName == "" || strings.TrimSpace(va.Spec.NodeName) == nodeName) {
			attachments[va.Name] = detachAttachmentAuthority{
				uid: va.UID, created: va.CreationTimestamp.Time, attacher: va.Spec.Attacher, node: va.Spec.NodeName,
				deleting: !va.DeletionTimestamp.IsZero(), attached: va.Status.Attached,
				metadata: cloneStringMap(va.Status.AttachmentMetadata),
			}
		}
	}
	return attachments, nil
}

func (s *ControllerServer) recheckDetachVolumeAttachments(ctx context.Context, pvName, nodeName string, expected map[string]detachAttachmentAuthority) error {
	current, err := s.detachVolumeAttachments(ctx, pvName, nodeName)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, current) {
		return attachmentEvidenceUnavailable("VolumeAttachment deletion authority changed during attachment checks", nil)
	}
	return status.FromContextError(ctx.Err()).Err()
}

func (s *ControllerServer) checkPhysicalDetachAuthority(ctx context.Context, volume, nodeName string, identity *physicalDetachIdentity, readback bool) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
		return nil
	}
	runtime := s.driver.kubeRuntime
	runtimeCtx, err := runtime.ResolveVolumeRuntimeContext(ctx, volume)
	if err != nil {
		return attachmentEvidenceUnavailable("cannot verify detach binding", err)
	}
	pv, err := runtime.client.CoreV1().PersistentVolumes().Get(ctx, runtimeCtx.PVName, metav1.GetOptions{})
	if err != nil || pv == nil || pv.UID != identity.pvUID || pv.Spec.CSI == nil || pv.Spec.CSI.Driver != s.driver.name || pv.Spec.CSI.VolumeHandle != volume {
		return attachmentEvidenceUnavailable("cannot verify owned PV identity for detach", err)
	}
	runtimeCtx.PVAnnotations = pv.Annotations
	runtimeCtx.Backend = strings.TrimSpace(pv.Annotations[annotationBackend])
	backend, err := persistentVolumeBackend(pv)
	if errors.Is(err, errAttachmentBackendUndeclared) {
		switch {
		case identity.attachments == nil:
			datastore, lookupErr := s.inspectVolumeDatastore(ctx, volume, identity.imageID)
			if lookupErr != nil {
				return lookupErr
			}
			backend = datastore.Backend
			identity.backendInferred = true
		case identity.backendInferred:
			backend = identity.backend
		default:
			return attachmentEvidenceUnavailable("detach backend declaration disappeared", nil)
		}
	} else if err != nil {
		return attachmentEvidenceUnavailable("cannot verify detach backend", err)
	}
	if identity.ceph && !isCephBackend(backend) {
		return status.Error(codes.Unavailable, "Ceph backend changed before detach")
	}
	if identity.backend != "" && identity.backend != backend {
		return attachmentEvidenceUnavailable("detach backend changed", nil)
	}
	if identity.background && identity.cephSingleWriter != (isCephBackend(backend) && hasSingleWriterAccessMode(pv.Spec.AccessModes)) {
		return attachmentEvidenceUnavailable("background detach scope changed", nil)
	}
	history, err := s.readAttachmentHistory(ctx, volume)
	if err != nil {
		return attachmentEvidenceUnavailable("detach history is unreadable", err)
	}
	attempt := history.PendingReplacementAttach
	if identity.replacementRetired {
		if attempt != nil {
			return attachmentEvidenceUnavailable("replacement detach attempt changed", nil)
		}
		attempt = identity.replacement
	}
	replacementDetach := attempt != nil
	if identity.replacement != nil && (!replacementDetach || *identity.replacement != *attempt) {
		return attachmentEvidenceUnavailable("replacement detach attempt changed", nil)
	}
	attachmentNode := nodeName
	if replacementDetach {
		attachmentNode = ""
	}
	attachments, err := s.detachVolumeAttachments(ctx, pv.Name, attachmentNode)
	if err != nil {
		return err
	}
	if identity.attachments != nil && !reflect.DeepEqual(identity.attachments, attachments) {
		return attachmentEvidenceUnavailable("VolumeAttachment deletion authority changed before detach attempt", nil)
	}
	if replacementDetach && (len(attachments) > 1 || (readback && identity.background && identity.replacementDisk.NodeID == 0 && len(attachments) != 0)) {
		return attachmentEvidenceUnavailable("replacement detach has conflicting VolumeAttachments", nil)
	}
	for _, va := range attachments {
		if va.attacher != s.driver.name || va.uid == "" || !va.deleting || (replacementDetach && va.node != nodeName) {
			return status.Error(codes.Unavailable, "current VolumeAttachment does not authorize physical detach")
		}
		if err := validateAttachmentImageAssertions(va.metadata, identity.imageID); err != nil {
			return status.Error(codes.Unavailable, err.Error())
		}
	}
	pvc, replacement, err := runtime.attachmentClaim(ctx, pv)
	if err != nil {
		return attachmentEvidenceUnavailable("detach claim identity is unverified", err)
	}
	runtimeCtx.PVCAnnotations = nil
	if pvc == nil || replacement {
		runtimeCtx.PVCNamespace = ""
		runtimeCtx.PVCName = ""
		runtimeCtx.PVCUID = ""
	} else {
		runtimeCtx.PVCName = pvc.Name
		runtimeCtx.PVCNamespace = pvc.Namespace
		runtimeCtx.PVCUID = pvc.UID
		runtimeCtx.PVCAnnotations = pvc.Annotations
	}

	if replacementDetach {
		phaseEligible := attempt.dispatchPossible()
		if readback {
			phaseEligible = attempt.prepared() || attempt.detached() || (identity.replacementRetired && attempt.dispatchPossible())
		}
		claim := pv.Spec.ClaimRef
		bound := pv.Status.Phase == corev1.VolumeBound && pvc != nil && pvc.Status.Phase == corev1.ClaimBound
		released := pv.Status.Phase == corev1.VolumeReleased
		if !phaseEligible || !attempt.matchesHistory(history) || attempt.Driver != s.driver.name ||
			attempt.Node != nodeName || attempt.ImageID != identity.imageID || attempt.VMID != identity.vmID ||
			attempt.ReplacementUID != string(identity.nodeUID) || attempt.PVUID != string(pv.UID) ||
			pv.Name != history.PVName || (!bound && !released) || !cephRetentionAccessMode(pv.Spec.AccessModes) ||
			!isCephBackend(backend) || replacement || claim == nil || attempt.PVCUID != string(claim.UID) ||
			claim.Namespace != history.PVCNamespace || claim.Name != history.PVCName ||
			(pvc != nil && !slices.Equal(pv.Spec.AccessModes, pvc.Spec.AccessModes)) {
			return attachmentEvidenceUnavailable("replacement detach binding or identity is unverified", nil)
		}
		va, pinned := attachments[attempt.VAName]
		if len(attachments) != 0 || attempt.detached() {
			if !pinned || string(va.uid) != attempt.VAUID {
				return attachmentEvidenceUnavailable("replacement detach attachment identity changed", nil)
			}
		} else {
			current, err := runtime.client.StorageV1().VolumeAttachments().Get(ctx, attempt.VAName, metav1.GetOptions{})
			if !apierrors.IsNotFound(err) && (err != nil || current.UID == "" || string(current.UID) == attempt.VAUID) {
				return attachmentEvidenceUnavailable("pinned replacement VolumeAttachment absence is unverified", err)
			}
		}
		if attempt.detached() {
			completion := attempt.DetachCompletion
			if target, present := va.metadata["volumeName"]; present && strings.TrimPrefix(strings.TrimSpace(target), "/dev/") != completion.Target {
				return attachmentEvidenceUnavailable("completed replacement detach target changed", nil)
			}
			identity.diskID, _ = strconv.Atoi(completion.DiskID)
		}
		disk, err := s.inspectReplacementDisk(ctx, *attempt, !readback)
		if err != nil {
			return err
		}
		if disk != nil {
			for _, va := range attachments {
				if target, present := va.metadata["volumeName"]; present && strings.TrimPrefix(strings.TrimSpace(target), "/dev/") != disk.Target {
					return attachmentEvidenceUnavailable("replacement detach VolumeAttachment target changed", nil)
				}
			}
			if (identity.diskID >= 0 && identity.diskID != disk.DiskID) ||
				(identity.replacement != nil && identity.replacementDisk != *disk) {
				return attachmentEvidenceUnavailable("replacement detach disk changed", nil)
			}
			identity.diskID = disk.DiskID
			identity.replacementDisk = *disk
		}
		identity.replacement = attempt
	}
	if history.Backend != "" && (identity.backendInferred || isCephBackend(history.Backend) || isCephBackend(backend)) && !strings.EqualFold(history.Backend, backend) {
		return status.Error(codes.Unavailable, "detach backend contradicts observed history")
	}
	if history.LastSuccessfulImageID > 0 && history.LastSuccessfulImageID != identity.imageID {
		return status.Error(codes.Unavailable, "detach image differs from observed history")
	}
	if !replacementDetach && history.LastSuccessfulNodeName == nodeName && history.LastSuccessfulOpenNebulaVMID > 0 && history.LastSuccessfulOpenNebulaVMID != identity.vmID {
		return status.Error(codes.Unavailable, "detach VM differs from observed history")
	}
	if !replacementDetach && history.LastSuccessfulNodeName == nodeName && history.LastSuccessfulDiskID != nil && identity.diskID >= 0 && *history.LastSuccessfulDiskID != identity.diskID {
		return status.Error(codes.Unavailable, "detach disk slot differs from observed history")
	}
	control := resolvedRecoveryControlState(volume, runtimeCtx, VolumeRecoveryControlState{})
	if control.Invalid || control.Active() {
		return status.Error(codes.FailedPrecondition, "physical detach is blocked by recovery controls")
	}
	target := opennebula.HostArtifactAttachmentTarget{
		NodeName: nodeName, VMID: identity.vmID, ImageID: identity.imageID, DiskID: identity.diskID,
	}
	if err := s.attachmentProtectionChecks(ctx, volume, runtimeCtx, target); err != nil {
		return err
	}
	desired, err := runtime.DesiredNodeForVolume(ctx, volume)
	if err != nil {
		return attachmentEvidenceUnavailable("current volume release is unverified", err)
	}
	protection, err := s.localRWOProtectionProjection(ctx, volume, nodeName, runtimeCtx, false, true)
	if err != nil || protection.Invalid {
		return attachmentEvidenceUnavailable("detach protection is unverified", err)
	}
	orphan, _ := s.orphanTeardownEligible(ctx, volume, nodeName, runtimeCtx, desired, nil)
	override, _ := crossNodeOverrideDeadline(runtimeCtx)
	if protection.Protected && protection.RequiredNode == nodeName && !bypassProtectionForOrphanTeardown(protection, orphan) && !override.After(time.Now().UTC()) {
		return status.Error(codes.Unavailable, "physical detach is blocked by local volume protection")
	}
	if err := s.recheckAttachmentAuthority(ctx, volume, runtimeCtx, target, pv, pvc, replacement); err != nil {
		return err
	}
	receipt := readback && replacementDetach && attempt.detached()
	terminalOnNode := false
	if claim := pv.Spec.ClaimRef; claim != nil && !receipt {
		pods, err := runtime.client.CoreV1().Pods(claim.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return attachmentEvidenceUnavailable("cannot verify Pod release", err)
		}
		remoteNode := ""
		for _, pod := range pods.Items {
			referencesPVC, err := podMayUseAttachmentClaim(&pod, claim, pvc, replacement)
			if err != nil {
				return attachmentEvidenceUnavailable("cannot verify Pod claim ownership", err)
			}
			if !referencesPVC {
				continue
			}
			if terminalPodMayBeReleased(&pod) {
				terminalOnNode = terminalOnNode || pod.Spec.NodeName == nodeName
				continue
			}
			if replacementDetach || pod.Spec.NodeName == "" || pod.Spec.NodeName == nodeName {
				return attachmentEvidenceUnavailable("a current Pod still references the volume on the detach node", nil)
			}
			if identity.background {
				if !identity.cephSingleWriter || !cephHistoryHasObservedAuthority(history) || (remoteNode != "" && remoteNode != pod.Spec.NodeName) {
					return attachmentEvidenceUnavailable("background detach is blocked by remote Pod demand", nil)
				}
				remoteNode = pod.Spec.NodeName
			}
		}
	}
	if !receipt && (desired.desiresNode(nodeName) || (replacementDetach && (desired.Desired || desired.MultipleNodes))) {
		return attachmentEvidenceUnavailable("current volume release is unverified", nil)
	}
	if identity.replacementRetired {
		current, err := s.readAttachmentHistory(ctx, volume)
		if err != nil || current.PendingReplacementAttach != nil || !attempt.matchesHistory(current) {
			return attachmentEvidenceUnavailable("replacement detach retirement is unverified", err)
		}
	} else if replacementDetach {
		if err := s.checkReplacementAttemptReadback(ctx, *attempt); err != nil {
			return err
		}
	}
	node, err := runtime.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil || node == nil || node.UID != identity.nodeUID || node.DeletionTimestamp != nil {
		return attachmentEvidenceUnavailable("detach node identity is unverified", err)
	}
	if !replacementDetach && history.LastSuccessfulNodeName == nodeName && history.LastSuccessfulNodeUID != "" && history.LastSuccessfulNodeUID != string(node.UID) {
		return status.Error(codes.Unavailable, "detach node identity differs from observed history")
	}
	ready := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			ready = condition.Status == corev1.ConditionTrue
		}
	}
	if !ready && (identity.background || isCephBackend(backend) || terminalOnNode) {
		return status.Error(codes.Unavailable, "detach node release observation is unavailable")
	}
	waiting := false
	if receipt || identity.replacementRetired {
		if err := s.checkReplacementMaintenance(ctx); err != nil {
			return err
		}
	}
	if receipt {
		waiting, err = s.replacementWaitingConsumers(ctx, pv, pvc, node)
		if err != nil {
			return err
		}
		if waiting && (pvc == nil || pv.Status.Phase != corev1.VolumeBound || pvc.DeletionTimestamp != nil) {
			return attachmentEvidenceUnavailable("replacement detach binding is unverified", nil)
		}
	}
	if !waiting && nodeReportsVolumeInUse(node, s.driver.name, volume) {
		return status.Error(codes.Unavailable, "detach node still reports the volume in use")
	}
	if err := s.recheckDetachVolumeAttachments(ctx, pv.Name, attachmentNode, attachments); err != nil {
		return err
	}
	identity.attachments = attachments
	identity.backend = backend
	return nil
}

func terminalPodMayBeReleased(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp == nil && pod.Spec.NodeName != "" &&
		(pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed)
}

func (s *ControllerServer) recheckAttachmentAuthority(ctx context.Context, volume string, runtimeCtx *VolumeRuntimeContext, target opennebula.HostArtifactAttachmentTarget, pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim, replacement bool) error {
	if err := s.attachmentProtectionChecks(ctx, volume, runtimeCtx, target); err != nil {
		return err
	}
	client := s.driver.kubeRuntime.client.CoreV1()
	currentPV, err := client.PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
	if err != nil || currentPV.UID != pv.UID || !reflect.DeepEqual(currentPV.Spec, pv.Spec) ||
		!reflect.DeepEqual(currentPV.Annotations, pv.Annotations) || currentPV.Status.Phase != pv.Status.Phase ||
		!reflect.DeepEqual(currentPV.DeletionTimestamp, pv.DeletionTimestamp) {
		return status.Error(codes.Unavailable, "volume binding or recovery controls changed during attachment checks")
	}
	currentPVC, currentReplacement, err := s.driver.kubeRuntime.attachmentClaim(ctx, currentPV)
	if err != nil || replacement != currentReplacement || (pvc == nil) != (currentPVC == nil) ||
		(pvc != nil && (currentPVC.UID != pvc.UID || !currentPVC.CreationTimestamp.Equal(&pvc.CreationTimestamp) || !reflect.DeepEqual(currentPVC.Spec, pvc.Spec) ||
			!reflect.DeepEqual(currentPVC.Status, pvc.Status) || !reflect.DeepEqual(currentPVC.Annotations, pvc.Annotations) ||
			!reflect.DeepEqual(metav1.GetControllerOfNoCopy(currentPVC), metav1.GetControllerOfNoCopy(pvc)) ||
			!reflect.DeepEqual(currentPVC.DeletionTimestamp, pvc.DeletionTimestamp))) {
		return status.Error(codes.Unavailable, "volume claim or recovery controls changed during attachment checks")
	}

	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

func (s *ControllerServer) inspectVolumeDatastore(ctx context.Context, volume string, imageID int) (opennebula.Datastore, error) {
	inspector, ok := s.volumeProvider.(opennebula.VolumeDatastoreInspector)
	if !ok {
		return opennebula.Datastore{}, attachmentEvidenceUnavailable("provider cannot verify the volume datastore", nil)
	}
	datastore, err := inspector.InspectVolumeDatastore(ctx, volume, imageID)
	if err != nil {
		return opennebula.Datastore{}, attachmentEvidenceUnavailable("volume datastore is unverified", err)
	}
	backend, err := verifiedAttachmentBackend("", datastore.Backend)
	if err != nil || backend == "cephfs" {
		return opennebula.Datastore{}, attachmentEvidenceUnavailable("volume datastore backend is unverified for a disk image", err)
	}
	datastore.Backend = backend
	return datastore, nil
}

func (s *ControllerServer) verifyProviderAttachmentBackend(ctx context.Context, volume string, imageID, vmID int, backend string) error {
	if backend == "" {
		return nil
	}
	provider, ok := s.volumeProvider.(opennebula.AttachmentBackendVerifier)
	if !ok {
		return attachmentEvidenceUnavailable("provider cannot verify the attachment backend", nil)
	}
	if err := provider.VerifyAttachmentBackend(ctx, volume, imageID, vmID, backend); err != nil {
		return attachmentEvidenceUnavailable("provider attachment backend is unverified", err)
	}
	return nil
}

func (s *ControllerServer) verifyDetachAbsenceTarget(ctx context.Context, volume, node string, imageID, vmID int, nodeUID types.UID) error {
	return s.checkDetachAbsenceTarget(ctx, volume, node, imageID, vmID, nodeUID, false)
}

func (s *ControllerServer) checkDetachAbsenceTarget(ctx context.Context, volume, node string, imageID, vmID int, nodeUID types.UID, retiredRequired bool) error {
	runtime := s.driver.kubeRuntime
	if runtime != nil && runtime.enabled {
		history, err := s.readAttachmentHistory(ctx, volume)
		if err != nil {
			return attachmentEvidenceUnavailable("cannot verify detach absence history", err)
		}
		if attempt := history.PendingReplacementAttach; attempt != nil {
			if attempt.Node == node && (attempt.prepared() || attempt.detached()) && attempt.matchesHistory(history) {
				return errReplacementDetachPending
			}
			return attachmentEvidenceUnavailable("replacement attachment release is unresolved", nil)
		}
		if history.LastSuccessfulImageID > 0 && history.LastSuccessfulImageID != imageID {
			return attachmentEvidenceUnavailable("detach absence image differs from history", nil)
		}
		if history.LastSuccessfulNodeName == node && history.LastSuccessfulOpenNebulaVMID > 0 && history.LastSuccessfulOpenNebulaVMID != vmID {
			return attachmentEvidenceUnavailable("detach absence VM differs from history", nil)
		}
		retired, err := s.detachAbsenceNodeRetired(ctx, node, history, nodeUID)
		if err != nil {
			return err
		}
		if retired {
			return s.verifyRetiredNodeAbsence(ctx, volume, node, imageID, vmID, history)
		}
	}
	if retiredRequired {
		return attachmentEvidenceUnavailable("retired node identity is unverified", nil)
	}
	currentVM, err := s.volumeProvider.NodeExists(ctx, node)
	if err != nil || vmID <= 0 || currentVM != vmID {
		return attachmentEvidenceUnavailable("detach absence VM identity changed", err)
	}
	currentImage, _, err := s.volumeProvider.VolumeExists(ctx, volume)
	if err != nil || imageID < 0 || currentImage != imageID {
		return attachmentEvidenceUnavailable("detach absence image identity is unverified", err)
	}
	return status.FromContextError(ctx.Err()).Err()
}

func (s *ControllerServer) verifyRetiredNodeAbsence(ctx context.Context, volume, node string, imageID, vmID int, history VolumeHistoryRecord) error {
	if history.LastSuccessfulNodeName != node || history.LastSuccessfulNodeUID == "" || imageID <= 0 ||
		history.LastSuccessfulImageID != imageID || vmID <= 0 || history.LastSuccessfulOpenNebulaVMID != vmID {
		return attachmentEvidenceUnavailable("retired node lacks pinned attachment identity", nil)
	}
	currentVM, err := s.volumeProvider.NodeExists(ctx, node)
	var missing *opennebula.VMNotFoundError
	if (err != nil && !errors.As(err, &missing)) || (err == nil && currentVM != vmID) {
		return attachmentEvidenceUnavailable("retired node VM identity changed", err)
	}
	verifier, ok := s.volumeProvider.(opennebula.VMVolumeAbsenceVerifier)
	if !ok {
		return attachmentEvidenceUnavailable("provider cannot verify retired node disk absence", nil)
	}
	if err := verifier.VerifyVolumeAbsentFromVM(ctx, volume, imageID, vmID); err != nil {
		return attachmentEvidenceUnavailable("retired node disk absence is unverified", err)
	}
	current, err := s.driver.kubeRuntime.NodeIdentity(ctx, node)
	if err != nil || current.Exists {
		return attachmentEvidenceUnavailable("retired node identity changed", err)
	}
	return status.FromContextError(ctx.Err()).Err()
}

func (s *ControllerServer) detachAbsenceNodeRetired(ctx context.Context, node string, history VolumeHistoryRecord, nodeUID types.UID) (bool, error) {
	if history.LastSuccessfulNodeName == node && history.LastSuccessfulNodeUID != "" {
		if nodeUID != "" && string(nodeUID) != history.LastSuccessfulNodeUID {
			return false, attachmentEvidenceUnavailable("detach absence node differs from history", nil)
		}
		nodeUID = types.UID(history.LastSuccessfulNodeUID)
	}
	if runtime := s.driver.kubeRuntime; runtime != nil && runtime.enabled && nodeUID != "" {
		current, err := runtime.NodeIdentity(ctx, node)
		if err != nil || (current.Exists && (current.Deleting || current.UID != string(nodeUID))) {
			return false, attachmentEvidenceUnavailable("detach absence node identity changed", err)
		}
		return !current.Exists, status.FromContextError(ctx.Err()).Err()
	}
	return false, status.FromContextError(ctx.Err()).Err()
}

func (s *ControllerServer) verifySoleDetachOwner(ctx context.Context, volume, node string, imageID, vmID, diskID int) (string, error) {
	inspector, ok := s.volumeProvider.(opennebula.VolumeAttachmentInspector)
	if !ok {
		return "", attachmentEvidenceUnavailable("cannot verify sole Ceph attachment for cleanup", nil)
	}
	metadata, err := inspector.InspectVolumeAttachment(ctx, volume, node)
	if err != nil || metadata == nil || metadata.ImageID != imageID || metadata.ImageName != volume ||
		metadata.RequestedNodeID != vmID || !metadata.AttachedToRequestedNode ||
		metadata.ImageRunningVMs != 1 || len(metadata.ImageVMIDs) != 1 || metadata.ImageVMIDs[0] != vmID ||
		len(metadata.DiskRecords) != 1 || metadata.DiskRecords[0].NodeID != vmID || metadata.DiskRecords[0].DiskID != diskID || strings.TrimSpace(metadata.DiskRecords[0].Target) == "" {
		return "", attachmentEvidenceUnavailable("cannot verify sole Ceph attachment for cleanup", err)
	}
	return strings.TrimSpace(metadata.DiskRecords[0].Target), nil
}

func (s *ControllerServer) resolvePhysicalDetachIdentity(ctx context.Context, volume, node string, identity *physicalDetachIdentity) error {
	runtime := s.driver.kubeRuntime
	if runtime != nil && runtime.enabled {
		if identity.pvUID == "" {
			runtimeCtx, err := runtime.ResolveVolumeRuntimeContext(ctx, volume)
			if err != nil {
				return attachmentEvidenceUnavailable("cannot verify detach binding", err)
			}
			pv, err := runtime.client.CoreV1().PersistentVolumes().Get(ctx, runtimeCtx.PVName, metav1.GetOptions{})
			if err != nil || pv.UID == "" {
				return attachmentEvidenceUnavailable("cannot verify detach PV UID", err)
			}
			identity.pvUID = pv.UID
		}
		current, err := runtime.NodeIdentity(ctx, node)
		if err != nil || !current.Exists || current.UID == "" || (identity.nodeUID != "" && identity.nodeUID != types.UID(current.UID)) {
			return attachmentEvidenceUnavailable("cannot verify detach node UID", err)
		}
		identity.nodeUID = types.UID(current.UID)
	}
	return nil
}

func (s *ControllerServer) finishVerifiedReplacementAbsence(ctx context.Context, volume, node string, identity physicalDetachIdentity) (bool, error) {
	if s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
		return false, nil
	}
	history, err := s.readAttachmentHistory(ctx, volume)
	if err != nil {
		return true, attachmentEvidenceUnavailable("detach history is unreadable", err)
	}
	attempt := history.PendingReplacementAttach
	if attempt == nil {
		return s.checkReplacedNodeStaleAttachment(ctx, volume, node, identity.imageID, identity.vmID, history, nil)
	}
	if attempt.Node != node || (!attempt.prepared() && !attempt.detached()) || !attempt.matchesHistory(history) {
		return true, attachmentEvidenceUnavailable("replacement attachment release is unresolved", nil)
	}
	if err := s.resolvePhysicalDetachIdentity(ctx, volume, node, &identity); err != nil {
		return true, err
	}
	identity.replacement = attempt
	if err := s.checkPhysicalDetachAuthority(ctx, volume, node, &identity, true); err != nil {
		return true, err
	}
	if attempt.detached() {
		return true, nil
	}
	return true, s.retireVerifiedReplacement(ctx, *attempt)
}

func (s *ControllerServer) retireVerifiedReplacement(ctx context.Context, attempt replacementAttachAttempt) error {
	if err := s.retireReplacementAttach(ctx, attempt); err != nil {
		return attachmentEvidenceUnavailable("replacement detach retirement is unverified", err)
	}
	return nil
}

func (s *ControllerServer) completeVerifiedReplacementDetach(ctx context.Context, volume, node string, identity *physicalDetachIdentity) error {
	attempt := *identity.replacement
	completed := attempt
	completed.Phase = replacementAttachDetached
	for name, va := range identity.attachments {
		disk := identity.replacementDisk
		completed.DetachCompletion = replacementDetachCompletion{VAName: name, VAUID: string(va.uid), DiskID: strconv.Itoa(disk.DiskID), Target: disk.Target, Serial: disk.Serial}
	}
	verified := *identity
	if len(identity.attachments) == 0 {
		verified.replacementRetired = true
	} else {
		verified.replacement = &completed
	}
	err := retryOnConflict(retry.DefaultRetry, func() error {
		if _, err := s.inspectReplacementDisk(ctx, attempt, false); err != nil {
			return err
		}
		if verified.replacementRetired {
			writeErr := s.retireReplacementAttach(ctx, attempt)
			if writeErr == nil || apierrors.IsConflict(writeErr) {
				return writeErr
			}
			current, err := s.readAttachmentHistory(ctx, volume)
			if err != nil || current.PendingReplacementAttach != nil || !attempt.matchesHistory(current) {
				return attachmentEvidenceUnavailable("replacement detach retirement is unverified", errors.Join(writeErr, err))
			}
			return nil
		}
		_, writeErr := s.driver.volumeHistory.updateHistory(ctx, volume, true, func(current *VolumeHistoryRecord) error {
			if current.PendingReplacementAttach == nil || *current.PendingReplacementAttach != attempt || !attempt.matchesHistory(*current) {
				return fmt.Errorf("replacement attachment history changed before recording detach")
			}
			current.PendingReplacementAttach = &completed
			return nil
		})
		if apierrors.IsConflict(writeErr) {
			return writeErr
		}
		if err := s.checkReplacementAttemptReadback(ctx, completed); err != nil {
			return attachmentEvidenceUnavailable("replacement detach completion is unverified", errors.Join(writeErr, err))
		}
		return nil
	})
	if err != nil {
		if status.Code(err) == codes.Unknown {
			return attachmentEvidenceUnavailable("replacement detach completion is unverified", err)
		}
		return err
	}
	return s.checkPhysicalDetachAuthority(ctx, volume, node, &verified, true)
}

func (s *ControllerServer) detachVolumeWithAuthority(ctx context.Context, volume, node string, identity physicalDetachIdentity) (bool, error) {
	if err := s.resolvePhysicalDetachIdentity(ctx, volume, node, &identity); err != nil {
		return false, err
	}
	authorize := func(ctx context.Context, imageID, vmID, diskID int) error {
		if imageID != identity.imageID || vmID != identity.vmID || (identity.diskID >= 0 && diskID != identity.diskID) {
			return status.Error(codes.Unavailable, "provider attachment identity changed before detach")
		}
		attempt := identity
		attempt.diskID = diskID
		return s.checkPhysicalDetachAuthority(ctx, volume, node, &attempt, false)
	}
	if err := s.checkPhysicalDetachAuthority(ctx, volume, node, &identity, false); err != nil {
		if parentErr := ctx.Err(); parentErr != nil {
			return false, parentErr
		}
		return false, err
	}
	provider, ok := s.volumeProvider.(opennebula.AuthorizedVolumeDetacher)
	if !ok {
		return false, attachmentEvidenceUnavailable("provider cannot authorize physical detach", nil)
	}
	var verifyOwner opennebula.DetachOwnerCheck
	if identity.cephSingleWriter {
		verifyOwner = func(ctx context.Context, imageID, vmID, diskID int) (string, error) {
			return s.verifySoleDetachOwner(ctx, volume, node, imageID, vmID, diskID)
		}
	}
	accepted, err := provider.DetachVolumeWithAuthority(opennebula.WithExpectedImageID(ctx, identity.imageID), volume, node, identity.vmID, identity.backend, verifyOwner, authorize)
	if parentErr := ctx.Err(); parentErr != nil {
		return false, parentErr
	}
	if err != nil {
		return false, err
	}
	retiredRequired := false
	if identity.ceph || isCephBackend(identity.backend) || identity.background {
		if _, err := s.volumeProvider.GetVolumeInNode(ctx, identity.imageID, identity.vmID); !errors.Is(err, opennebula.ErrAttachmentNotFound) {
			if !opennebula.IsAuthoritativeVolumeAbsence(err) {
				return false, attachmentEvidenceUnavailable("detach readback did not establish absence", err)
			}
			retiredRequired = true
		}
	}
	if identity.replacement != nil {
		if !accepted || retiredRequired {
			return false, attachmentEvidenceUnavailable("replacement detach outcome is unverified", nil)
		}
		if err := s.completeVerifiedReplacementDetach(ctx, volume, node, &identity); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := s.checkDetachAbsenceTarget(ctx, volume, node, identity.imageID, identity.vmID, identity.nodeUID, retiredRequired); err != nil {
		return false, err
	}
	return accepted, nil
}
