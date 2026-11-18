package driver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type physicalDetachIdentity struct {
	pvUID, nodeUID        types.UID
	imageID, vmID, diskID int
	background, ceph      bool
	backendInferred       bool
	cephSingleWriter      bool
	backend               string
	attachments           map[string]detachAttachmentAuthority
}

type detachAttachmentAuthority struct {
	uid            types.UID
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
				uid: va.UID, attacher: va.Spec.Attacher, node: va.Spec.NodeName,
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

func (s *ControllerServer) checkPhysicalDetachAuthority(ctx context.Context, volume, nodeName string, identity *physicalDetachIdentity) error {
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
	attachments, err := s.detachVolumeAttachments(ctx, pv.Name, nodeName)
	if err != nil {
		return err
	}
	if identity.attachments != nil && !reflect.DeepEqual(identity.attachments, attachments) {
		return attachmentEvidenceUnavailable("VolumeAttachment deletion authority changed before detach attempt", nil)
	}
	for _, va := range attachments {
		if va.attacher != s.driver.name || va.uid == "" || !va.deleting {
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
	if pvc == nil {
		runtimeCtx.PVCNamespace = ""
		runtimeCtx.PVCName = ""
		runtimeCtx.PVCUID = ""
	} else {
		runtimeCtx.PVCName = pvc.Name
		runtimeCtx.PVCNamespace = pvc.Namespace
		runtimeCtx.PVCUID = pvc.UID
		runtimeCtx.PVCAnnotations = pvc.Annotations
	}

	history, err := s.readAttachmentHistory(ctx, volume)
	if err != nil {
		return attachmentEvidenceUnavailable("detach history is unreadable", err)
	}
	if history.Backend != "" && (identity.backendInferred || isCephBackend(history.Backend) || isCephBackend(backend)) && !strings.EqualFold(history.Backend, backend) {
		return status.Error(codes.Unavailable, "detach backend contradicts observed history")
	}
	if history.LastSuccessfulImageID > 0 && history.LastSuccessfulImageID != identity.imageID {
		return status.Error(codes.Unavailable, "detach image differs from observed history")
	}
	if history.LastSuccessfulNodeName == nodeName && history.LastSuccessfulOpenNebulaVMID > 0 && history.LastSuccessfulOpenNebulaVMID != identity.vmID {
		return status.Error(codes.Unavailable, "detach VM differs from observed history")
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
	terminalOnNode := false
	if claim := pv.Spec.ClaimRef; claim != nil && !replacement {
		pods, err := runtime.client.CoreV1().Pods(claim.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return attachmentEvidenceUnavailable("cannot verify Pod release", err)
		}
		remoteNode := ""
		for _, pod := range pods.Items {
			referencesPVC, err := podReferencesClaim(&pod, claim.Name, pvc)
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
			if pod.Spec.NodeName == "" || pod.Spec.NodeName == nodeName {
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
	if desired.desiresNode(nodeName) {
		return attachmentEvidenceUnavailable("current volume release is unverified", nil)
	}
	node, err := runtime.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil || node == nil || node.UID != identity.nodeUID || node.DeletionTimestamp != nil {
		return attachmentEvidenceUnavailable("detach node identity is unverified", err)
	}
	if history.LastSuccessfulNodeName == nodeName && history.LastSuccessfulNodeUID != "" && history.LastSuccessfulNodeUID != string(node.UID) {
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
	uniqueName := corev1.UniqueVolumeName(fmt.Sprintf("kubernetes.io/csi/%s^%s", s.driver.name, volume))
	for _, inUse := range node.Status.VolumesInUse {
		if inUse == uniqueName {
			return status.Error(codes.Unavailable, "detach node still reports the volume in use")
		}
	}
	if err := s.recheckDetachVolumeAttachments(ctx, pv.Name, nodeName, attachments); err != nil {
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
		(pvc != nil && (currentPVC.UID != pvc.UID || !reflect.DeepEqual(currentPVC.Spec, pvc.Spec) ||
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
	runtime := s.driver.kubeRuntime
	if runtime != nil && runtime.enabled {
		history, err := s.readAttachmentHistory(ctx, volume)
		if err != nil {
			return attachmentEvidenceUnavailable("cannot verify detach absence history", err)
		}
		if history.LastSuccessfulImageID > 0 && history.LastSuccessfulImageID != imageID {
			return attachmentEvidenceUnavailable("detach absence image differs from history", nil)
		}
		if history.LastSuccessfulNodeName == node {
			if history.LastSuccessfulOpenNebulaVMID > 0 && history.LastSuccessfulOpenNebulaVMID != vmID {
				return attachmentEvidenceUnavailable("detach absence VM differs from history", nil)
			}
			if history.LastSuccessfulNodeUID != "" {
				if nodeUID != "" && string(nodeUID) != history.LastSuccessfulNodeUID {
					return attachmentEvidenceUnavailable("detach absence node differs from history", nil)
				}
				nodeUID = types.UID(history.LastSuccessfulNodeUID)
			}
		}
		if nodeUID != "" {
			current, err := runtime.NodeIdentity(ctx, node)
			if err != nil || !current.Exists || current.Deleting || current.UID != string(nodeUID) {
				return attachmentEvidenceUnavailable("detach absence node identity changed", err)
			}
		}
	}
	currentVM, err := s.volumeProvider.NodeExists(ctx, node)
	if err != nil || vmID <= 0 || currentVM != vmID {
		return attachmentEvidenceUnavailable("detach absence VM identity changed", err)
	}
	currentImage, _, err := s.volumeProvider.VolumeExists(ctx, volume)
	if err != nil || imageID <= 0 || currentImage != imageID {
		return attachmentEvidenceUnavailable("detach absence image identity is unverified", err)
	}
	return status.FromContextError(ctx.Err()).Err()
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

func (s *ControllerServer) detachVolumeWithAuthority(ctx context.Context, volume, node string, identity physicalDetachIdentity) (bool, error) {
	runtime := s.driver.kubeRuntime
	if runtime != nil && runtime.enabled {
		if identity.pvUID == "" {
			runtimeCtx, err := runtime.ResolveVolumeRuntimeContext(ctx, volume)
			if err != nil {
				return false, attachmentEvidenceUnavailable("cannot verify detach binding", err)
			}
			pv, err := runtime.client.CoreV1().PersistentVolumes().Get(ctx, runtimeCtx.PVName, metav1.GetOptions{})
			if err != nil || pv.UID == "" {
				return false, attachmentEvidenceUnavailable("cannot verify detach PV UID", err)
			}
			identity.pvUID = pv.UID
		}
		current, err := runtime.NodeIdentity(ctx, node)
		if err != nil || !current.Exists || current.UID == "" || (identity.nodeUID != "" && identity.nodeUID != types.UID(current.UID)) {
			return false, attachmentEvidenceUnavailable("cannot verify detach node UID", err)
		}
		identity.nodeUID = types.UID(current.UID)
	}
	authorize := func(ctx context.Context, imageID, vmID, diskID int) error {
		if imageID != identity.imageID || vmID != identity.vmID || (identity.diskID >= 0 && diskID != identity.diskID) {
			return status.Error(codes.Unavailable, "provider attachment identity changed before detach")
		}
		attempt := identity
		attempt.diskID = diskID
		return s.checkPhysicalDetachAuthority(ctx, volume, node, &attempt)
	}
	if err := s.checkPhysicalDetachAuthority(ctx, volume, node, &identity); err != nil {
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
	if identity.ceph || isCephBackend(identity.backend) || identity.background {
		if _, err := s.volumeProvider.GetVolumeInNode(ctx, identity.imageID, identity.vmID); !errors.Is(err, opennebula.ErrAttachmentNotFound) {
			return false, attachmentEvidenceUnavailable("detach readback did not establish absence", err)
		}
	}
	if err := s.verifyDetachAbsenceTarget(ctx, volume, node, identity.imageID, identity.vmID, identity.nodeUID); err != nil {
		return false, err
	}
	return accepted, nil
}
