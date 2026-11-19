package driver

import (
	"context"
	"crypto/sha256"
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
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

var errReplacementDetachPending = errors.New("replacement attachment requires verified absence and retirement")

type physicalDetachCheck uint8

const (
	detachDispatch physicalDetachCheck = iota
	detachReadback
	detachCanceledOrphanReadback
	detachPreparedRetirement
	detachRetiredReadback
	detachPublishMoveReadback
)

type physicalDetachIdentity struct {
	pvUID, nodeUID        types.UID
	pvName                string
	claimRef              corev1.ObjectReference
	imageID, vmID, diskID int
	background, ceph      bool
	backendInferred       bool
	cephSingleWriter      bool
	backend               string
	attachments           map[string]detachAttachmentAuthority
	replacement           *replacementAttachAttempt
	publishMove           *pendingPublishMove
	stickyRelease         *StickyAttachmentState
	replacementDisk       opennebula.VolumeDiskRecord
	replacementRetired    bool
	remoteNodeUID         types.UID
	remoteNodeName        string
	remoteVMID            int
	remoteWaiting         waitingConsumers
	retiredSource         bool
	placementGrant        detachPlacementGrant
}

type detachPlacementGrant struct {
	override      string
	expires       time.Time
	requiredNode  string
	requiredUntil time.Time
}

func (grant detachPlacementGrant) checkExpiry() error {
	now := time.Now().UTC()
	if (!grant.expires.IsZero() && !grant.expires.After(now)) ||
		(!grant.requiredUntil.IsZero() && !grant.requiredUntil.After(now)) {
		return attachmentEvidenceUnavailable("detach placement grant expired before dispatch", nil)
	}
	return nil
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
			attachments[va.Name] = detachAttachmentSnapshot(&va)
		}
	}
	return attachments, nil
}

func csiVolumeAttachmentName(volume, driver, node string) string {
	return fmt.Sprintf("csi-%x", sha256.Sum256([]byte(volume+driver+node)))
}

func detachAttachmentSnapshot(va *storagev1.VolumeAttachment) detachAttachmentAuthority {
	return detachAttachmentAuthority{
		uid: va.UID, created: va.CreationTimestamp.Time, attacher: va.Spec.Attacher, node: va.Spec.NodeName,
		deleting: !va.DeletionTimestamp.IsZero(), attached: va.Status.Attached,
		metadata: cloneStringMap(va.Status.AttachmentMetadata),
	}
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

func (s *ControllerServer) checkPhysicalDetachAuthority(ctx context.Context, volume, nodeName string, identity *physicalDetachIdentity, check physicalDetachCheck) error {
	readback := check != detachDispatch
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
	if isCephBackend(backend) {
		if err := validatePVImageAssertions(pv.Spec.CSI.VolumeAttributes, identity.imageID); err != nil {
			return err
		}
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
	var publishRelease *publishMoveDetach
	if check == detachPublishMoveReadback {
		move := identity.publishMove
		if replacementDetach || move == nil || move.Detach == nil ||
			(move.Detach.VAUID == "") == (move.Detach.MaintenanceRelease == "") ||
			move.Destination.Name != nodeName || move.Detach.VAName != csiVolumeAttachmentName(volume, s.driver.name, nodeName) ||
			!samePendingPublishMove(history.PendingPublishMove, move) {
			return attachmentEvidenceUnavailable("pending Publish detach readback authority changed", nil)
		}
		publishRelease = move.Detach
	}
	if !readback {
		move := history.PendingPublishMove
		if move != nil && move.Destination.Name != nodeName {
			move = nil
		}
		if (move != nil && (replacementDetach || move.Detach != nil)) ||
			(identity.attachments != nil && !samePendingPublishMove(identity.publishMove, move)) {
			return attachmentEvidenceUnavailable("pending Publish destination detach changed or is awaiting readback", nil)
		}
		identity.publishMove = move
	}
	if identity.replacement != nil && (!replacementDetach || *identity.replacement != *attempt) {
		return attachmentEvidenceUnavailable("replacement detach attempt changed", nil)
	}
	publishDispatch := !readback && identity.publishMove != nil
	publishScope := publishRelease != nil || publishDispatch
	attachmentNode := nodeName
	if replacementDetach || publishScope {
		attachmentNode = ""
	}
	attachments, err := s.detachVolumeAttachments(ctx, pv.Name, attachmentNode)
	if err != nil {
		return err
	}
	if identity.attachments != nil && !reflect.DeepEqual(identity.attachments, attachments) {
		return attachmentEvidenceUnavailable("VolumeAttachment deletion authority changed before detach attempt", nil)
	}
	accepted := readback && replacementDetach && attempt.detachAccepted()
	receipt := readback && replacementDetach && attempt.hasDetachReceipt()
	preparedReadback := readback && replacementDetach && attempt.prepared()
	remoteScope := replacementDetach && ((identity.background && attempt.dispatchPossible()) || receipt || accepted || preparedReadback)
	if replacementDetach && (len(attachments) > 2 || (!remoteScope && len(attachments) > 1)) {
		return attachmentEvidenceUnavailable("replacement detach has conflicting VolumeAttachments", nil)
	}
	var localVAName, destinationVAName string
	var localVA, destinationVA detachAttachmentAuthority
	var publishSuccessorName string
	var publishOld, publishSuccessor detachAttachmentAuthority
	var publishOldName string
	if publishScope {
		publishOldName = csiVolumeAttachmentName(volume, s.driver.name, nodeName)
		if va := attachments[publishOldName]; va.uid != "" && va.deleting && (publishDispatch || va.uid == publishRelease.VAUID) {
			publishOld = va
		}
		if publishDispatch && publishOld.uid == "" && (identity.stickyRelease == nil || !identity.stickyRelease.maintenanceHold()) {
			return attachmentEvidenceUnavailable("pending Publish destination detach custody is unverified", nil)
		}
	}
	for name, va := range attachments {
		if err := validateAttachmentImageAssertions(va.metadata, identity.imageID); err != nil {
			return status.Error(codes.Unavailable, err.Error())
		}
		if replacementDetach && va.node != nodeName {
			if remoteScope && destinationVAName == "" && replacementRemoteAttachment(*attempt, name, va) {
				destinationVAName, destinationVA = name, va
				continue
			}
			return status.Error(codes.Unavailable, "replacement detach has an unverified destination VolumeAttachment")
		}
		if publishScope {
			if name == publishOldName && publishOld.uid != "" {
				continue
			}
			if publishSuccessorName != "" || va.deleting || va.attached || len(va.metadata) != 0 ||
				va.uid == "" || va.uid == publishOld.uid || (publishRelease != nil && va.uid == publishRelease.VAUID) ||
				va.node == "" || (publishDispatch && va.node == nodeName) || va.attacher != s.driver.name ||
				name != csiVolumeAttachmentName(volume, s.driver.name, va.node) {
				return attachmentEvidenceUnavailable("pending Publish successor VolumeAttachment is unverified", nil)
			}
			publishSuccessorName, publishSuccessor = name, va
			continue
		}
		if va.attacher != s.driver.name || va.uid == "" || !va.deleting || (replacementDetach && localVAName != "") {
			return status.Error(codes.Unavailable, "current VolumeAttachment does not authorize physical detach")
		}
		localVAName, localVA = name, va
	}
	acceptedOrphan := accepted && attempt.acceptedOrphan()
	canceledSuccessor := check == detachCanceledOrphanReadback && acceptedOrphan && localVAName != ""
	preparedDeletion := preparedReadback && localVAName != ""
	preparedSuccessor := preparedDeletion && string(localVA.uid) != attempt.VAUID
	preparedRemote := preparedReadback && destinationVAName != ""
	retiredRequired := check == detachRetiredReadback
	if retiredRequired && !receipt && !acceptedOrphan && (!preparedDeletion || preparedSuccessor) {
		return attachmentEvidenceUnavailable("retired replacement source lacks its pinned deleting VolumeAttachment", nil)
	}
	if check == detachPreparedRetirement && (!preparedReadback || preparedDeletion) {
		return attachmentEvidenceUnavailable("prepared replacement retirement is unverified", nil)
	}
	strictWaiting := canceledSuccessor || preparedDeletion || preparedRemote
	if canceledSuccessor && destinationVAName != "" && (destinationVA.uid == localVA.uid || string(destinationVA.uid) == attempt.VAUID) {
		return attachmentEvidenceUnavailable("canceled orphan successor has conflicting VolumeAttachments", nil)
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

	retiredSource := false
	if replacementDetach {
		phaseEligible := attempt.dispatchPossible()
		if readback {
			phaseEligible = attempt.prepared() || attempt.detached() || accepted
		}
		claim := pv.Spec.ClaimRef
		bound := pv.Status.Phase == corev1.VolumeBound && pvc != nil && pvc.Status.Phase == corev1.ClaimBound
		released := pv.Status.Phase == corev1.VolumeReleased
		if !phaseEligible || !attempt.matchesHistory(history) || attempt.Driver != s.driver.name ||
			attempt.Node != nodeName || attempt.ImageID != identity.imageID || attempt.VMID != identity.vmID ||
			attempt.ReplacementUID != string(identity.nodeUID) || attempt.PVUID != string(pv.UID) ||
			pv.Name != history.PVName || (!bound && !released) || !cephRetentionAccessMode(pv.Spec.AccessModes) ||
			!isCephBackend(backend) || claim == nil || attempt.PVCUID != string(claim.UID) ||
			claim.Namespace != history.PVCNamespace || claim.Name != history.PVCName ||
			(pvc != nil && !replacement && !slices.Equal(pv.Spec.AccessModes, pvc.Spec.AccessModes)) {
			return attachmentEvidenceUnavailable("replacement detach binding or identity is unverified", nil)
		}
		if canceledSuccessor && destinationVAName != "" {
			if err := validateMarkedReplacementMetadata(destinationVA, pv, "", attempt.ImageID); err != nil {
				return err
			}
		}
		if preparedRemote {
			if string(destinationVA.uid) == attempt.VAUID || destinationVA.uid == localVA.uid {
				return attachmentEvidenceUnavailable("prepared replacement destination identity changed", nil)
			}
			if err := validateMarkedReplacementMetadata(destinationVA, pv, "", attempt.ImageID); err != nil {
				return err
			}
		}
		if replacement {
			if err := runtime.verifyReleasedClaimNameReuse(ctx, pv, pvc, readback); err != nil {
				return err
			}
		}
		if preparedDeletion {
			if localVAName != attempt.VAName || localVA.attached {
				return attachmentEvidenceUnavailable("prepared replacement detach attachment identity changed", nil)
			}
			if err := validateMarkedReplacementMetadata(localVA, pv, "", attempt.ImageID); err != nil {
				return err
			}
		}
		if attempt.detached() || accepted {
			completion := attempt.DetachCompletion
			if accepted {
				completion = attempt.DetachAcceptance.replacementDetachCompletion
			}
			if !canceledSuccessor && (localVAName != completion.VAName || string(localVA.uid) != completion.VAUID) {
				return attachmentEvidenceUnavailable("completed replacement detach attachment identity changed", nil)
			}
			if !canceledSuccessor {
				if err := validateMarkedReplacementMetadata(localVA, pv, completion.Target, attempt.ImageID); err != nil {
					return err
				}
			}
			identity.diskID, _ = strconv.Atoi(completion.DiskID)
		}
		if check == detachPreparedRetirement || retiredRequired || receipt || acceptedOrphan {
			_, err := runtime.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return attachmentEvidenceUnavailable("replacement source Node identity is unverified", err)
			}
			retiredSource = apierrors.IsNotFound(err)
			if retiredRequired && !retiredSource {
				return attachmentEvidenceUnavailable("replacement source Node has not retired", nil)
			}
			if retiredSource && receipt && localVAName == "" {
				return attachmentEvidenceUnavailable("retired replacement source lacks its receipt VolumeAttachment", nil)
			}
			if retiredSource && acceptedOrphan && localVAName != "" {
				return attachmentEvidenceUnavailable("retired replacement orphan has a current source VolumeAttachment", nil)
			}
		}
		var disk *opennebula.VolumeDiskRecord
		if retiredSource {
			if err := s.verifyRetiredReplacementSource(ctx, *attempt); err != nil {
				return err
			}
		} else {
			disk, err = s.inspectReplacementDisk(ctx, *attempt, !readback)
			if err != nil {
				return err
			}
		}
		if disk != nil {
			if err := validateMarkedReplacementMetadata(localVA, pv, disk.Target, attempt.ImageID); err != nil {
				return err
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
	remoteReplacement := remoteScope &&
		(!preparedReadback || preparedRemote) &&
		pv.Status.Phase == corev1.VolumeBound && pv.DeletionTimestamp == nil && pvc != nil && !replacement &&
		pvc.Status.Phase == corev1.ClaimBound && pvc.DeletionTimestamp == nil &&
		desired.Desired && !desired.MultipleNodes && desired.DemandSource == volumeDemandSourceActivePod &&
		desired.NodeName != "" && desired.NodeName != nodeName
	retiredRemote := retiredSource && acceptedOrphan && destinationVAName != ""
	if retiredSource && acceptedOrphan && remoteReplacement && !retiredRemote {
		return attachmentEvidenceUnavailable("retired replacement orphan has unverified remote demand", nil)
	}
	strictWaiting = strictWaiting || retiredRemote
	if remoteScope && desired.Desired && desired.NodeName != nodeName && !remoteReplacement {
		return attachmentEvidenceUnavailable("replacement detach has no verified source or destination for current remote demand", nil)
	}
	if destinationVAName != "" && (!remoteReplacement || destinationVA.node != desired.NodeName) {
		return attachmentEvidenceUnavailable("replacement detach destination differs from current demand", nil)
	}
	if identity.attachments != nil && remoteReplacement != (identity.remoteNodeUID != "") {
		return attachmentEvidenceUnavailable("replacement detach demand changed", nil)
	}
	protection, err := s.localRWOProtectionProjection(ctx, volume, nodeName, runtimeCtx, false, true)
	if err != nil || protection.Invalid {
		return attachmentEvidenceUnavailable("detach protection is unverified", err)
	}
	orphan, _ := s.orphanTeardownEligible(ctx, volume, nodeName, runtimeCtx, desired, nil)
	override, _ := crossNodeOverrideDeadline(runtimeCtx)
	requiresOverride := protection.Protected && protection.RequiredNode == nodeName && !bypassProtectionForOrphanTeardown(protection, orphan)
	if requiresOverride && !override.After(time.Now().UTC()) {
		return status.Error(codes.Unavailable, "physical detach is blocked by local volume protection")
	}
	var placementGrant detachPlacementGrant
	if !readback && (requiresOverride || protection.OverrideUsed) {
		placementGrant.override = pv.Annotations[annotationAllowCrossNodeUntil]
		placementGrant.expires = override
		if protection.Source == protectionSourceExplicitRequiredNode {
			placementGrant.requiredNode = protection.RequiredNode
			if protection.ExplicitRequiredNodeUntil != nil {
				placementGrant.requiredUntil = protection.ExplicitRequiredNodeUntil.UTC()
			}
		}
	}
	if !readback && identity.attachments != nil && placementGrant != identity.placementGrant {
		return attachmentEvidenceUnavailable("detach placement grant changed before dispatch", nil)
	}
	if err := s.recheckAttachmentAuthority(ctx, volume, runtimeCtx, target, pv, pvc, replacement); err != nil {
		return err
	}
	var publishWaitingNode *corev1.Node
	var publishWaitingVM int
	if publishSuccessorName != "" {
		if replacement || pvc == nil || pv.Status.Phase != corev1.VolumeBound || pvc.Status.Phase != corev1.ClaimBound ||
			!hasSingleWriterAccessMode(pv.Spec.AccessModes) ||
			!desired.Desired || desired.MultipleNodes || desired.NodeName != publishSuccessor.node ||
			desired.DemandSource != volumeDemandSourceActivePod {
			return attachmentEvidenceUnavailable("Publish successor binding or demand is unverified", nil)
		}
		publishWaitingNode, publishWaitingVM, err = s.publishMoveWaitingNode(ctx, identity.publishMove, publishSuccessor)
		if err != nil {
			return err
		}
	}
	waitingOnSource := (receipt || strictWaiting) && !retiredSource && !replacement && !remoteReplacement
	checkConsumers := func() (terminalOnNode bool, remoteConsumerNode string, err error) {
		if claim := pv.Spec.ClaimRef; claim != nil && !waitingOnSource && publishWaitingNode == nil {
			pods, err := runtime.client.CoreV1().Pods(claim.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return false, "", attachmentEvidenceUnavailable("cannot verify Pod release", err)
			}
			for _, pod := range pods.Items {
				referencesPVC, err := podMayUseAttachmentClaim(&pod, pv, pvc, replacement, readback && replacementDetach)
				if err != nil {
					return false, "", attachmentEvidenceUnavailable("cannot verify Pod claim ownership", err)
				}
				if !referencesPVC {
					continue
				}
				if terminalPodMayBeReleased(&pod) {
					terminalOnNode = terminalOnNode || pod.Spec.NodeName == nodeName
					continue
				}
				if pod.Spec.NodeName == "" || pod.Spec.NodeName == nodeName ||
					(replacementDetach && (!remoteReplacement || pod.Spec.NodeName != desired.NodeName ||
						pod.DeletionTimestamp != nil || pod.UID == "")) {
					return false, "", attachmentEvidenceUnavailable("a current Pod still references the volume on the detach node", nil)
				}
				if remoteReplacement || identity.background {
					if (!remoteReplacement && !identity.cephSingleWriter) || !cephHistoryHasObservedAuthority(history) || (remoteConsumerNode != "" && remoteConsumerNode != pod.Spec.NodeName) {
						return false, "", attachmentEvidenceUnavailable("background detach is blocked by remote Pod demand", nil)
					}
					remoteConsumerNode = pod.Spec.NodeName
				}
			}
		}
		return terminalOnNode, remoteConsumerNode, nil
	}
	terminalOnNode, remoteConsumerNode, err := checkConsumers()
	if err != nil {
		return err
	}
	if !receipt && !strictWaiting && publishWaitingNode == nil &&
		(desired.desiresNode(nodeName) || (replacementDetach && !remoteReplacement && (desired.Desired || desired.MultipleNodes))) {
		return attachmentEvidenceUnavailable("current volume release is unverified", nil)
	}
	var node *corev1.Node
	if retiredSource {
		if terminalOnNode {
			return attachmentEvidenceUnavailable("retired replacement source still has a Pod reference", nil)
		}
	} else {
		node, err = runtime.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
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
	}
	if replacementDetach {
		if replacement {
			if err := runtime.verifyReleasedClaimNameReuse(ctx, pv, pvc, readback); err != nil {
				return err
			}
		}
		if node != nil && localVAName != "" && (preparedDeletion || string(localVA.uid) != attempt.VAUID) &&
			(node.CreationTimestamp.IsZero() || !localVA.created.After(node.CreationTimestamp.Time)) {
			return attachmentEvidenceUnavailable("deleting successor VolumeAttachment does not belong to the replacement node", nil)
		}
	}
	var destination *corev1.Node
	var waitingTarget *opennebula.HostArtifactAttachmentTarget
	if remoteReplacement {
		if remoteConsumerNode != desired.NodeName {
			return attachmentEvidenceUnavailable("replacement detach destination demand changed", nil)
		}
		destination, err = runtime.client.CoreV1().Nodes().Get(ctx, remoteConsumerNode, metav1.GetOptions{})
		if err != nil || destination.Name != remoteConsumerNode || destination.UID == "" || destination.DeletionTimestamp != nil ||
			!slices.ContainsFunc(destination.Status.Conditions, func(c corev1.NodeCondition) bool {
				return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
			}) || (destinationVAName != "" && (destination.CreationTimestamp.IsZero() ||
			!destinationVA.created.After(destination.CreationTimestamp.Time))) ||
			(identity.remoteNodeUID != "" && (identity.remoteNodeUID != destination.UID || identity.remoteNodeName != destination.Name)) {
			return attachmentEvidenceUnavailable("replacement detach destination node is unverified", err)
		}
		if canceledSuccessor && (string(destination.UID) == attempt.OriginalNodeUID || destination.UID == node.UID) {
			return attachmentEvidenceUnavailable("orphan successor destination node is unverified", nil)
		}
		if (preparedRemote || retiredRemote) && !replacementDestinationReleased(destination, remoteConsumerNode, destinationVA, *attempt) {
			return attachmentEvidenceUnavailable("prepared replacement destination node is unverified", nil)
		}
		if (preparedRemote && preparedDeletion) || retiredRemote {
			vmID, err := s.volumeProvider.NodeExists(ctx, destination.Name)
			verifier, ok := s.volumeProvider.(opennebula.ReplacementDestinationAbsenceVerifier)
			if err != nil || !ok || vmID <= 0 || vmID == attempt.VMID || vmID == attempt.OriginalVMID ||
				(retiredRemote && identity.remoteVMID != 0 && vmID != identity.remoteVMID) {
				return attachmentEvidenceUnavailable("prepared replacement destination VM is unverified", err)
			}
			if err := verifier.VerifyReplacementDestinationAbsent(ctx, volume, destination.Name, attempt.ImageID, vmID, 0); err != nil {
				return attachmentEvidenceUnavailable("prepared replacement destination disk absence is unverified", err)
			}
			waitingTarget = &opennebula.HostArtifactAttachmentTarget{NodeName: destination.Name, VMID: vmID, ImageID: attempt.ImageID, DiskID: -1}
			if retiredRemote {
				if string(destinationVA.uid) == attempt.VAUID || validateMarkedReplacementMetadata(destinationVA, pv, "", attempt.ImageID) != nil {
					return attachmentEvidenceUnavailable("retired replacement destination attachment is unverified", nil)
				}
				identity.remoteVMID = vmID
			}
		}
		identity.remoteNodeUID, identity.remoteNodeName = destination.UID, destination.Name
	}
	var waitingNode *corev1.Node
	switch {
	case publishWaitingNode != nil:
		waitingNode = publishWaitingNode
	case waitingOnSource:
		waitingNode = node
	case (canceledSuccessor || preparedRemote || retiredRemote) && remoteReplacement:
		waitingNode = destination
	}
	var waiting waitingConsumers
	requireStarted := (strictWaiting || publishWaitingNode != nil) && waitingNode != nil && nodeReportsVolumeInUse(waitingNode, s.driver.name, volume)
	if waitingNode != nil {
		waiting, err = s.replacementWaitingConsumers(ctx, pv, pvc, waitingNode, requireStarted)
		if err != nil {
			return err
		}
		if len(waiting) != 0 && (pvc == nil || pv.Status.Phase != corev1.VolumeBound || pvc.DeletionTimestamp != nil ||
			(strictWaiting && (pvc.Status.Phase != corev1.ClaimBound || pv.DeletionTimestamp != nil))) {
			return attachmentEvidenceUnavailable("replacement detach binding is unverified", nil)
		}
		if (strictWaiting && remoteReplacement || publishWaitingNode != nil) && len(waiting) == 0 {
			return attachmentEvidenceUnavailable("orphan successor destination demand is unverified", nil)
		}
		if retiredRemote && identity.remoteWaiting != nil && !sameWaitingConsumers(identity.remoteWaiting, waiting) {
			return attachmentEvidenceUnavailable("retired replacement destination demand changed", nil)
		}
	}
	waitingOnTarget := waitingNode == node || (readback && publishWaitingNode != nil && publishOld.uid == "" &&
		publishWaitingNode.Name == nodeName && node != nil && publishWaitingNode.UID == node.UID && requireStarted)
	if node != nil && (!waitingOnTarget || len(waiting) == 0) && nodeReportsVolumeInUse(node, s.driver.name, volume) {
		return status.Error(codes.Unavailable, "detach node still reports the volume in use")
	}
	if err := s.recheckAttachmentAuthority(ctx, volume, runtimeCtx, target, pv, pvc, replacement); err != nil {
		return err
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
	if waitingTarget != nil {
		if err := s.attachmentProtectionChecks(ctx, volume, runtimeCtx, *waitingTarget); err != nil {
			return err
		}
	}
	if retiredRequired || waitingTarget != nil {
		if err := s.attachmentProtectionChecks(ctx, volume, runtimeCtx, target); err != nil {
			return err
		}
	}
	if (retiredSource && localVAName != "") || (waitingTarget != nil && preparedDeletion && !preparedSuccessor) {
		if err := s.recheckReplacementVolumeAttachment(ctx, pv.Name, localVAName, localVA); err != nil {
			return err
		}
	} else if retiredSource {
		if _, err := runtime.client.StorageV1().VolumeAttachments().Get(ctx, attempt.VAName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return attachmentEvidenceUnavailable("retired replacement source VolumeAttachment is unverified", err)
		}
	} else if canceledSuccessor || preparedSuccessor {
		if err := s.checkReplacementSuccessor(ctx, *attempt, pv, localVAName, localVA); err != nil {
			return err
		}
	} else if replacementDetach {
		if err := s.checkReplacementVAOrigin(ctx, *attempt, pv.Name, localVAName, localVA); err != nil {
			return err
		}
	}
	finalTerminal, finalRemote, err := checkConsumers()
	if err != nil {
		return err
	}
	if finalTerminal != terminalOnNode || finalRemote != remoteConsumerNode {
		return attachmentEvidenceUnavailable("Pod release changed during attachment checks", nil)
	}
	if waitingNode != nil {
		finalWaiting, err := s.replacementWaitingConsumers(ctx, pv, pvc, waitingNode, requireStarted)
		if err != nil {
			return err
		}
		if !sameWaitingConsumers(waiting, finalWaiting) {
			return attachmentEvidenceUnavailable("Pod release changed during attachment checks", nil)
		}
	}
	if retiredSource {
		if _, err := runtime.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return attachmentEvidenceUnavailable("retired replacement source Node identity changed", err)
		}
	} else if err := s.recheckAttachmentReleaseNode(ctx, node, volume); err != nil {
		return err
	}
	if destination != nil {
		if err := s.recheckAttachmentReleaseNode(ctx, destination, volume); err != nil {
			return err
		}
	}
	if publishWaitingNode != nil {
		target := opennebula.HostArtifactAttachmentTarget{NodeName: publishWaitingNode.Name, VMID: publishWaitingVM, ImageID: identity.imageID, DiskID: -1}
		if err := s.attachmentProtectionChecks(ctx, volume, runtimeCtx, target); err != nil {
			return err
		}
		if err := s.recheckAttachmentReleaseNode(ctx, publishWaitingNode, volume); err != nil {
			return err
		}
		if err := s.recheckPublishMoveSuccessor(ctx, pv.Name, publishSuccessorName, publishSuccessor); err != nil {
			return err
		}
	}
	if publishScope && publishSuccessorName != publishOldName {
		if err := s.recheckPublishMoveOldVA(ctx, identity.publishMove, pv.Name, publishOldName, publishOld); err != nil {
			return err
		}
	}
	if err := s.recheckDetachVolumeAttachments(ctx, pv.Name, attachmentNode, attachments); err != nil {
		return err
	}
	if waitingTarget != nil {
		if err := s.recheckReplacementVolumeAttachment(ctx, pv.Name, destinationVAName, destinationVA); err != nil {
			return err
		}
	}
	if err := s.recheckAttachmentBinding(ctx, pv, pvc, replacement); err != nil {
		return err
	}
	if replacementDetach {
		identity.pvName, identity.claimRef = pv.Name, *pv.Spec.ClaimRef
	}
	identity.attachments = attachments
	if !readback {
		identity.placementGrant = placementGrant
	}
	identity.backend = backend
	identity.retiredSource = retiredSource
	if retiredRemote {
		identity.remoteWaiting = waiting
	}
	return nil
}

func replacementRemoteAttachment(attempt replacementAttachAttempt, name string, va detachAttachmentAuthority) bool {
	return name == csiVolumeAttachmentName(attempt.VolumeID, attempt.Driver, va.node) &&
		va.node != "" && va.node != attempt.Node && va.attacher == attempt.Driver &&
		va.uid != "" && !va.deleting && !va.attached
}

func (s *ControllerServer) recheckAttachmentReleaseNode(ctx context.Context, observed *corev1.Node, volume string) error {
	current, err := s.driver.kubeRuntime.client.CoreV1().Nodes().Get(ctx, observed.Name, metav1.GetOptions{})
	if err != nil || current == nil || current.Name != observed.Name || current.UID != observed.UID || current.DeletionTimestamp != nil ||
		!current.CreationTimestamp.Equal(&observed.CreationTimestamp) ||
		nodeReportsVolumeInUse(current, s.driver.name, volume) != nodeReportsVolumeInUse(observed, s.driver.name, volume) {
		return attachmentEvidenceUnavailable("Node identity or release changed during attachment checks", err)
	}
	currentReady, _ := nodeReadyConditionStatus(current)
	observedReady, _ := nodeReadyConditionStatus(observed)
	if currentReady != observedReady {
		return attachmentEvidenceUnavailable("Node readiness changed during attachment checks", nil)
	}
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
	return s.recheckAttachmentBinding(ctx, pv, pvc, replacement)
}

func (s *ControllerServer) recheckAttachmentBinding(ctx context.Context, pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim, replacement bool) error {
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

func (s *ControllerServer) verifyProviderAttachmentBackend(ctx context.Context, volume, node string, imageID, vmID int, backend string) error {
	if backend == "" {
		return nil
	}
	provider, ok := s.volumeProvider.(opennebula.AttachmentBackendVerifier)
	if !ok {
		return attachmentEvidenceUnavailable("provider cannot verify the attachment backend", nil)
	}
	if err := provider.VerifyAttachmentBackend(ctx, volume, node, imageID, vmID, backend); err != nil {
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
			if attempt.Node == node && (attempt.prepared() || attempt.detached() || attempt.detachAccepted()) && attempt.matchesHistory(history) {
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
		if err := s.checkDetachAbsencePVImage(ctx, volume, imageID, history, false); err != nil {
			return err
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

func (s *ControllerServer) checkDetachAbsencePVImage(ctx context.Context, volume string, imageID int, history VolumeHistoryRecord, imageAbsent bool) error {
	pv, err := s.ownedDeletionPV(ctx, volume)
	if err != nil {
		return attachmentEvidenceUnavailable("cannot verify detach absence PV identity", err)
	}
	if pv != nil {
		return s.verifyPVImageAssertions(ctx, pv, imageID, history, imageAbsent, nil)
	}
	return nil
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

func (s *ControllerServer) finishVerifiedReplacementAbsence(ctx context.Context, volume, node string, identity physicalDetachIdentity, retiredRequired bool) (bool, error) {
	if s.driver.kubeRuntime == nil || !s.driver.kubeRuntime.enabled {
		return false, nil
	}
	history, err := s.readAttachmentHistory(ctx, volume)
	if err != nil {
		return true, attachmentEvidenceUnavailable("detach history is unreadable", err)
	}
	attempt := history.PendingReplacementAttach
	if retiredRequired && (attempt == nil || !attempt.canReadbackRetiredSource() || attempt.Node != node) {
		return false, nil
	}
	if attempt == nil {
		return s.checkReplacedNodeStaleAttachment(ctx, volume, node, identity.imageID, identity.vmID, history, nil)
	}
	if !attempt.matchesHistory(history) {
		return true, attachmentEvidenceUnavailable("replacement attachment release is unresolved", nil)
	}
	if !retiredRequired && attempt.acceptedOrphan() {
		pv, err := s.ownedDeletionPV(ctx, volume)
		if err != nil {
			return true, attachmentEvidenceUnavailable("replacement detach binding is unreadable", err)
		}
		if reboundOrphanPV(pv, *attempt) {
			if identity.imageID != attempt.ImageID {
				return true, attachmentEvidenceUnavailable("rebound attachment image changed", nil)
			}
			if _, err := s.verifyReboundOrphanRelease(ctx, history, *attempt, node, identity.vmID); err != nil {
				return true, err
			}
			return true, s.checkReplacementAttemptReadback(ctx, *attempt)
		}
	}
	if retiredRequired {
		identity.pvUID, identity.nodeUID = types.UID(attempt.PVUID), types.UID(attempt.ReplacementUID)
		identity.replacement = attempt
		if attempt.detachAccepted() {
			return true, s.completeVerifiedReplacementDetach(ctx, volume, node, &identity, detachRetiredReadback)
		}
		if err := s.checkPhysicalDetachAuthority(ctx, volume, node, &identity, detachRetiredReadback); err != nil {
			return true, err
		}
		return true, s.checkReplacementAttemptReadback(ctx, *attempt)
	}
	if attempt.Node != node {
		return true, s.verifyCanceledReplacementDestination(ctx, history, *attempt, node, identity.imageID, identity.vmID)
	}
	if !attempt.prepared() && !attempt.detached() && !attempt.detachAccepted() {
		return true, attachmentEvidenceUnavailable("replacement attachment release is unresolved", nil)
	}
	if err := s.resolvePhysicalDetachIdentity(ctx, volume, node, &identity); err != nil {
		return true, err
	}
	identity.replacement = attempt
	if attempt.detachAccepted() {
		return true, s.completeVerifiedReplacementDetach(ctx, volume, node, &identity, detachCanceledOrphanReadback)
	}
	if err := s.checkPhysicalDetachAuthority(ctx, volume, node, &identity, detachReadback); err != nil {
		return true, err
	}
	if attempt.detached() {
		return true, nil
	}
	if identity.attachments[attempt.VAName].deleting {
		return true, s.checkReplacementAttemptReadback(ctx, *attempt)
	}
	return true, s.retireVerifiedReplacement(ctx, *attempt)
}

func (s *ControllerServer) retireVerifiedReplacement(ctx context.Context, attempt replacementAttachAttempt) error {
	if err := s.retireReplacementAttach(ctx, attempt); err != nil {
		return attachmentEvidenceUnavailable("replacement detach retirement is unverified", err)
	}
	return nil
}

func (s *ControllerServer) recordReplacementDetachAcceptance(ctx context.Context, volume, node, vmName string, imageID, vmID, diskID int, identity *physicalDetachIdentity) error {
	attempt := identity.replacement
	disk := identity.replacementDisk
	if attempt == nil || !attempt.dispatchPossible() || attempt.Driver != s.driver.name ||
		attempt.VolumeID != volume || attempt.Node != node || attempt.ReplacementUID != string(identity.nodeUID) ||
		attempt.PVUID != string(identity.pvUID) || attempt.PVCUID != string(identity.claimRef.UID) || identity.attachments == nil ||
		imageID != attempt.ImageID || imageID != identity.imageID || vmID != attempt.VMID || vmID != disk.NodeID ||
		vmName != disk.NodeName || diskID != disk.DiskID {
		return attachmentEvidenceUnavailable("accepted replacement detach scope is unverified", nil)
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	accepted := *attempt
	accepted.Phase = replacementDetachAccepted
	accepted.DetachAcceptance = replacementDetachAcceptance{
		VMName: vmName,
		replacementDetachCompletion: replacementDetachCompletion{
			DiskID: strconv.Itoa(diskID), Target: disk.Target, Serial: disk.Serial,
		},
	}
	if va, ok := identity.attachments[attempt.VAName]; ok {
		accepted.DetachAcceptance.VAName, accepted.DetachAcceptance.VAUID = attempt.VAName, string(va.uid)
	}
	err := retryOnConflict(retry.DefaultRetry, func() error {
		_, writeErr := s.driver.volumeHistory.updateHistory(ctx, volume, true, func(current *VolumeHistoryRecord) error {
			if current.PendingReplacementAttach == nil || *current.PendingReplacementAttach != *attempt ||
				!accepted.matchesHistory(*current) || current.PVName != identity.pvName ||
				current.PVCName != identity.claimRef.Name || current.PVCNamespace != identity.claimRef.Namespace {
				return fmt.Errorf("replacement attachment history changed before recording detach acceptance")
			}
			current.PendingReplacementAttach = &accepted
			return nil
		})
		if apierrors.IsConflict(writeErr) {
			return writeErr
		}
		if err := s.checkReplacementAttemptReadback(ctx, accepted); err != nil {
			return attachmentEvidenceUnavailable("replacement detach acceptance is unverified", errors.Join(writeErr, err))
		}
		return nil
	})
	if err == nil {
		identity.replacement = &accepted
	}
	return err
}

func (s *ControllerServer) completeVerifiedReplacementDetach(ctx context.Context, volume, node string, identity *physicalDetachIdentity, check physicalDetachCheck) error {
	attempt := *identity.replacement
	if !attempt.detachAccepted() {
		return attachmentEvidenceUnavailable("replacement detach acceptance is unavailable", nil)
	}
	completed := attempt
	completed.Phase = replacementAttachDetached
	completed.DetachAcceptance = replacementDetachAcceptance{}
	completed.DetachCompletion = attempt.DetachAcceptance.replacementDetachCompletion
	retire := completed.DetachCompletion.VAUID == ""
	acknowledgedSuccessor := false
	err := retryOnConflict(retry.DefaultRetry, func() error {
		if err := s.checkPhysicalDetachAuthority(ctx, volume, node, identity, check); err != nil {
			return err
		}
		if check == detachCanceledOrphanReadback && attempt.acceptedOrphan() && identity.attachments[attempt.VAName].node == node {
			acknowledgedSuccessor = true
			return nil
		}
		if retire {
			if identity.retiredSource && identity.remoteNodeUID != "" {
				if err := s.checkPhysicalDetachAuthority(ctx, volume, node, identity, detachRetiredReadback); err != nil {
					return err
				}
			}
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
	if acknowledgedSuccessor {
		return nil
	}
	if retire {
		identity.replacementRetired = true
	} else {
		identity.replacement = &completed
	}
	if check != detachRetiredReadback {
		check = detachReadback
	}
	return s.checkPhysicalDetachAuthority(ctx, volume, node, identity, check)
}

func (s *ControllerServer) detachVolumeWithAuthority(ctx context.Context, volume, node string, identity physicalDetachIdentity) (bool, error) {
	if err := s.resolvePhysicalDetachIdentity(ctx, volume, node, &identity); err != nil {
		return false, err
	}
	var move *pendingPublishMove
	var detach *publishMoveDetach
	authorize := func(ctx context.Context, imageID, vmID, diskID int) error {
		if imageID != identity.imageID || vmID != identity.vmID || (identity.diskID >= 0 && diskID != identity.diskID) {
			return status.Error(codes.Unavailable, "provider attachment identity changed before detach")
		}
		if err := identity.placementGrant.checkExpiry(); err != nil {
			return err
		}
		attempt := identity
		attempt.diskID = diskID
		if err := s.checkPhysicalDetachAuthority(ctx, volume, node, &attempt, detachDispatch); err != nil {
			return err
		}
		if err := s.checkPublishMoveAdmission(ctx, volume); err != nil {
			return err
		}
		var err error
		move, detach, err = s.preparePublishMoveDetach(ctx, volume, node, attempt)
		if err != nil {
			return err
		}
		if err := attempt.placementGrant.checkExpiry(); err != nil {
			return err
		}
		identity = attempt
		return nil
	}
	if err := s.checkPhysicalDetachAuthority(ctx, volume, node, &identity, detachDispatch); err != nil {
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
	providerCtx := opennebula.WithExpectedImageID(ctx, identity.imageID)
	if attempt := identity.replacement; attempt != nil {
		providerCtx = opennebula.WithReplacementDetachReadback(providerCtx, attempt.OriginalVMID, attempt.OriginalVMName,
			func(ctx context.Context, vmName string, imageID, vmID, diskID int) error {
				return s.recordReplacementDetachAcceptance(ctx, volume, node, vmName, imageID, vmID, diskID, &identity)
			})
	} else if identity.publishMove != nil {
		providerCtx = opennebula.WithDetachAcceptance(providerCtx, func(ctx context.Context, imageID, vmID, diskID int) error {
			if move == nil || detach == nil || move.Image != imageID || move.Destination.VM != vmID || move.Slot != diskID {
				return attachmentEvidenceUnavailable("pending Publish destination detach acceptance is unverified", nil)
			}
			return s.recordPublishMoveDetach(ctx, volume, move, detach)
		})
	}
	accepted, err := provider.DetachVolumeWithAuthority(providerCtx, volume, node, identity.vmID, identity.backend, verifyOwner, authorize)
	if parentErr := ctx.Err(); parentErr != nil {
		return false, parentErr
	}
	if err != nil {
		return false, err
	}
	if identity.publishMove != nil {
		if !accepted || move == nil || detach == nil {
			return false, attachmentEvidenceUnavailable("pending Publish destination detach outcome is unverified", nil)
		}
		updated := *move
		updated.Detach = detach
		return true, s.settlePublishMoveDetach(ctx, volume, &updated, false)
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
		if !accepted {
			return false, attachmentEvidenceUnavailable("replacement detach outcome is unverified", nil)
		}
		check := detachReadback
		if retiredRequired {
			check = detachRetiredReadback
		}
		if err := s.completeVerifiedReplacementDetach(ctx, volume, node, &identity, check); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := s.checkDetachAbsenceTarget(ctx, volume, node, identity.imageID, identity.vmID, identity.nodeUID, retiredRequired); err != nil {
		return false, err
	}
	return accepted, nil
}
