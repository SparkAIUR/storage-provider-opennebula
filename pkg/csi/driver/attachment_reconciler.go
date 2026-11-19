package driver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

type AttachmentReconciler struct {
	server       *ControllerServer
	interval     time.Duration
	orphanGrace  time.Duration
	staleVAGrace time.Duration

	mu              sync.Mutex
	orphanSeen      map[string]time.Time
	staleVASeen     map[string]time.Time
	divergentSeen   map[string]time.Time
	multiAttachSeen map[string]time.Time
}

func NewAttachmentReconciler(server *ControllerServer) *AttachmentReconciler {
	intervalSeconds, ok := server.driver.PluginConfig.GetInt(config.StuckAttachmentReconcilerIntervalVar)
	if !ok || intervalSeconds <= 0 {
		intervalSeconds = 60
	}
	orphanGraceSeconds, ok := server.driver.PluginConfig.GetInt(config.StuckAttachmentOrphanGraceSecondsVar)
	if !ok || orphanGraceSeconds <= 0 {
		orphanGraceSeconds = 120
	}
	staleVAGraceSeconds, ok := server.driver.PluginConfig.GetInt(config.StuckAttachmentStaleVAGraceSecondsVar)
	if !ok || staleVAGraceSeconds <= 0 {
		staleVAGraceSeconds = 90
	}
	return &AttachmentReconciler{
		server:          server,
		interval:        time.Duration(intervalSeconds) * time.Second,
		orphanGrace:     time.Duration(orphanGraceSeconds) * time.Second,
		staleVAGrace:    time.Duration(staleVAGraceSeconds) * time.Second,
		orphanSeen:      map[string]time.Time{},
		staleVASeen:     map[string]time.Time{},
		divergentSeen:   map[string]time.Time{},
		multiAttachSeen: map[string]time.Time{},
	}
}

func (r *AttachmentReconciler) Run(ctx context.Context) {
	if r == nil || r.server == nil || r.server.driver == nil || r.server.driver.kubeRuntime == nil || !r.server.driver.kubeRuntime.enabled {
		return
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.server.driver.controllerLeadership != nil && !r.server.driver.controllerLeadership.IsLeader() {
				continue
			}
			if err := r.ReconcileOnce(ctx); err != nil {
				klog.V(2).InfoS("Attachment reconciler loop failed", "err", err)
			}
		}
	}
}

func (r *AttachmentReconciler) ReconcileOnce(ctx context.Context) error {
	client := r.server.driver.kubeRuntime.client
	if client == nil {
		return nil
	}

	attachments, vmNames, err := r.server.volumeProvider.ListCurrentAttachments(ctx)
	if err != nil {
		return err
	}
	vaList, err := client.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	pvList, err := client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	podList, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	nodeList, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	pvByName := make(map[string]*corev1.PersistentVolume, len(pvList.Items))
	pvByHandle := make(map[string]*corev1.PersistentVolume, len(pvList.Items))
	ambiguousHandles := map[string]bool{}
	presentHandles := make(map[string]*corev1.PersistentVolume, len(pvList.Items))
	for idx := range pvList.Items {
		pv := &pvList.Items[idx]
		if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle == "" {
			continue
		}
		handle := pv.Spec.CSI.VolumeHandle
		presentHandles[handle] = pv
		if pv.Spec.CSI.Driver != r.server.driver.name {
			continue
		}
		pvByName[pv.Name] = pv
		if pvByHandle[handle] != nil || ambiguousHandles[handle] {
			ambiguousHandles[handle] = true
			delete(pvByHandle, handle)
			continue
		}
		pvByHandle[handle] = pv
	}
	existingNodes := make(map[string]struct{}, len(nodeList.Items))
	for idx := range nodeList.Items {
		existingNodes[strings.TrimSpace(nodeList.Items[idx].Name)] = struct{}{}
	}
	r.cleanupStaleDriverState(ctx, presentHandles, existingNodes)

	podsByClaim := make(map[string][]*corev1.Pod)
	for idx := range podList.Items {
		pod := &podList.Items[idx]
		if terminalPodMayBeReleased(pod) {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if name := podVolumeClaimName(pod, volume); name != "" {
				key := pod.Namespace + "/" + name
				podsByClaim[key] = append(podsByClaim[key], pod)
			}
		}
	}

	attachmentsByVolume := make(map[string][]opennebula.ObservedAttachment)
	attachmentByVolumeNode := make(map[string]struct{})
	unresolvedVolume := make(map[string]bool)
	nodesByVM := map[int]*corev1.Node{}
	if len(attachments) > 0 {
		nodesByVM = attachmentNodesByVM(vmNames, nodeList.Items)
	}
	for _, attachment := range attachments {
		node := nodesByVM[attachment.NodeID]
		if node == nil || attachment.DiskID < 0 {
			unresolvedVolume[attachment.VolumeHandle] = true
			continue
		}
		attachment.NodeName = node.Name
		attachmentsByVolume[attachment.VolumeHandle] = append(attachmentsByVolume[attachment.VolumeHandle], attachment)
		attachmentByVolumeNode[attachment.VolumeHandle+"@"+node.Name] = struct{}{}
	}

	vaByVolume := make(map[string][]storagev1.VolumeAttachment)
	for _, va := range vaList.Items {
		if va.Spec.Attacher != r.server.driver.name || va.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		pv := pvByName[*va.Spec.Source.PersistentVolumeName]
		if pv == nil || pv.Spec.CSI == nil || ambiguousHandles[pv.Spec.CSI.VolumeHandle] {
			continue
		}
		vaByVolume[pv.Spec.CSI.VolumeHandle] = append(vaByVolume[pv.Spec.CSI.VolumeHandle], va)
	}

	now := time.Now().UTC()
	currentOrphans := map[string]struct{}{}
	currentStaleVAs := map[string]struct{}{}
	currentDivergent := map[string]struct{}{}
	currentMultiAttach := map[string]struct{}{}

	for volumeHandle, observed := range attachmentsByVolume {
		pv := pvByHandle[volumeHandle]
		if pv == nil || pv.Spec.CSI == nil || ambiguousHandles[pv.Spec.CSI.VolumeHandle] {
			continue
		}
		backend, backendErr := persistentVolumeBackend(pv)
		undeclared := errors.Is(backendErr, errAttachmentBackendUndeclared)
		if unresolvedVolume[volumeHandle] || (backendErr != nil && !undeclared) || opennebula.IsSharedFilesystemVolumeID(volumeHandle) {
			continue
		}
		if undeclared {
			backend, _ = verifiedAttachmentBackend("", observed[0].Backend)
		}
		if backend == "" || backend == "cephfs" {
			continue
		}
		if slices.ContainsFunc(observed, func(attachment opennebula.ObservedAttachment) bool {
			providerBackend, err := verifiedAttachmentBackend("", attachment.Backend)
			if err != nil {
				return undeclared
			}
			return providerBackend != backend
		}) {
			continue
		}

		pvc, replacement, err := r.server.driver.kubeRuntime.attachmentClaim(ctx, pv)
		if err != nil {
			continue
		}
		users := make(map[string]bool)
		unknownConsumer := false
		if claim := pv.Spec.ClaimRef; claim != nil {
			for _, pod := range podsByClaim[claim.Namespace+"/"+claim.Name] {
				referencesPVC, err := podMayUseAttachmentClaim(pod, pv, pvc, replacement, false)
				if err != nil {
					unknownConsumer = true
					break
				}
				if referencesPVC {
					users[pod.Spec.NodeName] = true
				}
			}
		}
		if unknownConsumer {
			continue
		}
		vas := vaByVolume[volumeHandle]
		singleWriter := hasSingleWriterAccessMode(pv.Spec.AccessModes)
		cephSingleWriter := singleWriter && isCephBackend(backend)
		if singleWriter && len(observed) > 1 {
			currentMultiAttach[volumeHandle] = struct{}{}
			r.server.driver.metrics.RecordAttachmentReconciler("multi_attach", "warn", "observed")
			if cephSingleWriter {
				continue
			}
		}

		for _, attachment := range observed {
			key := volumeHandle + "@" + attachment.NodeName + "@" + string(pv.UID)
			remoteCephDemand := cephSingleWriter &&
				!users[""] && !users[attachment.NodeName] && len(users) == 1
			matchingAttachedVA := slices.IndexFunc(vas, func(va storagev1.VolumeAttachment) bool {
				return strings.TrimSpace(va.Spec.NodeName) == attachment.NodeName && va.Status.Attached
			})
			if matchingAttachedVA >= 0 {
				va := vas[matchingAttachedVA]
				if !remoteCephDemand || va.DeletionTimestamp == nil {
					continue
				}
				history, err := r.server.readAttachmentHistory(ctx, volumeHandle)
				attempt := history.PendingReplacementAttach
				if err != nil || attempt == nil || !attempt.dispatchPossible() || !attempt.matchesHistory(history) ||
					va.Name != attempt.VAName || va.UID == "" {
					continue
				}
			}
			if len(users) > 0 && !remoteCephDemand {
				continue
			}
			queueScope := ""
			if remoteCephDemand {
				queueScope = attachment.NodeName
			}
			if r.skipVolume(ctx, volumeHandle, queueScope) {
				continue
			}
			if len(vas) > 0 {
				currentDivergent[key] = struct{}{}
				if !r.due(r.divergentSeen, key, now, r.orphanGrace) {
					continue
				}
				if err := r.detachObservedAttachment(ctx, attachment, pv.UID, nodesByVM[attachment.NodeID].UID, backend, cephSingleWriter, hotplugQueuePriorityCritical); err != nil {
					r.server.driver.metrics.RecordAttachmentReconciler("divergent_attachment", "detach", attachmentCleanupOutcome(err))
					klog.V(2).InfoS("Failed to detach divergent attachment", "volume", volumeHandle, "node", attachment.NodeName, "err", err)
				} else {
					r.server.driver.metrics.RecordAttachmentReconciler("divergent_attachment", "detach", "success")
					r.recordPVCEventForPV(ctx, pv, eventReasonStuckAttachmentDetached, fmt.Sprintf("detached divergent attachment for volume %s from node %s", volumeHandle, attachment.NodeName))
				}
				continue
			}

			currentOrphans[key] = struct{}{}
			if !r.due(r.orphanSeen, key, now, r.orphanGrace) {
				continue
			}
			if err := r.detachObservedAttachment(ctx, attachment, pv.UID, nodesByVM[attachment.NodeID].UID, backend, cephSingleWriter, hotplugQueuePriorityBackground); err != nil {
				r.server.driver.metrics.RecordAttachmentReconciler("orphan_attachment", "detach", attachmentCleanupOutcome(err))
				klog.V(2).InfoS("Failed to detach orphan attachment", "volume", volumeHandle, "node", attachment.NodeName, "err", err)
			} else {
				r.server.driver.metrics.RecordAttachmentReconciler("orphan_attachment", "detach", "success")
				r.recordPVCEventForPV(ctx, pv, eventReasonStuckAttachmentDetached, fmt.Sprintf("detached orphan attachment for volume %s from node %s", volumeHandle, attachment.NodeName))
			}
		}
	}

	for idx := range vaList.Items {
		va := &vaList.Items[idx]
		if va.Spec.Attacher != r.server.driver.name || va.Spec.Source.PersistentVolumeName == nil || !va.Status.Attached || va.UID == "" {
			continue
		}
		pv := pvByName[*va.Spec.Source.PersistentVolumeName]
		if pv == nil || pv.Spec.CSI == nil || ambiguousHandles[pv.Spec.CSI.VolumeHandle] {
			continue
		}
		volumeHandle := pv.Spec.CSI.VolumeHandle
		if unresolvedVolume[volumeHandle] || r.skipDiskAttachmentReconcile(ctx, volumeHandle, pv) {
			continue
		}
		if _, ok := attachmentByVolumeNode[volumeHandle+"@"+va.Spec.NodeName]; ok {
			continue
		}
		key := string(va.UID)
		currentStaleVAs[key] = struct{}{}
		if !r.due(r.staleVASeen, key, now, r.staleVAGrace) {
			continue
		}
		if err := r.deleteStaleVolumeAttachment(ctx, pv, va); err != nil {
			r.server.driver.metrics.RecordAttachmentReconciler("stale_volume_attachment", "delete", "error")
			klog.V(2).InfoS("Failed to delete stale VolumeAttachment", "name", va.Name, "err", err)
			continue
		}
		r.server.driver.metrics.RecordAttachmentReconciler("stale_volume_attachment", "delete", "success")
		r.recordPVCEventForPV(ctx, pv, eventReasonStuckAttachmentRepublish, fmt.Sprintf("requested deletion of stale VolumeAttachment %s for volume %s to trigger republish", va.Name, volumeHandle))
	}

	r.reconcileReleasedReplacementAttempts(ctx, pvByHandle, attachmentsByVolume, vaByVolume, unresolvedVolume, currentOrphans, now)
	r.prune(r.orphanSeen, currentOrphans)
	r.prune(r.staleVASeen, currentStaleVAs)
	r.prune(r.divergentSeen, currentDivergent)
	r.prune(r.multiAttachSeen, currentMultiAttach)
	return nil
}

func (r *AttachmentReconciler) reconcileReleasedReplacementAttempts(ctx context.Context, pvs map[string]*corev1.PersistentVolume, disks map[string][]opennebula.ObservedAttachment, vas map[string][]storagev1.VolumeAttachment, unresolved map[string]bool, currentOrphans map[string]struct{}, now time.Time) {
	s := r.server
	manager := s.driver.volumeHistory
	if manager == nil {
		return
	}
	cm, err := manager.runtime.GetConfigMap(ctx, manager.namespace, volumeHistoryStateConfigMapName)
	if err != nil {
		return
	}
	for volume, raw := range cm.Data {
		history, err := decodeAttachmentHistory(raw, volume)
		if err != nil || history.PendingReplacementAttach == nil {
			continue
		}
		attempt := history.PendingReplacementAttach
		if attempt.detached() {
			r.retireCompletedReplacement(ctx, history)
			continue
		}
		pv := pvs[volume]
		successor := slices.ContainsFunc(vas[volume], func(va storagev1.VolumeAttachment) bool {
			return va.Spec.NodeName == attempt.Node && va.DeletionTimestamp == nil && string(va.UID) != attempt.VAUID
		})
		awaitingFinalization := slices.ContainsFunc(vas[volume], func(va storagev1.VolumeAttachment) bool {
			return string(va.UID) == attempt.VAUID || va.DeletionTimestamp != nil
		})
		if (attempt.prepared() || attempt.detachAccepted()) && attempt.matchesHistory(history) && attempt.Driver == s.driver.name &&
			pv != nil && (attempt.PVUID == string(pv.UID) || attempt.acceptedOrphan()) && len(disks[volume]) == 0 &&
			(attempt.detachAccepted() || !awaitingFinalization) && !unresolved[volume] && !r.skipDiskAttachmentReconcile(ctx, volume, pv) {
			key := volume + "@" + attempt.Node + "@" + string(pv.UID) + "@" + attempt.ID
			currentOrphans[key] = struct{}{}
			if r.due(r.orphanSeen, key, now, r.orphanGrace) {
				r.settleReleasedReplacement(ctx, history, *attempt, pv, successor)
			}
		}
	}
}

func (r *AttachmentReconciler) settleReleasedReplacement(ctx context.Context, history VolumeHistoryRecord, attempt replacementAttachAttempt, pv *corev1.PersistentVolume, successor bool) {
	s := r.server
	nodeRelease := s.driver.operationLocks.Acquire(controllerNodeLockKey(attempt.Node))
	defer nodeRelease()
	volumeRelease := s.driver.operationLocks.Acquire(controllerVolumeLockKey(attempt.VolumeID))
	defer volumeRelease()
	identity := physicalDetachIdentity{pvUID: pv.UID, nodeUID: types.UID(attempt.ReplacementUID), imageID: attempt.ImageID, vmID: attempt.VMID,
		diskID: -1, background: true, cephSingleWriter: true, replacement: &attempt}
	var err error
	if attempt.acceptedOrphan() && reboundOrphanPV(pv, attempt) {
		err = s.retireReboundOrphanReceipt(ctx, history, attempt)
	} else if successor && (attempt.acceptedOrphan() || attempt.prepared()) {
		err = s.retireReplacementForSuccessor(ctx, history, attempt)
	} else if attempt.detachAccepted() {
		err = s.completeVerifiedReplacementDetach(ctx, attempt.VolumeID, attempt.Node, &identity, detachReadback)
	} else if err = s.checkPhysicalDetachAuthority(ctx, attempt.VolumeID, attempt.Node, &identity, detachPreparedRetirement); err == nil {
		err = s.retireVerifiedReplacement(ctx, attempt)
	}
	if err != nil {
		klog.V(2).InfoS("Replacement release is unverified", "volume", attempt.VolumeID, "node", attempt.Node, "err", err)
	}
}

func (r *AttachmentReconciler) retireCompletedReplacement(ctx context.Context, history VolumeHistoryRecord) {
	s := r.server
	attempt := history.PendingReplacementAttach
	nodeRelease := s.driver.operationLocks.Acquire(controllerNodeLockKey(attempt.Node))
	defer nodeRelease()
	volumeRelease := s.driver.operationLocks.Acquire(controllerVolumeLockKey(attempt.VolumeID))
	defer volumeRelease()
	if _, err := s.retireCompletedReplacement(ctx, history); err != nil {
		klog.V(2).InfoS("Completed replacement retirement is unverified", "volume", attempt.VolumeID, "node", attempt.Node, "err", err)
	}
}

func attachmentNodesByVM(vmNames opennebula.VMNameInventory, nodes []corev1.Node) map[int]*corev1.Node {
	byVM := make(map[int]*corev1.Node)
	conflicts := make(map[int]bool)
	for i := range nodes {
		node := &nodes[i]
		vmID, err := vmNames.Resolve(node.Name)
		var notFound *opennebula.VMNotFoundError
		var ambiguous *opennebula.VMDuplicateNameError
		switch {
		case errors.As(err, &notFound):
			continue
		case errors.As(err, &ambiguous):
			for _, id := range ambiguous.Match {
				conflicts[id] = true
			}
			continue
		case err != nil || vmID <= 0:
			continue
		}
		if byVM[vmID] != nil || node.UID == "" || node.DeletionTimestamp != nil {
			conflicts[vmID] = true
		}
		byVM[vmID] = node
	}
	for vmID := range conflicts {
		delete(byVM, vmID)
	}
	return byVM
}

func (r *AttachmentReconciler) cleanupStaleDriverState(ctx context.Context, pvByHandle map[string]*corev1.PersistentVolume, existingNodes map[string]struct{}) {
	if r == nil || r.server == nil || r.server.driver == nil {
		return
	}
	if r.server.driver.stickyAttachments != nil {
		for volumeID, state := range r.server.driver.stickyAttachments.Snapshot() {
			if pvByHandle[volumeID] == nil || nodeMissing(existingNodes, state.NodeID) {
				_ = r.server.driver.stickyAttachments.Clear(volumeID)
			}
		}
	}
	if r.server.driver.volumeHistory != nil {
		for volumeID := range r.server.driver.volumeHistory.Snapshot() {
			if pvByHandle[volumeID] == nil {
				_ = r.server.driver.volumeHistory.Clear(ctx, volumeID)
			}
		}
	}
	if r.server.driver.volumeRepairState != nil {
		for volumeID := range r.server.driver.volumeRepairState.Snapshot() {
			if pvByHandle[volumeID] == nil {
				_ = r.server.driver.volumeRepairState.Clear(ctx, volumeID)
			}
		}
	}
	if r.server.driver.volumeRecoveryControl != nil {
		for volumeID, observed := range r.server.driver.volumeRecoveryControl.Snapshot() {
			if pvByHandle[volumeID] == nil {
				_ = r.server.driver.volumeRecoveryControl.clearIfCurrent(ctx, volumeID, observed)
			}
		}
	}
	if r.server.driver.kubeRuntime != nil && r.server.driver.kubeRuntime.enabled {
		r.cleanupNodeScopedState(ctx, existingNodes)
		r.cleanupLocalDeviceReports(ctx, pvByHandle, existingNodes)
	}
}

func (r *AttachmentReconciler) cleanupNodeScopedState(ctx context.Context, existingNodes map[string]struct{}) {
	if r == nil || r.server == nil || r.server.driver == nil || r.server.driver.kubeRuntime == nil {
		return
	}
	namespace := namespaceFromServiceAccount()
	for _, name := range []string{hotplugStateConfigMapName, hotplugQueueStateConfigMapName} {
		cm, err := r.server.driver.kubeRuntime.GetConfigMap(ctx, namespace, name)
		if err != nil {
			continue
		}
		for key := range cm.Data {
			if name == hotplugStateConfigMapName && isMaintenanceConfigMapKey(key) {
				continue
			}
			if nodeMissing(existingNodes, key) {
				_ = r.server.driver.kubeRuntime.DeleteConfigMapKey(ctx, namespace, name, key)
			}
		}
	}
}

func (r *AttachmentReconciler) cleanupLocalDeviceReports(ctx context.Context, pvByHandle map[string]*corev1.PersistentVolume, existingNodes map[string]struct{}) {
	if r == nil || r.server == nil || r.server.driver == nil || r.server.driver.kubeRuntime == nil {
		return
	}
	namespace := namespaceFromServiceAccount()
	cm, err := r.server.driver.kubeRuntime.GetConfigMap(ctx, namespace, localDeviceStateConfigMapName)
	if err != nil {
		return
	}
	for key, raw := range cm.Data {
		report, err := decodeLocalDeviceReport(key, raw, "")
		if err != nil {
			continue
		}
		if pvByHandle[strings.TrimSpace(report.VolumeID)] != nil && !nodeMissing(existingNodes, report.Node) {
			continue
		}
		ambiguous := false
		for handle := range pvByHandle {
			node, matches := localDeviceReportKeyNode(key, handle)
			if matches && !nodeMissing(existingNodes, node) {
				ambiguous = true
				break
			}
		}
		if !ambiguous {
			_ = r.server.driver.kubeRuntime.deleteConfigMapKeyIfCurrent(ctx, cm, key, raw)
		}
	}
}

func nodeMissing(existingNodes map[string]struct{}, node string) bool {
	node = strings.TrimSpace(node)
	if node == "" {
		return false
	}
	_, ok := existingNodes[node]
	return !ok
}

func (r *AttachmentReconciler) skipDiskAttachmentReconcile(ctx context.Context, volumeHandle string, pv *corev1.PersistentVolume) bool {
	if r.skipVolume(ctx, volumeHandle, "") {
		return true
	}
	// The stuck attachment reconciler compares Kubernetes VolumeAttachment
	// objects to OpenNebula VM disk inventory. Shared filesystem volumes are
	// intentionally node-agnostic and never appear in that disk inventory, so
	// treating their VolumeAttachments as stale creates an endless
	// delete/republish loop for healthy CephFS RWX workloads.
	if opennebula.IsSharedFilesystemVolumeID(volumeHandle) {
		return true
	}
	if pv != nil {
		backend, err := persistentVolumeBackend(pv)
		return (err != nil && !errors.Is(err, errAttachmentBackendUndeclared)) || backend == "cephfs"
	}
	return false
}

func (r *AttachmentReconciler) skipVolume(ctx context.Context, volumeHandle, cleanupNode string) bool {
	if r == nil || r.server == nil || r.server.driver == nil {
		return true
	}
	if r.server.driver.kubeRuntime == nil || !r.server.driver.kubeRuntime.enabled {
		return true
	}
	runtimeCtx, err := r.server.driver.kubeRuntime.ResolveVolumeRuntimeContext(ctx, volumeHandle)
	if err != nil {
		return true
	}
	control := resolvedRecoveryControlState(volumeHandle, runtimeCtx, VolumeRecoveryControlState{})
	if control.Invalid || control.Active() {
		return true
	}
	if r.server.driver.stickyAttachments != nil {
		if _, ok := r.server.driver.stickyAttachments.Get(volumeHandle); ok {
			return true
		}
	}
	if r.server.driver.hotplugQueue != nil && r.server.driver.hotplugQueue.hasVolume(volumeHandle, cleanupNode) {
		return true
	}
	return false
}

func (r *AttachmentReconciler) detachObservedAttachment(ctx context.Context, attachment opennebula.ObservedAttachment, pvUID, nodeUID types.UID, backend string, cephSingleWriter bool, priority HotplugQueuePriority) error {
	if pvUID == "" || nodeUID == "" {
		return errAttachmentSkipped
	}
	confirmedAbsent := func(ctx context.Context) error {
		if err := r.server.verifyDetachAbsenceTarget(ctx, attachment.VolumeHandle, attachment.NodeName, attachment.ImageID, attachment.NodeID, nodeUID); err != nil {
			return err
		}
		return errAttachmentAbsent
	}
	detached := false
	err := r.server.withQueuedHotplug(ctx, attachment.NodeName, "detach", attachment.VolumeHandle, priority, func(queueCtx context.Context) error {
		nodeRelease := r.server.driver.operationLocks.Acquire(controllerNodeLockKey(attachment.NodeName))
		defer nodeRelease()

		volumeRelease := r.server.driver.operationLocks.Acquire(controllerVolumeLockKey(attachment.VolumeHandle))
		defer volumeRelease()

		decision := r.server.evaluateVolumeReconcile(queueCtx, attachment.NodeName, "detach", attachment.VolumeHandle)
		if decision.Action == VolumeReconcileProceed || decision.Action == VolumeReconcileComplete {
			if decision.Evidence.OpenNebulaImageID != attachment.ImageID || decision.Evidence.OpenNebulaNodeID != attachment.NodeID {
				return fmt.Errorf("provider attachment identity changed since inventory")
			}
		}
		if decision.Action != VolumeReconcileProceed {
			if decision.Action == VolumeReconcileComplete && errors.Is(decision.Evidence.AttachedErr, opennebula.ErrAttachmentNotFound) {
				return confirmedAbsent(queueCtx)
			}
			if err := decision.Err(); err != nil {
				if decision.Reason == queueReasonSameNodeReuseRequired || decision.Reason == queueReasonRecoveryModeManual || strings.HasPrefix(decision.Reason, "local_") {
					return fmt.Errorf("%w: %v", errAttachmentSkipped, err)
				}
				return err
			}
			return errAttachmentSkipped
		}

		if r.server.driver.stickyAttachments != nil {
			if _, ok := r.server.driver.stickyAttachments.Get(attachment.VolumeHandle); ok {
				return errAttachmentSkipped
			}
		}
		if r.server.driver.hotplugGuard != nil {
			if _, ok := r.server.hotplugCooldownState(queueCtx, attachment.NodeName); ok {
				return errAttachmentSkipped
			}
		}
		if _, err := r.server.volumeProvider.GetVolumeInNode(queueCtx, attachment.ImageID, attachment.NodeID); err != nil {
			if errors.Is(err, opennebula.ErrAttachmentNotFound) {
				return confirmedAbsent(queueCtx)
			}
			return err
		}
		detachStarted := time.Now()
		accepted, err := r.server.detachVolumeWithAuthority(queueCtx, attachment.VolumeHandle, attachment.NodeName,
			physicalDetachIdentity{pvUID: pvUID, nodeUID: nodeUID, imageID: attachment.ImageID, vmID: attachment.NodeID, diskID: attachment.DiskID, background: true, backend: backend, cephSingleWriter: cephSingleWriter})
		if err != nil {
			r.server.handleHotplugTimeout(queueCtx, attachment.NodeName, attachment.VolumeHandle, nil, "detach", observedAttachmentBackend(attachment), err)
			return err
		}
		if !accepted {
			return errAttachmentAbsent
		}
		detached = true
		r.server.clearHotplugGuardState(queueCtx, attachment.NodeName)
		if sizeBytes, sizeErr := r.server.volumeProvider.ResolveVolumeSizeBytes(queueCtx, attachment.VolumeHandle); sizeErr == nil {
			r.server.driver.observeAdaptiveTimeout(queueCtx, "detach", observedAttachmentBackend(attachment), sizeBytes, time.Since(detachStarted))
		}
		return nil
	})
	if err == nil && !detached {
		if _, observedErr := r.server.volumeProvider.GetVolumeInNode(ctx, attachment.ImageID, attachment.NodeID); observedErr != nil {
			if errors.Is(observedErr, opennebula.ErrAttachmentNotFound) {
				if err := r.server.verifyDetachAbsenceTarget(ctx, attachment.VolumeHandle, attachment.NodeName, attachment.ImageID, attachment.NodeID, nodeUID); err != nil {
					return err
				}
				return errAttachmentAbsent
			}
			return observedErr
		}
		return errAttachmentSkipped
	}
	return err
}

func (r *AttachmentReconciler) recordPVCEventForPV(ctx context.Context, pv *corev1.PersistentVolume, reason, message string) {
	if claim := pv.Spec.ClaimRef; claim != nil {
		r.server.driver.kubeRuntime.EmitPVCEvent(ctx, claim.Namespace, claim.Name, reason, message, claim.UID)
	}
}

func (r *AttachmentReconciler) due(store map[string]time.Time, key string, now time.Time, grace time.Duration) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	firstSeen, ok := store[key]
	if !ok {
		store[key] = now
		return false
	}
	return now.Sub(firstSeen) >= grace
}

func (r *AttachmentReconciler) prune(store map[string]time.Time, current map[string]struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range store {
		if _, ok := current[key]; !ok {
			delete(store, key)
		}
	}
}

func observedAttachmentBackend(attachment opennebula.ObservedAttachment) string {
	if strings.TrimSpace(attachment.Backend) != "" {
		return strings.TrimSpace(attachment.Backend)
	}
	return "disk"
}

var errAttachmentSkipped = errors.New("attachment cleanup skipped")
var errAttachmentAbsent = errors.New("attachment already authoritatively absent")

func attachmentCleanupOutcome(err error) string {
	if errors.Is(err, errAttachmentSkipped) {
		return "skipped"
	}
	if errors.Is(err, errAttachmentAbsent) {
		return "absent"
	}
	return "unknown_or_failed"
}

func (r *AttachmentReconciler) deleteStaleVolumeAttachment(ctx context.Context, pv *corev1.PersistentVolume, va *storagev1.VolumeAttachment) error {
	if r.server.driver.operationLocks == nil || pv.UID == "" || va.UID == "" || va.ResourceVersion == "" {
		return fmt.Errorf("stale attachment identity is incomplete")
	}
	nodeRelease := r.server.driver.operationLocks.Acquire(controllerNodeLockKey(va.Spec.NodeName))
	defer nodeRelease()
	volume := pv.Spec.CSI.VolumeHandle
	volumeRelease := r.server.driver.operationLocks.Acquire(controllerVolumeLockKey(volume))
	defer volumeRelease()
	client := r.server.driver.kubeRuntime.client
	checkIdentity := func() error {
		currentPV, err := client.CoreV1().PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		currentVA, err := client.StorageV1().VolumeAttachments().Get(ctx, va.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if currentPV.UID != pv.UID || currentPV.ResourceVersion != pv.ResourceVersion || !reflect.DeepEqual(currentPV.Spec, pv.Spec) ||
			currentVA.UID != va.UID || currentVA.ResourceVersion != va.ResourceVersion || !reflect.DeepEqual(currentVA.Spec, va.Spec) ||
			!currentVA.Status.Attached || currentVA.Spec.Attacher != r.server.driver.name {
			return fmt.Errorf("stale attachment identity changed")
		}
		if r.skipDiskAttachmentReconcile(ctx, volume, currentPV) {
			return errAttachmentSkipped
		}
		return nil
	}
	if err := checkIdentity(); err != nil {
		return err
	}
	history, err := r.server.readAttachmentHistory(ctx, volume)
	if err != nil {
		return err
	}
	if history.PendingReplacementAttach != nil {
		return errAttachmentSkipped
	}
	imageID, _, err := r.server.volumeProvider.VolumeExists(ctx, volume)
	if err != nil || imageID < 0 {
		return fmt.Errorf("stale attachment image identity is unknown: %v", err)
	}
	if history.LastSuccessfulImageID > 0 && history.LastSuccessfulImageID != imageID {
		return fmt.Errorf("stale attachment image differs from history")
	}
	backend, err := persistentVolumeBackend(pv)
	if err != nil && !errors.Is(err, errAttachmentBackendUndeclared) {
		return err
	}
	datastore, err := r.server.inspectVolumeDatastore(ctx, volume, imageID)
	if err != nil {
		return err
	}
	if backend != "" && backend != datastore.Backend || history.Backend != "" && !strings.EqualFold(history.Backend, datastore.Backend) {
		return fmt.Errorf("stale attachment backend differs from current or historical declarations")
	}
	if isCephBackend(datastore.Backend) {
		if err := validatePVImageAssertions(pv.Spec.CSI.VolumeAttributes, imageID); err != nil {
			return err
		}
	}
	if err := validateAttachmentImageAssertions(va.Status.AttachmentMetadata, imageID); err != nil {
		return err
	}
	identity, err := r.server.driver.kubeRuntime.NodeIdentity(ctx, va.Spec.NodeName)
	if err != nil || !identity.Exists || identity.UID == "" {
		return fmt.Errorf("stale attachment Kubernetes node identity is unknown: %v", err)
	}
	nodeID, err := r.server.volumeProvider.NodeExists(ctx, va.Spec.NodeName)
	if err != nil || nodeID <= 0 {
		return fmt.Errorf("stale attachment node identity is unknown: %v", err)
	}
	if err := checkIdentity(); err != nil {
		return err
	}
	handled, err := r.server.checkReplacedNodeStaleAttachment(ctx, volume, va.Spec.NodeName, imageID, nodeID, history, va)
	if err != nil {
		return err
	}
	if !handled {
		if _, err := r.server.volumeProvider.GetVolumeInNode(ctx, imageID, nodeID); !errors.Is(err, opennebula.ErrAttachmentNotFound) {
			return fmt.Errorf("stale attachment absence is unverified: %v", err)
		}
		if err := checkIdentity(); err != nil {
			return err
		}
		if err := r.server.verifyDetachAbsenceTarget(ctx, volume, va.Spec.NodeName, imageID, nodeID, types.UID(identity.UID)); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	uid, version := va.UID, va.ResourceVersion
	return client.StorageV1().VolumeAttachments().Delete(ctx, va.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}})
}
