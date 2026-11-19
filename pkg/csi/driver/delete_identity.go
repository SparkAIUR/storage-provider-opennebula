package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const annotationDeleteVolumeIdentity = "storage-provider.opennebula.sparkaiur.io/delete-volume-identity"

type volumeDeletionIdentity struct {
	Version int       `json:"version"`
	PVUID   types.UID `json:"pvUID"`
	Driver  string    `json:"driver"`
	Handle  string    `json:"handle"`
	ImageID *int      `json:"imageID"`
	Backend string    `json:"backend,omitempty"`
}

func (s *ControllerServer) ownedDeletionPV(ctx context.Context, volume string) (*corev1.PersistentVolume, error) {
	runtime := s.driver.kubeRuntime
	if runtime == nil || !runtime.enabled {
		return nil, nil
	}
	pvs, err := runtime.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var owned *corev1.PersistentVolume
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if csi := pv.Spec.CSI; csi == nil || csi.Driver != s.driver.name || csi.VolumeHandle != volume {
			continue
		}
		if owned != nil || pv.UID == "" {
			return nil, fmt.Errorf("owned PV identity for deletion is ambiguous")
		}
		owned = pv
	}
	return owned, nil
}

func (s *ControllerServer) prepareVolumeDeletion(ctx context.Context, volume string) (context.Context, error) {
	if manager := s.driver.volumeHistory; manager != nil {
		if err := manager.RefreshEntry(ctx, volume); err != nil {
			return ctx, attachmentEvidenceUnavailable("deletion history is unverified", err)
		}
	}
	history := s.currentVolumeHistory(volume)
	if history.PendingReplacementAttach != nil {
		retired, err := s.retireCompletedReplacement(ctx, history)
		if err != nil {
			return ctx, err
		}
		if retired {
			history, err = s.readAttachmentHistory(ctx, volume)
			if err != nil {
				return ctx, attachmentEvidenceUnavailable("deletion history is unverified", err)
			}
		}
	}
	if history.PendingReplacementAttach != nil {
		return ctx, attachmentEvidenceUnavailable("replacement attachment holds volume deletion", nil)
	}
	pv, err := s.ownedDeletionPV(ctx, volume)
	if err != nil {
		return ctx, attachmentEvidenceUnavailable("deletion PV identity is unverified", err)
	}
	imageID := history.LastSuccessfulImageID
	var raw, admittedBackend string
	var marked bool
	if pv != nil {
		raw, marked = pv.Annotations[annotationDeleteVolumeIdentity]
	}
	if marked {
		var record volumeDeletionIdentity
		err := decodeCephRetentionProtection(raw, &record)
		if err != nil || record.Version != 1 || record.PVUID != pv.UID || record.Driver != s.driver.name ||
			(record.Backend != "" && record.Backend != "local" && record.Backend != "ceph") ||
			record.Handle != volume || record.ImageID == nil || *record.ImageID < 0 || (imageID > 0 && *record.ImageID != imageID) {
			return ctx, attachmentEvidenceUnavailable("PV deletion identity is unverified", err)
		}
		imageID, admittedBackend = *record.ImageID, record.Backend
	}
	if err := s.verifyVolumeDeletionProtection(ctx, volume, imageID, history.LastSuccessfulOpenNebulaVMID, pv != nil); err != nil {
		return ctx, err
	}
	if marked || imageID > 0 {
		ctx = opennebula.WithExpectedImageID(ctx, imageID)
	}
	if history.LastSuccessfulOpenNebulaVMID > 0 {
		ctx = opennebula.WithExpectedDeletionVMID(ctx, history.LastSuccessfulOpenNebulaVMID)
	}
	var stage func(context.Context, int) error
	var expectedVersion string
	verify := func(ctx context.Context, actualImageID int, imageAbsent bool) error {
		return s.verifyPVDeletion(ctx, pv, volume, actualImageID, history, expectedVersion, raw, marked, imageAbsent, &admittedBackend)
	}
	if pv != nil {
		expectedVersion = pv.ResourceVersion
		if !marked && history.LastSuccessfulImageID <= 0 {
			stage = func(ctx context.Context, actualImageID int) error {
				if err := verify(ctx, actualImageID, false); err != nil {
					return err
				}
				var err error
				expectedVersion, raw, err = s.markPVDeletion(ctx, pv, volume, actualImageID, admittedBackend)
				if err != nil {
					return err
				}
				marked = true
				return verify(ctx, actualImageID, false)
			}
		}
	}
	ctx = opennebula.WithVolumeDeletionAuthorization(ctx, stage, verify)
	return ctx, nil
}

func (s *ControllerServer) verifyVolumeDeletionProtection(ctx context.Context, volume string, imageID, vmID int, owned bool) error {
	if manager := s.driver.volumeQuarantine; manager != nil && manager.runtime != nil && manager.runtime.enabled {
		cm, err := manager.runtime.GetConfigMap(ctx, manager.namespace, volumeQuarantineStateConfigMapName)
		if err != nil && !apierrors.IsNotFound(err) {
			return attachmentEvidenceUnavailable("cannot inspect cleanup quarantine", err)
		}
		if err == nil {
			if _, exists := cm.Data[volume]; exists {
				return status.Error(codes.FailedPrecondition, "volume has unresolved cleanup quarantine; external verification required")
			}
		}
	}
	if owned {
		return s.attachmentProtectionChecks(ctx, volume, nil, opennebula.HostArtifactAttachmentTarget{ImageID: imageID, VMID: vmID})
	}
	return nil
}

func (s *ControllerServer) markPVDeletion(ctx context.Context, pv *corev1.PersistentVolume, volume string, imageID int, backend string) (string, string, error) {
	if imageID < 0 || pv.ResourceVersion == "" {
		return "", "", fmt.Errorf("provider image identity or PV resourceVersion is unavailable")
	}
	identity, err := json.Marshal(volumeDeletionIdentity{Version: 1, PVUID: pv.UID, Driver: s.driver.name, Handle: volume, ImageID: &imageID, Backend: backend})
	if err != nil {
		return "", "", err
	}
	raw := string(identity)
	path := "/metadata/annotations/" + strings.ReplaceAll(annotationDeleteVolumeIdentity, "/", "~1")
	var value any = raw
	if len(pv.Annotations) == 0 {
		path, value = "/metadata/annotations", map[string]string{annotationDeleteVolumeIdentity: raw}
	}
	patch, err := json.Marshal([]map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": pv.UID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": pv.ResourceVersion},
		{"op": "add", "path": path, "value": value},
	})
	if err != nil {
		return "", "", err
	}
	updated, err := s.driver.kubeRuntime.client.CoreV1().PersistentVolumes().Patch(ctx, pv.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return "", "", err
	}
	if updated.ResourceVersion == "" {
		return "", "", fmt.Errorf("PV deletion identity write returned no resourceVersion")
	}
	return updated.ResourceVersion, raw, nil
}

func (s *ControllerServer) verifyPVDeletion(ctx context.Context, pv *corev1.PersistentVolume, volume string, imageID int, history VolumeHistoryRecord, expectedVersion, raw string, marked, imageAbsent bool, admittedBackend *string) error {
	if (pv != nil && imageID < 0) || (history.LastSuccessfulImageID > 0 && history.LastSuccessfulImageID != imageID) {
		return fmt.Errorf("provider image identity is unverified for deletion")
	}
	current, err := s.ownedDeletionPV(ctx, volume)
	if err != nil {
		return err
	}
	if (pv == nil) != (current == nil) {
		return fmt.Errorf("PV deletion identity changed during admission")
	}
	if current != nil {
		currentRaw, currentMarked := current.Annotations[annotationDeleteVolumeIdentity]
		if current.Name != pv.Name || current.UID != pv.UID || current.ResourceVersion != expectedVersion ||
			!apiequality.Semantic.DeepEqual(current.Spec.ClaimRef, pv.Spec.ClaimRef) || currentMarked != marked || currentRaw != raw {
			return fmt.Errorf("PV deletion identity changed during admission")
		}
		if err := s.verifyPVImageAssertions(ctx, current, imageID, history, imageAbsent, admittedBackend); err != nil {
			return err
		}
	}
	if manager := s.driver.volumeHistory; manager != nil && manager.runtime != nil && manager.runtime.enabled {
		currentHistory, err := s.readAttachmentHistory(ctx, volume)
		if err != nil {
			return err
		}
		if currentHistory.PendingReplacementAttach != nil || currentHistory.LastSuccessfulImageID != history.LastSuccessfulImageID ||
			currentHistory.LastSuccessfulOpenNebulaVMID != history.LastSuccessfulOpenNebulaVMID ||
			currentHistory.LastSuccessfulNodeName != history.LastSuccessfulNodeName ||
			currentHistory.LastSuccessfulNodeUID != history.LastSuccessfulNodeUID || currentHistory.Backend != history.Backend ||
			!currentHistory.LastSuccessfulPublishTime.Equal(history.LastSuccessfulPublishTime) || !currentHistory.LastSuccessfulStageTime.Equal(history.LastSuccessfulStageTime) {
			return fmt.Errorf("deletion history changed during admission")
		}
	} else if pv != nil {
		return fmt.Errorf("volume history manager is unavailable")
	}
	return s.verifyVolumeDeletionProtection(ctx, volume, imageID, history.LastSuccessfulOpenNebulaVMID, pv != nil)
}
