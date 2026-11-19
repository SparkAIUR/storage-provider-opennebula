package driver

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	inventoryv1alpha1 "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/apis/storageprovider/v1alpha1"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	paramPVCName      = "csi.storage.k8s.io/pvc/name"
	paramPVCNamespace = "csi.storage.k8s.io/pvc/namespace"
	paramPVCUID       = "csi.storage.k8s.io/pvc/uid"
	paramPVName       = "csi.storage.k8s.io/pv/name"

	annotationBackend               = "storage-provider.opennebula.sparkaiur.io/backend"
	annotationDatastoreID           = "storage-provider.opennebula.sparkaiur.io/datastore-id"
	annotationDatastoreName         = "storage-provider.opennebula.sparkaiur.io/datastore-name"
	annotationSelectionPolicy       = "storage-provider.opennebula.sparkaiur.io/selection-policy"
	annotationPlacementSummary      = "storage-provider.opennebula.sparkaiur.io/placement-summary"
	annotationLastAttachedNode      = "storage-provider.opennebula.sparkaiur.io/last-attached-node"
	annotationRequiredNode          = "storage-provider.opennebula.sparkaiur.io/required-node"
	annotationRequiredNodeUntil     = "storage-provider.opennebula.sparkaiur.io/required-node-until"
	annotationPreferredNode         = "storage-provider.opennebula.sparkaiur.io/preferred-node"
	annotationPlacementReason       = "storage-provider.opennebula.sparkaiur.io/placement-reason"
	annotationRecoveryMode          = "storage-provider.opennebula.sparkaiur.io/recovery-mode"
	annotationRecoveryModeUntil     = "storage-provider.opennebula.sparkaiur.io/recovery-mode-until"
	annotationRecoveryTicket        = "storage-provider.opennebula.sparkaiur.io/recovery-ticket"
	annotationAdoptAttachedDevice   = "storage-provider.opennebula.sparkaiur.io/adopt-attached-device"
	annotationConfirmedDeviceSerial = "storage-provider.opennebula.sparkaiur.io/confirmed-device-serial"
	annotationConfirmedVolumeName   = "storage-provider.opennebula.sparkaiur.io/confirmed-volume-name"
	annotationRestartOpt            = "storage-provider.opennebula.sparkaiur.io/restart-optimization"
	annotationDetachGrace           = "storage-provider.opennebula.sparkaiur.io/detach-grace-seconds"
	annotationLastNodePref          = "storage-provider.opennebula.sparkaiur.io/last-node-preference"
	annotationPreferredLastNode     = "storage-provider.opennebula.sparkaiur.io/preferred-last-node"
	annotationAllowCrossNodeUntil   = "storage-provider.opennebula.sparkaiur.io/allow-cross-node-until"
	annotationRequiredNodeInjected  = "storage-provider.opennebula.sparkaiur.io/required-node-injected"
	annotationPreferredNodeInjected = "storage-provider.opennebula.sparkaiur.io/preferred-node-injected"
	annotationPlacementSource       = "storage-provider.opennebula.sparkaiur.io/placement-source"
	annotationPlacementDecision     = "storage-provider.opennebula.sparkaiur.io/placement-decision"
	annotationLastNodeInjected      = "storage-provider.opennebula.sparkaiur.io/last-node-preference-injected"
	annotationSystemDSAffinity      = "storage-provider.opennebula.sparkaiur.io/system-ds-affinity"
	annotationSystemDSInjected      = "storage-provider.opennebula.sparkaiur.io/system-ds-affinity-injected"
	annotationSelectedNode          = "volume.kubernetes.io/selected-node"

	annotationLegacyLastAttachedNode = "csi.opennebula.io/last-attached-node"

	restartOptimizationAnnotationValue = "sticky-local-restart-v1"
	lastNodePreferenceDisabledValue    = "disabled"

	topologySystemDSLabel = "topology.opennebula.sparkaiur.io/system-ds"
)

type PlacementReport struct {
	Backend                     string `json:"backend"`
	DatastoreID                 int    `json:"datastoreID"`
	DatastoreName               string `json:"datastoreName"`
	SelectionPolicy             string `json:"selectionPolicy"`
	FallbackUsed                bool   `json:"fallbackUsed"`
	CompatibilityAwareSelection bool   `json:"compatibilityAwareSelection"`
	LastAttachedNode            string `json:"lastAttachedNode,omitempty"`
	RestartOptimization         string `json:"restartOptimization,omitempty"`
	DetachGraceSeconds          int    `json:"detachGraceSeconds,omitempty"`
}

type VolumeRuntimeContext struct {
	PVName               string
	pvUID                types.UID
	pvClaimRef           *corev1.ObjectReference
	pvPhase              corev1.PersistentVolumePhase
	pvDeleting           bool
	pvcPhase             corev1.PersistentVolumeClaimPhase
	pvcDeleting          bool
	PVCNamespace         string
	PVCName              string
	PVCUID               types.UID
	verifiedClaimBinding bool
	PVAnnotations        map[string]string
	PVCAnnotations       map[string]string
	AccessModes          []corev1.PersistentVolumeAccessMode
	Backend              string
	backendAttribute     string
	DatastoreID          int
	RestartMode          string
	DetachGraceHint      int
}

type KubeRuntime struct {
	driverName      string
	client          kubernetes.Interface
	inventoryClient ctrlclient.Client
	recorder        record.EventRecorder
	enabled         bool
	staleVAGrace    time.Duration

	inventoryNodeCacheMu sync.Mutex
	inventoryNodeCache   map[string]inventoryNodeCacheEntry
}

type inventoryNodeCacheEntry struct {
	node      *inventoryv1alpha1.OpenNebulaNode
	expiresAt time.Time
}

const hotplugStateConfigMapName = "opennebula-csi-hotplug-state"

type KubernetesNodeHealth struct {
	Ready         bool
	Unschedulable bool
}

func NewKubeRuntime(component string, pluginConfig config.CSIPluginConfig) *KubeRuntime {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		klog.V(2).InfoS("Kubernetes runtime disabled", "reason", "not running in cluster", "err", err)
		return &KubeRuntime{}
	}
	config.ApplyKubeAPIClientRateLimit(cfg, pluginConfig)

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Warningf("failed to initialize Kubernetes client for runtime integrations: %v", err)
		return &KubeRuntime{}
	}
	inventoryClient := newInventoryRuntimeClient(cfg)

	broadcaster := record.NewBroadcaster()
	broadcaster.StartStructuredLogging(0)
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	recorder := broadcaster.NewRecorder(clientgoscheme.Scheme, corev1.EventSource{Component: component})

	return &KubeRuntime{
		driverName:      component,
		client:          client,
		inventoryClient: inventoryClient,
		recorder:        recorder,
		enabled:         true,
		staleVAGrace:    90 * time.Second,
	}
}

func newInventoryRuntimeClient(cfg *rest.Config) ctrlclient.Client {
	scheme := kruntime.NewScheme()
	if err := inventoryv1alpha1.AddToScheme(scheme); err != nil {
		klog.V(2).InfoS("Inventory runtime client disabled", "err", err)
		return nil
	}
	client, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		klog.V(2).InfoS("Inventory runtime client disabled", "err", err)
		return nil
	}
	return client
}

func (r *KubeRuntime) EmitPVCEvent(ctx context.Context, namespace, name, reason, message string, expectedUID ...types.UID) {
	r.emitPVCEvent(ctx, namespace, name, corev1.EventTypeNormal, reason, message, expectedUID...)
}

func (r *KubeRuntime) emitPVCEvent(ctx context.Context, namespace, name, eventType, reason, message string, expectedUID ...types.UID) {
	if r == nil || !r.enabled || namespace == "" || name == "" {
		return
	}
	if len(expectedUID) != 0 && expectedUID[0] == "" {
		return
	}
	pvc, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		klog.V(2).InfoS("Skipping PVC event emission", "namespace", namespace, "name", name, "reason", reason, "err", err)
		return
	}
	if len(expectedUID) != 0 && pvc.UID != expectedUID[0] {
		return
	}
	r.emitNamespacedEvent(ctx, pvc.Namespace, pvc.Name, pvc.UID, "PersistentVolumeClaim", eventType, reason, message)
}

func (r *KubeRuntime) EmitPVEvent(ctx context.Context, name, reason, message string) {
	if r == nil || !r.enabled || name == "" {
		return
	}
	pv, err := r.client.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		klog.V(2).InfoS("Skipping PV event emission", "name", name, "reason", reason, "err", err)
		return
	}
	r.recorder.Eventf(pv, corev1.EventTypeNormal, reason, "%s", message)
}

func (r *KubeRuntime) EmitWarningEventOnPVC(ctx context.Context, namespace, name, reason, message string, expectedUID ...types.UID) {
	r.emitPVCEvent(ctx, namespace, name, corev1.EventTypeWarning, reason, message, expectedUID...)
}

func (r *KubeRuntime) AnnotatePVAsync(ctx context.Context, pvName string, report PlacementReport) {
	if r == nil || !r.enabled || pvName == "" {
		return
	}

	go func() {
		pollCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err := wait.PollUntilContextTimeout(pollCtx, time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := r.client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}

			payload, err := buildPVAnnotationPatch(report)
			if err != nil {
				return false, err
			}

			_, err = r.client.CoreV1().PersistentVolumes().Patch(ctx, pvName, types.MergePatchType, payload, metav1.PatchOptions{})
			if err != nil {
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			klog.V(2).InfoS("Failed to annotate PV", "pvName", pvName, "err", err)
		}
	}()
}

func (r *KubeRuntime) annotatePublishedPVAsync(binding *VolumeRuntimeContext, driver, volume string, report PlacementReport) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := r.annotatePublishedPV(ctx, binding, driver, volume, report); err != nil {
			klog.V(2).InfoS("Failed to annotate published PV", "pvName", binding.PVName, "err", err)
		}
	}()
}

func (r *KubeRuntime) annotatePublishedPV(ctx context.Context, binding *VolumeRuntimeContext, driver, volume string, report PlacementReport) error {
	if binding.pvUID == "" || binding.pvDeleting {
		return nil
	}
	pvs := r.client.CoreV1().PersistentVolumes()
	pv, err := pvs.Get(ctx, binding.PVName, metav1.GetOptions{})
	if err != nil || !publishPVMatches(pv, binding, driver, volume) ||
		pv.Annotations[annotationPlacementSummary] != binding.PVAnnotations[annotationPlacementSummary] {
		return err
	}
	if binding.verifiedClaimBinding {
		pvc, reused, err := r.attachmentClaim(ctx, pv)
		if err != nil || reused || pvc == nil || pvc.UID != binding.PVCUID || pvc.Status.Phase != binding.pvcPhase ||
			pvc.DeletionTimestamp != nil || !samePublishAnnotations(binding.PVCAnnotations, pvc.Annotations) {
			return err
		}
	}
	updates, err := placementAnnotations(report)
	if err != nil {
		return err
	}
	annotations := cloneStringMap(pv.Annotations)
	for key, value := range updates {
		annotations[key] = value
	}
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": pv.UID},
		{"op": "test", "path": "/spec/csi/driver", "value": driver},
		{"op": "test", "path": "/spec/csi/volumeHandle", "value": volume},
	}
	if pv.ResourceVersion != "" {
		patch = append(patch, map[string]any{"op": "test", "path": "/metadata/resourceVersion", "value": pv.ResourceVersion})
	}
	if len(pv.Annotations) != 0 {
		patch = append(patch, map[string]any{"op": "test", "path": "/metadata/annotations", "value": pv.Annotations})
	} else if pv.ResourceVersion == "" {
		return fmt.Errorf("PV annotation update requires resourceVersion when annotations are absent")
	}
	if pv.Spec.ClaimRef != nil {
		patch = append(patch, map[string]any{"op": "test", "path": "/spec/claimRef", "value": pv.Spec.ClaimRef})
	}
	patch = append(patch, map[string]any{"op": "add", "path": "/metadata/annotations", "value": annotations})
	payload, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = pvs.Patch(ctx, pv.Name, types.JSONPatchType, payload, metav1.PatchOptions{})
	return err
}

func (r *KubeRuntime) consumePublishGrant(ctx context.Context, binding *VolumeRuntimeContext, driver, volume string) (*VolumeRuntimeContext, bool, error) {
	pvs := r.client.CoreV1().PersistentVolumes()
	admittedGrant := binding.PVAnnotations[annotationAllowCrossNodeUntil]
	current := *binding
	current.PVAnnotations = cloneStringMap(binding.PVAnnotations)
	consumed := false
	err := retryOnConflict(retry.DefaultRetry, func() error {
		pv, err := pvs.Get(ctx, binding.PVName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		grant := pv.Annotations[annotationAllowCrossNodeUntil]
		current.PVAnnotations[annotationAllowCrossNodeUntil] = grant
		if !publishPVMatches(pv, &current, driver, volume) {
			return attachmentEvidenceUnavailable("Publish PV authority changed during grant consumption", nil)
		}
		pvc, reused, err := r.attachmentClaim(ctx, pv)
		if err != nil {
			return err
		}
		if reused || pvc == nil || pvc.UID != binding.PVCUID || pvc.Status.Phase != binding.pvcPhase ||
			pvc.DeletionTimestamp != nil || !samePublishAnnotations(binding.PVCAnnotations, pvc.Annotations) {
			return attachmentEvidenceUnavailable("Publish claim authority changed during grant consumption", nil)
		}
		if grant != admittedGrant {
			return nil
		}
		delete(pv.Annotations, annotationAllowCrossNodeUntil)
		_, err = pvs.Update(ctx, pv, metav1.UpdateOptions{})
		if err == nil {
			consumed = true
			delete(current.PVAnnotations, annotationAllowCrossNodeUntil)
		}
		return err
	})
	return &current, consumed, err
}

func (r *KubeRuntime) GetNodeLabel(ctx context.Context, nodeName, labelKey string) (string, error) {
	if r == nil || !r.enabled {
		return "", fmt.Errorf("kubernetes runtime is not enabled")
	}
	if strings.TrimSpace(nodeName) == "" {
		return "", fmt.Errorf("node name is required")
	}
	if strings.TrimSpace(labelKey) == "" {
		return "", fmt.Errorf("label key is required")
	}

	node, err := r.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(node.Labels[labelKey]), nil
}

func (r *KubeRuntime) GetNodeHostname(ctx context.Context, nodeName string) (string, error) {
	value, err := r.GetNodeLabel(ctx, nodeName, corev1.LabelHostname)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value), nil
	}
	return strings.TrimSpace(nodeName), nil
}

func (r *KubeRuntime) UpsertConfigMapData(ctx context.Context, namespace, name string, data map[string]string) error {
	if r == nil || !r.enabled {
		return fmt.Errorf("kubernetes runtime is not enabled")
	}
	if namespace == "" {
		namespace = namespaceFromServiceAccount()
	}
	cmClient := r.client.CoreV1().ConfigMaps(namespace)
	payload, err := json.Marshal(map[string]any{"data": data})
	if err != nil {
		return err
	}
	_, err = cmClient.Patch(ctx, name, types.MergePatchType, payload, metav1.PatchOptions{})
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}
	_, err = cmClient.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data:       data,
	}, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		_, err = cmClient.Patch(ctx, name, types.MergePatchType, payload, metav1.PatchOptions{})
	}
	return err
}

// Snapshot writes carry the fetched resourceVersion, including deletion. A delayed
// request that outlives its timeout cannot overwrite a newer successful snapshot.
func (r *KubeRuntime) setConfigMapSnapshot(ctx context.Context, namespace, name, key, payload string) error {
	client := r.client.CoreV1().ConfigMaps(namespace)
	keyHash := sha256.Sum256([]byte(key))
	annotations := map[string]string{fmt.Sprintf("storage-provider.opennebula.sparkaiur.io/snapshot-%x", keyHash[:16]): uuid.NewString()}
	result := retryOnConflict(retry.DefaultRetry, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		cm, err := client.Get(ctx, name, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			data := map[string]string{}
			if payload != "" {
				data[key] = payload
			}
			_, err = client.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Annotations: annotations}, Data: data}, metav1.CreateOptions{})
			if errors.IsAlreadyExists(err) {
				return errors.NewConflict(corev1.Resource("configmaps"), name, err)
			}
			return err
		}
		if err != nil {
			return err
		}
		var value any
		if payload != "" {
			value = payload
		}
		patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"resourceVersion": cm.ResourceVersion, "annotations": annotations}, "data": map[string]any{key: value}})
		if err != nil {
			return err
		}
		_, err = client.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
		return err
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	return result
}

func (r *KubeRuntime) DeleteConfigMapKey(ctx context.Context, namespace, name, key string) error {
	if r == nil || !r.enabled {
		return fmt.Errorf("kubernetes runtime is not enabled")
	}
	if namespace == "" {
		namespace = namespaceFromServiceAccount()
	}
	cmClient := r.client.CoreV1().ConfigMaps(namespace)
	payload, err := json.Marshal(map[string]any{
		"data": map[string]any{key: nil},
	})
	if err != nil {
		return err
	}
	_, err = cmClient.Patch(ctx, name, types.MergePatchType, payload, metav1.PatchOptions{})
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

func (r *KubeRuntime) deleteConfigMapKeyIfCurrent(ctx context.Context, observed *corev1.ConfigMap, key, value string) error {
	client := r.client.CoreV1().ConfigMaps(observed.Namespace)
	return retryOnConflict(retry.DefaultRetry, func() error {
		current, err := client.Get(ctx, observed.Name, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if raw, present := current.Data[key]; current.UID != observed.UID || !present || raw != value {
			return nil
		}
		delete(current.Data, key)
		_, err = client.Update(ctx, current, metav1.UpdateOptions{})
		return err
	})
}

func (r *KubeRuntime) DeletePVAnnotation(ctx context.Context, pvName, key string) error {
	if r == nil || !r.enabled {
		return fmt.Errorf("kubernetes runtime is not enabled")
	}
	if strings.TrimSpace(pvName) == "" || strings.TrimSpace(key) == "" {
		return nil
	}
	return retryOnConflict(retry.DefaultRetry, func() error {
		pv, err := r.client.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if len(pv.Annotations) == 0 {
			return nil
		}
		if _, ok := pv.Annotations[key]; !ok {
			return nil
		}
		delete(pv.Annotations, key)
		_, err = r.client.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		return err
	})
}

func (r *KubeRuntime) GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	if r == nil || !r.enabled {
		return nil, fmt.Errorf("kubernetes runtime is not enabled")
	}
	if namespace == "" {
		namespace = namespaceFromServiceAccount()
	}
	return r.client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (r *KubeRuntime) attachmentClaim(ctx context.Context, pv *corev1.PersistentVolume) (*corev1.PersistentVolumeClaim, bool, error) {
	claim := pv.Spec.ClaimRef
	if claim == nil {
		return nil, false, nil
	}
	if claim.UID == "" {
		return nil, false, fmt.Errorf("PV claim UID is unavailable")
	}
	pvc, err := r.client.CoreV1().PersistentVolumeClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if pvc.UID != "" && pvc.UID != claim.UID && pv.Status.Phase == corev1.VolumeReleased && pvc.Spec.VolumeName != pv.Name {
		return pvc, true, nil
	}
	if pvc.UID != claim.UID || pvc.Spec.VolumeName != pv.Name {
		return nil, false, fmt.Errorf("PV claim identity is unverified")
	}
	return pvc, false, nil
}

func (r *KubeRuntime) verifyReleasedClaimNameReuse(ctx context.Context, original *corev1.PersistentVolume, reused *corev1.PersistentVolumeClaim, readback bool) error {
	if original.Status.Phase != corev1.VolumeReleased {
		return attachmentEvidenceUnavailable("reused claim name has no verified independent binding", nil)
	}
	if readback && pendingReplacementClaim(original, reused) {
		return ctx.Err()
	}
	return r.verifyIndependentClaimBinding(ctx, original, reused)
}

func pendingReplacementClaim(original *corev1.PersistentVolume, reused *corev1.PersistentVolumeClaim) bool {
	claim := original.Spec.ClaimRef
	return claim != nil && claim.UID != "" && reused.UID != "" && reused.UID != claim.UID &&
		reused.Name == claim.Name && reused.Namespace == claim.Namespace &&
		reused.Status.Phase == corev1.ClaimPending && reused.Spec.VolumeName != original.Name
}

func (r *KubeRuntime) verifyIndependentClaimBinding(ctx context.Context, original *corev1.PersistentVolume, reused *corev1.PersistentVolumeClaim) error {
	if reused.UID == "" || reused.Status.Phase != corev1.ClaimBound ||
		reused.Spec.VolumeName == "" || reused.Spec.VolumeName == original.Name {
		return attachmentEvidenceUnavailable("reused claim name has no verified independent binding", nil)
	}
	other, err := r.client.CoreV1().PersistentVolumes().Get(ctx, reused.Spec.VolumeName, metav1.GetOptions{})
	if err != nil || other.Status.Phase != corev1.VolumeBound || other.DeletionTimestamp != nil || other.UID == "" || other.UID == original.UID ||
		other.Spec.ClaimRef == nil || other.Spec.ClaimRef.UID != reused.UID ||
		other.Spec.ClaimRef.Namespace != reused.Namespace || other.Spec.ClaimRef.Name != reused.Name ||
		(other.Spec.CSI != nil && other.Spec.CSI.Driver == original.Spec.CSI.Driver && other.Spec.CSI.VolumeHandle == original.Spec.CSI.VolumeHandle) {
		return attachmentEvidenceUnavailable("reused claim name has no verified independent binding", err)
	}
	return ctx.Err()
}

func (r *KubeRuntime) ResolveVolumeRuntimeContext(ctx context.Context, volumeHandle string) (*VolumeRuntimeContext, error) {
	if r == nil || !r.enabled {
		return nil, fmt.Errorf("kubernetes runtime is not enabled")
	}
	if strings.TrimSpace(volumeHandle) == "" {
		return nil, fmt.Errorf("volume handle is required")
	}

	pvs, err := r.client.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	driverName := firstNonEmpty(r.driverName, DefaultDriverName)
	matches := 0
	for _, pv := range pvs.Items {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == driverName && pv.Spec.CSI.VolumeHandle == volumeHandle {
			matches++
		}
	}
	if matches > 1 {
		return nil, fmt.Errorf("multiple owned persistent volumes for handle %s", volumeHandle)
	}
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != driverName || pv.Spec.CSI.VolumeHandle != volumeHandle {
			continue
		}

		return r.volumeRuntimeContext(ctx, pv), nil
	}

	return nil, fmt.Errorf("persistent volume for handle %s not found", volumeHandle)
}

func (r *KubeRuntime) volumeRuntimeContext(ctx context.Context, pv *corev1.PersistentVolume) *VolumeRuntimeContext {
	runtimeCtx := &VolumeRuntimeContext{
		PVName:        pv.Name,
		pvUID:         pv.UID,
		pvClaimRef:    pv.Spec.ClaimRef.DeepCopy(),
		pvPhase:       pv.Status.Phase,
		pvDeleting:    pv.DeletionTimestamp != nil,
		PVAnnotations: cloneStringMap(pv.Annotations),
		AccessModes:   append([]corev1.PersistentVolumeAccessMode(nil), pv.Spec.AccessModes...),
	}
	runtimeCtx.Backend = strings.TrimSpace(pv.Annotations[annotationBackend])
	runtimeCtx.backendAttribute = pv.Spec.CSI.VolumeAttributes[annotationBackend]
	runtimeCtx.RestartMode = strings.TrimSpace(runtimeCtx.PVAnnotations[annotationRestartOpt])
	if rawGrace := strings.TrimSpace(runtimeCtx.PVAnnotations[annotationDetachGrace]); rawGrace != "" {
		if parsed, convErr := strconv.Atoi(rawGrace); convErr == nil {
			runtimeCtx.DetachGraceHint = parsed
		}
	}
	if rawDatastoreID := strings.TrimSpace(runtimeCtx.PVAnnotations[annotationDatastoreID]); rawDatastoreID != "" {
		if parsed, convErr := strconv.Atoi(rawDatastoreID); convErr == nil {
			runtimeCtx.DatastoreID = parsed
		}
	}

	if pv.Spec.ClaimRef != nil {
		runtimeCtx.PVCNamespace = pv.Spec.ClaimRef.Namespace
		runtimeCtx.PVCName = pv.Spec.ClaimRef.Name
		pvc, pvcErr := r.client.CoreV1().PersistentVolumeClaims(runtimeCtx.PVCNamespace).Get(ctx, runtimeCtx.PVCName, metav1.GetOptions{})
		if pvcErr == nil && pv.Spec.ClaimRef.UID != "" && pvc.UID != pv.Spec.ClaimRef.UID {
			runtimeCtx.PVCNamespace = ""
			runtimeCtx.PVCName = ""
		} else if pvcErr == nil {
			runtimeCtx.PVCUID = pv.Spec.ClaimRef.UID
			runtimeCtx.pvcPhase, runtimeCtx.pvcDeleting = pvc.Status.Phase, pvc.DeletionTimestamp != nil
			runtimeCtx.verifiedClaimBinding = runtimeCtx.PVCUID != "" && pvc.Spec.VolumeName == pv.Name
			runtimeCtx.PVCAnnotations = cloneStringMap(pvc.Annotations)
			if runtimeCtx.RestartMode == "" {
				runtimeCtx.RestartMode = strings.TrimSpace(runtimeCtx.PVCAnnotations[annotationRestartOpt])
			}
			if runtimeCtx.DetachGraceHint == 0 {
				if rawGrace := strings.TrimSpace(runtimeCtx.PVCAnnotations[annotationDetachGrace]); rawGrace != "" {
					if parsed, convErr := strconv.Atoi(rawGrace); convErr == nil {
						runtimeCtx.DetachGraceHint = parsed
					}
				}
			}
		}
	}
	return runtimeCtx
}

func (r *KubeRuntime) PodUIDExists(ctx context.Context, uid string) (bool, error) {
	if r == nil || !r.enabled {
		return false, fmt.Errorf("kubernetes runtime is not enabled")
	}
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return false, fmt.Errorf("pod UID is required")
	}
	pods, err := r.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, pod := range pods.Items {
		if string(pod.UID) == uid {
			return true, nil
		}
	}
	return false, nil
}

func (r *KubeRuntime) IsNodeReady(ctx context.Context, nodeName string) (bool, error) {
	health, err := r.NodeHealth(ctx, nodeName)
	if err != nil {
		return false, err
	}
	return health.Ready, nil
}

func (r *KubeRuntime) NodeHealth(ctx context.Context, nodeName string) (KubernetesNodeHealth, error) {
	if r == nil || !r.enabled {
		return KubernetesNodeHealth{}, fmt.Errorf("kubernetes runtime is not enabled")
	}
	node, err := r.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return KubernetesNodeHealth{}, err
	}
	health := KubernetesNodeHealth{Unschedulable: node.Spec.Unschedulable}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			health.Ready = condition.Status == corev1.ConditionTrue
			return health, nil
		}
	}
	return health, nil
}

func (r *KubeRuntime) OpenNebulaNode(ctx context.Context, nodeName string) (*inventoryv1alpha1.OpenNebulaNode, error) {
	if r == nil || !r.enabled || r.inventoryClient == nil {
		return nil, fmt.Errorf("inventory runtime is not enabled")
	}
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return nil, fmt.Errorf("node name is required")
	}
	now := time.Now().UTC()
	r.inventoryNodeCacheMu.Lock()
	if entry, ok := r.inventoryNodeCache[nodeName]; ok && entry.expiresAt.After(now) && entry.node != nil {
		cached := entry.node.DeepCopy()
		r.inventoryNodeCacheMu.Unlock()
		return cached, nil
	}
	r.inventoryNodeCacheMu.Unlock()

	node := &inventoryv1alpha1.OpenNebulaNode{}
	if err := r.inventoryClient.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		return nil, err
	}

	r.inventoryNodeCacheMu.Lock()
	if r.inventoryNodeCache == nil {
		r.inventoryNodeCache = map[string]inventoryNodeCacheEntry{}
	}
	r.inventoryNodeCache[nodeName] = inventoryNodeCacheEntry{
		node:      node.DeepCopy(),
		expiresAt: now.Add(5 * time.Second),
	}
	r.inventoryNodeCacheMu.Unlock()
	return node, nil
}

func (r *KubeRuntime) OpenNebulaDatastoreByID(ctx context.Context, datastoreID int) (*inventoryv1alpha1.OpenNebulaDatastore, error) {
	if r == nil || !r.enabled || r.inventoryClient == nil {
		return nil, fmt.Errorf("inventory runtime is not enabled")
	}
	if datastoreID <= 0 {
		return nil, fmt.Errorf("datastore id is required")
	}

	var list inventoryv1alpha1.OpenNebulaDatastoreList
	if err := r.inventoryClient.List(ctx, &list); err != nil {
		return nil, err
	}
	for i := range list.Items {
		item := &list.Items[i]
		if item.Status.ID == datastoreID || item.Spec.Discovery.OpenNebulaDatastoreID == datastoreID {
			return item.DeepCopy(), nil
		}
	}
	return nil, fmt.Errorf("opennebula datastore %d was not found in inventory", datastoreID)
}

func (r *KubeRuntime) VolumeDesiredOnNode(ctx context.Context, volumeHandle, nodeName string) (bool, string, error) {
	if r == nil || !r.enabled {
		return false, "", fmt.Errorf("kubernetes runtime is not enabled")
	}
	nodeName = strings.TrimSpace(nodeName)
	if strings.TrimSpace(volumeHandle) == "" || nodeName == "" {
		return false, "", nil
	}
	state, err := r.DesiredNodeForVolume(ctx, volumeHandle)
	if err != nil {
		return false, "", err
	}
	if !state.Desired {
		return false, state.DemandSourceOrReason(), nil
	}
	if state.MultipleNodes {
		for _, referencedNode := range state.ReferencedNodes {
			if strings.TrimSpace(referencedNode) == nodeName {
				return true, state.DemandSourceOrReason(), nil
			}
		}
		return false, state.DemandSourceOrReason(), nil
	}
	if strings.TrimSpace(state.NodeName) == nodeName {
		return true, state.DemandSourceOrReason(), nil
	}
	return false, state.DemandSourceOrReason(), nil
}

func (r *KubeRuntime) staleVolumeAttachmentGrace() time.Duration {
	if r == nil || r.staleVAGrace <= 0 {
		return 90 * time.Second
	}
	return r.staleVAGrace
}

func (r *KubeRuntime) AutomaticMaintenanceTrigger(ctx context.Context, nodeName string) (string, bool, error) {
	identity, err := r.NodeIdentity(ctx, nodeName)
	if err != nil {
		return "", false, err
	}
	if !identity.Exists {
		return "", false, nil
	}
	if identity.Unschedulable {
		draining, err := r.nodeHasTerminatingWorkloadPods(ctx, nodeName)
		if err != nil {
			return "", false, err
		}
		if draining {
			return protectionTriggerDrainInProgress, true, nil
		}
		return protectionTriggerNodeUnschedulable, true, nil
	}
	if !identity.Ready {
		return protectionTriggerNodeNotReady, true, nil
	}
	return "", false, nil
}

func (r *KubeRuntime) nodeHasTerminatingWorkloadPods(ctx context.Context, nodeName string) (bool, error) {
	if r == nil || !r.enabled {
		return false, fmt.Errorf("kubernetes runtime is not enabled")
	}
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return false, nil
	}
	pods, err := r.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + nodeName})
	if err != nil {
		return false, err
	}
	for idx := range pods.Items {
		pod := &pods.Items[idx]
		if pod.DeletionTimestamp == nil {
			continue
		}
		if podControllerKind(pod) == "DaemonSet" {
			continue
		}
		return true, nil
	}
	return false, nil
}

func podControllerKind(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller {
			return strings.TrimSpace(owner.Kind)
		}
	}
	return ""
}

func (r *KubeRuntime) emitNamespacedEvent(ctx context.Context, namespace, name string, uid types.UID, kind, eventType, reason, message string) {
	if r == nil || !r.enabled || namespace == "" || name == "" {
		return
	}

	now := metav1.Now()
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("%s-", strings.ToLower(name)),
			Namespace:    namespace,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:       kind,
			Namespace:  namespace,
			Name:       name,
			UID:        uid,
			APIVersion: "v1",
		},
		Reason:              reason,
		Message:             message,
		Source:              corev1.EventSource{Component: DefaultDriverName},
		Type:                eventType,
		FirstTimestamp:      now,
		LastTimestamp:       now,
		Count:               1,
		ReportingController: DefaultDriverName,
	}

	if _, err := r.client.CoreV1().Events(namespace).Create(ctx, event, metav1.CreateOptions{}); err != nil {
		if errors.IsAlreadyExists(err) {
			return
		}
		klog.V(2).InfoS("Failed to emit namespaced event", "namespace", namespace, "name", name, "reason", reason, "err", err)
	}
}

func placementAnnotations(report PlacementReport) (map[string]string, error) {
	summary, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal placement summary: %w", err)
	}

	annotations := map[string]string{
		annotationBackend:          report.Backend,
		annotationDatastoreID:      fmt.Sprintf("%d", report.DatastoreID),
		annotationDatastoreName:    report.DatastoreName,
		annotationSelectionPolicy:  report.SelectionPolicy,
		annotationPlacementSummary: string(summary),
	}
	if strings.TrimSpace(report.LastAttachedNode) != "" {
		annotations[annotationLastAttachedNode] = strings.TrimSpace(report.LastAttachedNode)
	}
	if strings.TrimSpace(report.RestartOptimization) != "" {
		annotations[annotationRestartOpt] = strings.TrimSpace(report.RestartOptimization)
	}
	if report.DetachGraceSeconds > 0 {
		annotations[annotationDetachGrace] = strconv.Itoa(report.DetachGraceSeconds)
	}

	return annotations, nil
}

func buildPVAnnotationPatch(report PlacementReport) ([]byte, error) {
	annotations, err := placementAnnotations(report)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"metadata": map[string]any{"annotations": annotations}})
}

func namespaceFromServiceAccount() string {
	data, err := os.ReadFile(filepath.Clean("/var/run/secrets/kubernetes.io/serviceaccount/namespace"))
	if err != nil {
		return "default"
	}
	if value := strings.TrimSpace(string(data)); value != "" {
		return value
	}
	return "default"
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
