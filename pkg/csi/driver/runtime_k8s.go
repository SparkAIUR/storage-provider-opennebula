package driver

import (
	"context"
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
	PVName          string
	PVCNamespace    string
	PVCName         string
	PVAnnotations   map[string]string
	PVCAnnotations  map[string]string
	AccessModes     []corev1.PersistentVolumeAccessMode
	Backend         string
	DatastoreID     int
	RestartMode     string
	DetachGraceHint int
}

type KubeRuntime struct {
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

func (r *KubeRuntime) EmitPVCEvent(ctx context.Context, namespace, name, reason, message string) {
	if r == nil || !r.enabled || namespace == "" || name == "" {
		return
	}
	pvc, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		klog.V(2).InfoS("Skipping PVC event emission", "namespace", namespace, "name", name, "reason", reason, "err", err)
		return
	}
	r.emitNamespacedEvent(ctx, pvc.Namespace, pvc.Name, pvc.UID, "PersistentVolumeClaim", corev1.EventTypeNormal, reason, message)
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

func (r *KubeRuntime) EmitWarningEventOnPVC(ctx context.Context, namespace, name, reason, message string) {
	if r == nil || !r.enabled || namespace == "" || name == "" {
		return
	}
	pvc, err := r.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		klog.V(2).InfoS("Skipping PVC warning emission", "namespace", namespace, "name", name, "reason", reason, "err", err)
		return
	}
	r.emitNamespacedEvent(ctx, pvc.Namespace, pvc.Name, pvc.UID, "PersistentVolumeClaim", corev1.EventTypeWarning, reason, message)
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

func (r *KubeRuntime) DeletePVAnnotation(ctx context.Context, pvName, key string) error {
	if r == nil || !r.enabled {
		return fmt.Errorf("kubernetes runtime is not enabled")
	}
	if strings.TrimSpace(pvName) == "" || strings.TrimSpace(key) == "" {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
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

	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.CSI == nil || pv.Spec.CSI.VolumeHandle != volumeHandle {
			continue
		}

		runtimeCtx := &VolumeRuntimeContext{
			PVName:        pv.Name,
			PVAnnotations: cloneStringMap(pv.Annotations),
			AccessModes:   append([]corev1.PersistentVolumeAccessMode(nil), pv.Spec.AccessModes...),
		}
		runtimeCtx.Backend = strings.TrimSpace(runtimeCtx.PVAnnotations[annotationBackend])
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
			if pvcErr == nil {
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

		return runtimeCtx, nil
	}

	return nil, fmt.Errorf("persistent volume for handle %s not found", volumeHandle)
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

func buildPVAnnotationPatch(report PlacementReport) ([]byte, error) {
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

	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": annotations,
		},
	}
	return json.Marshal(patch)
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
