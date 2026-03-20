package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

const (
	paramPVCName      = "csi.storage.k8s.io/pvc/name"
	paramPVCNamespace = "csi.storage.k8s.io/pvc/namespace"
	paramPVName       = "csi.storage.k8s.io/pv/name"

	annotationBackend          = "storage-provider.opennebula.sparkaiur.io/backend"
	annotationDatastoreID      = "storage-provider.opennebula.sparkaiur.io/datastore-id"
	annotationDatastoreName    = "storage-provider.opennebula.sparkaiur.io/datastore-name"
	annotationSelectionPolicy  = "storage-provider.opennebula.sparkaiur.io/selection-policy"
	annotationPlacementSummary = "storage-provider.opennebula.sparkaiur.io/placement-summary"

	topologySystemDSLabel = "topology.opennebula.sparkaiur.io/system-ds"
)

type PlacementReport struct {
	Backend                     string `json:"backend"`
	DatastoreID                 int    `json:"datastoreID"`
	DatastoreName               string `json:"datastoreName"`
	SelectionPolicy             string `json:"selectionPolicy"`
	FallbackUsed                bool   `json:"fallbackUsed"`
	CompatibilityAwareSelection bool   `json:"compatibilityAwareSelection"`
}

type KubeRuntime struct {
	client   kubernetes.Interface
	recorder record.EventRecorder
	enabled  bool
}

func NewKubeRuntime(component string) *KubeRuntime {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		klog.V(2).InfoS("Kubernetes runtime disabled", "reason", "not running in cluster", "err", err)
		return &KubeRuntime{}
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Warningf("failed to initialize Kubernetes client for runtime integrations: %v", err)
		return &KubeRuntime{}
	}

	broadcaster := record.NewBroadcaster()
	broadcaster.StartStructuredLogging(0)
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	recorder := broadcaster.NewRecorder(clientgoscheme.Scheme, corev1.EventSource{Component: component})

	return &KubeRuntime{
		client:   client,
		recorder: recorder,
		enabled:  true,
	}
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
		Reason:             reason,
		Message:            message,
		Source:             corev1.EventSource{Component: DefaultDriverName},
		Type:               eventType,
		FirstTimestamp:     now,
		LastTimestamp:      now,
		Count:              1,
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
