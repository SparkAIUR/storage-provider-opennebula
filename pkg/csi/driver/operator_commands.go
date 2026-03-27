package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	inventoryv1alpha1 "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/apis/storageprovider/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type InventoryValidateOptions struct {
	DatastoreID  int
	StorageClass string
	Size         string
	FioArgs      []string
	Timeout      time.Duration
}

type inventoryValidateResult struct {
	DatastoreID int    `json:"datastoreID"`
	Name        string `json:"name"`
	Phase       string `json:"phase"`
	Summary     string `json:"summary"`
	RunNonce    string `json:"runNonce"`
}

type SupportBundle struct {
	Timestamp               time.Time                                           `json:"timestamp"`
	Config                  map[string]any                                      `json:"config"`
	FeatureGates            FeatureGates                                        `json:"featureGates"`
	ControllerLeadership    map[string]any                                      `json:"controllerLeadership"`
	HotplugCooldowns        map[string]any                                      `json:"hotplugCooldowns"`
	StickyAttachments       map[string]any                                      `json:"stickyAttachments"`
	HotplugQueue            map[string]any                                      `json:"hotplugQueue"`
	AdaptiveTimeouts        map[string]any                                      `json:"adaptiveTimeouts"`
	AdaptiveRecommendations map[string]any                                      `json:"adaptiveRecommendations"`
	Datastores              []inventoryv1alpha1.OpenNebulaDatastore             `json:"datastores"`
	BenchmarkRuns           []inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRun `json:"benchmarkRuns"`
	Nodes                   []inventoryv1alpha1.OpenNebulaNode                  `json:"nodes"`
	StorageClassAudit       []inventoryv1alpha1.StorageClassDetail              `json:"storageClassAudit"`
	VolumeAttachments       []storagev1.VolumeAttachment                        `json:"volumeAttachments"`
	Events                  []corev1.Event                                      `json:"events"`
}

func RunInventoryValidateCommand(ctx context.Context, cfg config.CSIPluginConfig, opts InventoryValidateOptions, w io.Writer) error {
	if opts.DatastoreID <= 0 {
		return fmt.Errorf("datastore ID must be provided")
	}
	if strings.TrimSpace(opts.StorageClass) == "" {
		return fmt.Errorf("storage class must be provided")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 15 * time.Minute
	}

	client, err := inventoryClient()
	if err != nil {
		return err
	}

	ds, err := findDatastoreByID(ctx, client, opts.DatastoreID)
	if err != nil {
		return err
	}

	runNonce := fmt.Sprintf("manual-%d", time.Now().UnixNano())
	runName := fmt.Sprintf("ds-%d-%d", opts.DatastoreID, time.Now().Unix())
	benchmarkRun := &inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRun{
		TypeMeta: metav1.TypeMeta{
			APIVersion: inventoryv1alpha1.SchemeGroupVersion.String(),
			Kind:       "OpenNebulaDatastoreBenchmarkRun",
		},
		ObjectMeta: metav1.ObjectMeta{Name: runName},
		Spec: inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRunSpec{
			DatastoreID:      opts.DatastoreID,
			StorageClassName: opts.StorageClass,
			Size:             opts.Size,
			FioArgs:          append([]string(nil), opts.FioArgs...),
		},
	}
	if err := client.Create(ctx, benchmarkRun); err != nil {
		return fmt.Errorf("failed to create datastore benchmark run: %w", err)
	}

	deadlineCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	for {
		current := &inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRun{}
		if err := client.Get(deadlineCtx, ctrlclient.ObjectKey{Name: runName}, current); err != nil {
			return err
		}
		switch current.Status.Phase {
		case inventoryv1alpha1.ValidationPhaseSucceeded, inventoryv1alpha1.ValidationPhaseFailed:
			result := inventoryValidateResult{
				DatastoreID: current.Status.DatastoreID,
				Name:        ds.Status.Name,
				Phase:       current.Status.Phase,
				Summary:     benchmarkSummaryForCommand(current.Status),
				RunNonce:    runNonce,
			}
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			return enc.Encode(result)
		}
		select {
		case <-deadlineCtx.Done():
			return fmt.Errorf("timed out waiting for validation run %s on datastore %d", runNonce, opts.DatastoreID)
		case <-time.After(5 * time.Second):
		}
	}
}

func RunSupportBundleCommand(ctx context.Context, cfg config.CSIPluginConfig, w io.Writer) error {
	kubeClient, _, err := preflightKubeClients()
	if err != nil {
		return err
	}
	invClient, err := inventoryClient()
	if err != nil {
		return err
	}

	var dsList inventoryv1alpha1.OpenNebulaDatastoreList
	if err := invClient.List(ctx, &dsList); err != nil {
		return err
	}
	var nodeList inventoryv1alpha1.OpenNebulaNodeList
	if err := invClient.List(ctx, &nodeList); err != nil {
		return err
	}
	var benchmarkList inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRunList
	if err := invClient.List(ctx, &benchmarkList); err != nil {
		return err
	}

	scList, err := kubeClient.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	vaList, err := kubeClient.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	eventList, err := kubeClient.CoreV1().Events("").List(ctx, metav1.ListOptions{Limit: 50})
	if err != nil {
		return err
	}
	hotplugSnapshot := map[string]any{}
	if cm, cmErr := kubeClient.CoreV1().ConfigMaps(namespaceFromServiceAccount()).Get(ctx, hotplugStateConfigMapName, metav1.GetOptions{}); cmErr == nil {
		for node, raw := range cm.Data {
			var parsed any
			if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
				hotplugSnapshot[node] = raw
				continue
			}
			hotplugSnapshot[node] = parsed
		}
	}
	stickySnapshot := configMapJSONSnapshot(ctx, kubeClient, stickyAttachmentStateConfigMapName)
	queueSnapshot := configMapJSONSnapshot(ctx, kubeClient, hotplugQueueStateConfigMapName)
	adaptiveSnapshot := configMapJSONSnapshot(ctx, kubeClient, adaptiveTimeoutObservationsConfigMapName)
	adaptiveRecommendations := adaptiveRecommendationSnapshot(cfg, adaptiveSnapshot)

	bundle := SupportBundle{
		Timestamp:    time.Now().UTC(),
		Config:       supportBundleConfig(cfg),
		FeatureGates: loadFeatureGates(cfg),
		ControllerLeadership: map[string]any{
			"enabled":        getBool(cfg, config.ControllerLeaderElectionEnabledVar),
			"leaseName":      getString(cfg, config.ControllerLeaderElectionLeaseNameVar),
			"leaseNamespace": getString(cfg, config.ControllerLeaderElectionLeaseNamespaceVar),
			"leaseDuration":  getInt(cfg, config.ControllerLeaderElectionLeaseDurationVar),
			"renewDeadline":  getInt(cfg, config.ControllerLeaderElectionRenewDeadlineVar),
			"retryPeriod":    getInt(cfg, config.ControllerLeaderElectionRetryPeriodVar),
		},
		HotplugCooldowns:        hotplugSnapshot,
		StickyAttachments:       stickySnapshot,
		HotplugQueue:            queueSnapshot,
		AdaptiveTimeouts:        adaptiveSnapshot,
		AdaptiveRecommendations: adaptiveRecommendations,
		Datastores:              dsList.Items,
		BenchmarkRuns:           benchmarkList.Items,
		Nodes:                   nodeList.Items,
		StorageClassAudit:       flattenStorageClassDetails(dsList.Items, scList.Items),
		VolumeAttachments:       vaList.Items,
		Events:                  eventList.Items,
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(bundle)
}

func inventoryClient() (ctrlclient.Client, error) {
	_, _, err := preflightKubeClients()
	if err != nil {
		return nil, err
	}
	restConfig, err := loadKubeRestConfig()
	if err != nil {
		return nil, err
	}
	scheme := runtime.NewScheme()
	if err := inventoryv1alpha1.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return ctrlclient.New(restConfig, ctrlclient.Options{Scheme: scheme})
}

func loadKubeRestConfig() (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig := strings.TrimSpace(os.Getenv("KUBECONFIG")); kubeconfig != "" {
		loadingRules.ExplicitPath = kubeconfig
	} else if home, homeErr := os.UserHomeDir(); homeErr == nil {
		loadingRules.ExplicitPath = filepath.Join(home, ".kube", "config")
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
}

func findDatastoreByID(ctx context.Context, client ctrlclient.Client, datastoreID int) (*inventoryv1alpha1.OpenNebulaDatastore, error) {
	var list inventoryv1alpha1.OpenNebulaDatastoreList
	if err := client.List(ctx, &list); err != nil {
		return nil, err
	}
	for idx := range list.Items {
		item := &list.Items[idx]
		if item.Spec.Discovery.OpenNebulaDatastoreID == datastoreID {
			return item, nil
		}
	}
	return nil, fmt.Errorf("opennebuladatastore with ID %d was not found", datastoreID)
}

func validationSummaryForCommand(status inventoryv1alpha1.OpenNebulaDatastoreStatus) string {
	if strings.TrimSpace(status.LastValidationSummary) != "" && status.LastValidationSummary != "-" {
		return status.LastValidationSummary
	}
	if strings.TrimSpace(status.Validation.Message) != "" {
		return status.Validation.Message
	}
	return status.Validation.Phase
}

func benchmarkSummaryForCommand(status inventoryv1alpha1.OpenNebulaDatastoreBenchmarkRunStatus) string {
	if strings.TrimSpace(status.Summary) != "" && status.Summary != "-" {
		return status.Summary
	}
	if strings.TrimSpace(status.Message) != "" {
		return status.Message
	}
	return status.Phase
}

func configMapJSONSnapshot(ctx context.Context, kubeClient kubernetes.Interface, name string) map[string]any {
	snapshot := map[string]any{}
	cm, err := kubeClient.CoreV1().ConfigMaps(namespaceFromServiceAccount()).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return snapshot
	}
	for key, raw := range cm.Data {
		var parsed any
		if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
			snapshot[key] = raw
			continue
		}
		snapshot[key] = parsed
	}
	return snapshot
}

func adaptiveRecommendationSnapshot(cfg config.CSIPluginConfig, observations map[string]any) map[string]any {
	recommendations := map[string]any{}
	trackerCfg := loadAdaptiveTimeoutConfig(cfg)
	for source, raw := range observations {
		rawJSON, err := json.Marshal(raw)
		if err != nil {
			continue
		}
		tracker := opennebula.NewAdaptiveTimeoutTracker(trackerCfg)
		if err := tracker.UnmarshalJSON(rawJSON); err != nil {
			continue
		}
		sourceRecommendations := map[string]opennebula.HotplugTimeoutRecommendation{}
		for rawKey, snapshot := range tracker.Snapshot() {
			recommendation := tracker.Recommend(snapshot.Key, adaptiveStaticFloor(cfg, snapshot.Key))
			sourceRecommendations[rawKey] = recommendation
		}
		recommendations[source] = sourceRecommendations
	}
	return recommendations
}

func adaptiveStaticFloor(cfg config.CSIPluginConfig, key opennebula.HotplugObservationKey) time.Duration {
	if key.Operation == "device_resolution" {
		timeoutSeconds, ok := cfg.GetInt(config.NodeDeviceDiscoveryTimeoutVar)
		if !ok || timeoutSeconds <= 0 {
			timeoutSeconds = 30
		}
		return time.Duration(timeoutSeconds) * time.Second
	}
	sizeBytes := adaptiveRepresentativeSizeBytes(key.SizeBucket)
	policy := loadHotplugTimeoutPolicy(cfg)
	timeout := policy.BaseTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	if sizeBytes > 0 && policy.Per100GiB > 0 {
		sizeGi := int64(math.Ceil(float64(sizeBytes) / float64(1024*1024*1024)))
		increments := int64(math.Ceil(float64(sizeGi) / 100.0))
		if increments < 1 {
			increments = 1
		}
		timeout += time.Duration(increments) * policy.Per100GiB
	}
	if policy.MaxTimeout > 0 && timeout > policy.MaxTimeout {
		timeout = policy.MaxTimeout
	}
	if timeout < policy.BaseTimeout {
		timeout = policy.BaseTimeout
	}
	return timeout
}

func adaptiveRepresentativeSizeBytes(bucket string) int64 {
	switch bucket {
	case "le20Gi":
		return 20 * 1024 * 1024 * 1024
	case "gt20Gi-le100Gi":
		return 100 * 1024 * 1024 * 1024
	case "gt100Gi-le500Gi":
		return 500 * 1024 * 1024 * 1024
	case "gt500Gi":
		return 1024 * 1024 * 1024 * 1024
	default:
		return 20 * 1024 * 1024 * 1024
	}
}

func supportBundleConfig(cfg config.CSIPluginConfig) map[string]any {
	return map[string]any{
		"endpoint":                             getString(cfg, config.OpenNebulaRPCEndpointVar),
		"defaultDatastores":                    getString(cfg, config.DefaultDatastoresVar),
		"datastoreSelectionPolicy":             getString(cfg, config.DatastorePolicyVar),
		"allowedDatastoreTypes":                getString(cfg, config.AllowedDatastoreTypesVar),
		"metricsEndpoint":                      getString(cfg, config.MetricsEndpointVar),
		"vmHotplugTimeoutBaseSeconds":          getInt(cfg, config.VMHotplugTimeoutBaseVar),
		"vmHotplugTimeoutPer100GiSeconds":      getInt(cfg, config.VMHotplugTimeoutPer100GiVar),
		"vmHotplugTimeoutMaxSeconds":           getInt(cfg, config.VMHotplugTimeoutMaxVar),
		"vmHotplugCooldownSeconds":             getInt(cfg, config.VMHotplugStuckCooldownSecondsVar),
		"nodeDeviceDiscoveryTimeoutSeconds":    getInt(cfg, config.NodeDeviceDiscoveryTimeoutVar),
		"localRestartOptimizationEnabled":      getBool(cfg, config.LocalRestartOptimizationEnabledVar),
		"localRestartDetachGraceSeconds":       getInt(cfg, config.LocalRestartDetachGraceSecondsVar),
		"localRestartDetachGraceMaxSeconds":    getInt(cfg, config.LocalRestartDetachGraceMaxSecondsVar),
		"localRestartRequireNodeReady":         getBool(cfg, config.LocalRestartRequireNodeReadyVar),
		"inventoryControllerEnabled":           getBool(cfg, config.InventoryControllerEnabledVar),
		"inventoryAuthorityMode":               getString(cfg, config.InventoryDatastoreAuthorityModeVar),
		"inventoryValidationEnabled":           getBool(cfg, config.InventoryValidationEnabledVar),
		"inventoryValidationDefaultImage":      getString(cfg, config.InventoryValidationDefaultImageVar),
		"preflightLocalImmediateBindingPolicy": getString(cfg, config.PreflightLocalImmediateBindingPolicyVar),
	}
}

func flattenStorageClassDetails(datastores []inventoryv1alpha1.OpenNebulaDatastore, classes []storagev1.StorageClass) []inventoryv1alpha1.StorageClassDetail {
	byName := make(map[string]inventoryv1alpha1.StorageClassDetail)
	for _, ds := range datastores {
		for _, detail := range ds.Status.StorageClassDetails {
			existing, ok := byName[detail.Name]
			if !ok {
				byName[detail.Name] = detail
				continue
			}
			existing.Warnings = append(existing.Warnings, detail.Warnings...)
			byName[detail.Name] = existing
		}
	}
	result := make([]inventoryv1alpha1.StorageClassDetail, 0, len(classes))
	for _, class := range classes {
		if detail, ok := byName[class.Name]; ok {
			result = append(result, detail)
		}
	}
	return result
}

func getString(cfg config.CSIPluginConfig, key string) string {
	value, _ := cfg.GetString(key)
	return value
}

func getInt(cfg config.CSIPluginConfig, key string) int {
	value, _ := cfg.GetInt(key)
	return value
}

func getBool(cfg config.CSIPluginConfig, key string) bool {
	value, _ := cfg.GetBool(key)
	return value
}
