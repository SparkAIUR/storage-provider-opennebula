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
	mount "k8s.io/mount-utils"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type InventoryValidateOptions struct {
	DatastoreID  int
	StorageClass string
	Size         string
	AccessModes  []corev1.PersistentVolumeAccessMode
	FioArgs      []string
	Timeout      time.Duration
}

type VolumeHealthOptions struct {
	VolumeID string
	PVName   string
	PVC      string
}

type inventoryValidateResult struct {
	DatastoreID int    `json:"datastoreID"`
	Name        string `json:"name"`
	Phase       string `json:"phase"`
	Summary     string `json:"summary"`
	RunName     string `json:"runName"`
	RunNonce    string `json:"runNonce,omitempty"`
}

type VolumeHealthReport struct {
	PVName             string `json:"pvName,omitempty"`
	PVCNamespace       string `json:"pvcNamespace,omitempty"`
	PVCName            string `json:"pvcName,omitempty"`
	VolumeID           string `json:"volumeID,omitempty"`
	VolumeAttachment   string `json:"volumeAttachment,omitempty"`
	NodeName           string `json:"nodeName,omitempty"`
	OpenNebulaImageID  string `json:"opennebulaImageID,omitempty"`
	ExpectedSerial     string `json:"expectedSerial,omitempty"`
	ByIDPath           string `json:"byIDPath,omitempty"`
	ResolvedDevicePath string `json:"resolvedDevicePath,omitempty"`
	StagePath          string `json:"stagePath,omitempty"`
	MountPath          string `json:"mountPath,omitempty"`
	MountSource        string `json:"mountSource,omitempty"`
	Status             string `json:"status"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
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
	VolumeHealth            []VolumeHealthReport                                `json:"volumeHealth,omitempty"`
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
			AccessModes:      append([]corev1.PersistentVolumeAccessMode(nil), opts.AccessModes...),
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
				RunName:     runName,
				RunNonce:    runName,
			}
			enc := json.NewEncoder(w)
			enc.SetIndent("", "  ")
			return enc.Encode(result)
		}
		select {
		case <-deadlineCtx.Done():
			return fmt.Errorf("timed out waiting for validation run %s on datastore %d", runName, opts.DatastoreID)
		case <-time.After(5 * time.Second):
		}
	}
}

func RunVolumeHealthCommand(ctx context.Context, cfg config.CSIPluginConfig, opts VolumeHealthOptions, w io.Writer) error {
	kubeClient, _, err := preflightKubeClients()
	if err != nil {
		return err
	}
	reports, err := collectVolumeHealthReports(ctx, kubeClient, opts)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(reports)
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
	volumeHealth, _ := collectVolumeHealthReports(ctx, kubeClient, VolumeHealthOptions{})
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
		VolumeHealth:            volumeHealth,
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

func collectVolumeHealthReports(ctx context.Context, kubeClient kubernetes.Interface, opts VolumeHealthOptions) ([]VolumeHealthReport, error) {
	pvs, err := kubeClient.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	vas, err := kubeClient.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	localSessions, _ := newLocalDiskSessionStore(localDiskSessionRootPath).List()
	sharedSessions, _ := newSharedFilesystemSessionStore(sharedFilesystemSessionRootPath).List()
	mountSources := currentMountSources()

	reports := make([]VolumeHealthReport, 0)
	for _, pv := range pvs.Items {
		if pv.Spec.CSI == nil || !volumeHealthMatches(pv, opts) {
			continue
		}
		report := VolumeHealthReport{
			PVName:   pv.Name,
			VolumeID: pv.Spec.CSI.VolumeHandle,
			Status:   "unknown",
		}
		if pv.Spec.ClaimRef != nil {
			report.PVCNamespace = pv.Spec.ClaimRef.Namespace
			report.PVCName = pv.Spec.ClaimRef.Name
		}
		for _, va := range vas.Items {
			if va.Spec.Source.PersistentVolumeName == nil || *va.Spec.Source.PersistentVolumeName != pv.Name {
				continue
			}
			report.VolumeAttachment = va.Name
			report.NodeName = va.Spec.NodeName
			break
		}
		if session, ok := localSessionForVolume(localSessions, report.VolumeID); ok {
			report.StagePath = session.StagingTargetPath
			report.ExpectedSerial = session.DeviceSerial
			report.OpenNebulaImageID = firstNonEmpty(session.OpenNebulaImageID, report.OpenNebulaImageID)
			report.ResolvedDevicePath = session.DevicePath
			report.ByIDPath, report.ResolvedDevicePath = resolveByIDForReport(session.DeviceSerial, report.ResolvedDevicePath)
			report.MountSource = mountSources[session.StagingTargetPath]
			report.MountPath = session.StagingTargetPath
			report.Status, report.Reason, report.Message = classifyVolumeHealthReport(report)
			reports = append(reports, report)
			for _, target := range session.PublishedTargets {
				targetReport := report
				targetReport.MountPath = target.TargetPath
				targetReport.MountSource = mountSources[target.TargetPath]
				targetReport.Status, targetReport.Reason, targetReport.Message = classifyVolumeHealthReport(targetReport)
				reports = append(reports, targetReport)
			}
			continue
		}
		if session, ok := sharedSessionForVolume(sharedSessions, report.VolumeID); ok {
			report.StagePath = session.StagingTargetPath
			report.MountPath = session.StagingTargetPath
			report.MountSource = mountSources[session.StagingTargetPath]
			report.Status = "cephfs-session"
			report.Reason = "CephFSSessionPresent"
			reports = append(reports, report)
			for _, target := range session.PublishedTargets {
				targetReport := report
				targetReport.MountPath = target.TargetPath
				targetReport.MountSource = mountSources[target.TargetPath]
				reports = append(reports, targetReport)
			}
			continue
		}
		report.Status = "no-node-session"
		report.Reason = "SessionNotFound"
		report.Message = "No local node session was found in this container"
		reports = append(reports, report)
	}
	return reports, nil
}

func volumeHealthMatches(pv corev1.PersistentVolume, opts VolumeHealthOptions) bool {
	if strings.TrimSpace(opts.VolumeID) != "" && pv.Spec.CSI.VolumeHandle != strings.TrimSpace(opts.VolumeID) {
		return false
	}
	if strings.TrimSpace(opts.PVName) != "" && pv.Name != strings.TrimSpace(opts.PVName) {
		return false
	}
	if strings.TrimSpace(opts.PVC) != "" {
		namespace, name, ok := strings.Cut(strings.TrimSpace(opts.PVC), "/")
		if !ok || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != namespace || pv.Spec.ClaimRef.Name != name {
			return false
		}
	}
	return true
}

func currentMountSources() map[string]string {
	result := map[string]string{}
	mountPoints, err := mount.New("").List()
	if err != nil {
		return result
	}
	for _, mountPoint := range mountPoints {
		result[mountPoint.Path] = mountPoint.Device
	}
	return result
}

func localSessionForVolume(sessions []localDiskSession, volumeID string) (localDiskSession, bool) {
	for _, session := range sessions {
		if session.VolumeID == volumeID {
			return session, true
		}
	}
	return localDiskSession{}, false
}

func sharedSessionForVolume(sessions []sharedFilesystemSession, volumeID string) (sharedFilesystemSession, bool) {
	for _, session := range sessions {
		if session.VolumeID == volumeID {
			return session, true
		}
	}
	return sharedFilesystemSession{}, false
}

func resolveByIDForReport(serial, fallback string) (string, string) {
	if strings.TrimSpace(serial) == "" {
		return "", fallback
	}
	candidates, err := nodeByIDGlob(filepath.Join(defaultDiskPath, "disk", "by-id", "*"))
	if err != nil {
		return "", fallback
	}
	for _, candidate := range candidates {
		if !strings.Contains(strings.ToLower(filepath.Base(candidate)), strings.ToLower(serial)) {
			continue
		}
		resolved, err := nodeEvalSymlinks(candidate)
		if err != nil {
			return candidate, fallback
		}
		return candidate, resolved
	}
	return "", fallback
}

func classifyVolumeHealthReport(report VolumeHealthReport) (string, string, string) {
	if strings.TrimSpace(report.MountPath) == "" {
		return "unknown", "MountPathMissing", "No node mount path was available"
	}
	if strings.TrimSpace(report.MountSource) == "" {
		return "stale", "MountSourceMissing", "Mount path is not present in the current mount table"
	}
	if strings.HasPrefix(report.MountSource, "/dev/") {
		if _, err := os.Stat(report.MountSource); err != nil {
			return "stale", "MountSourceDeviceMissing", err.Error()
		}
		if report.ExpectedSerial != "" && !strings.Contains(strings.ToLower(report.MountSource), strings.ToLower(report.ExpectedSerial)) && report.ByIDPath != "" && report.ResolvedDevicePath != report.MountSource {
			return "stale", "DeviceSerialMoved", fmt.Sprintf("expected serial %s resolves to %s but mount source is %s", report.ExpectedSerial, report.ResolvedDevicePath, report.MountSource)
		}
	}
	return "healthy", "MountPresent", "Mount source is present"
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
		"localRWOStaleMountActivePodRecovery":  getBool(cfg, config.LocalRWOStaleMountActivePodRecoveryVar),
		"localRWOStaleMountMaxAttempts":        getInt(cfg, config.LocalRWOStaleMountMaxAttemptsVar),
		"localRWOStaleMountBackoffSeconds":     getInt(cfg, config.LocalRWOStaleMountBackoffSecondsVar),
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
