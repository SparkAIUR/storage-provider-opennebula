/*
Copyright 2025, OpenNebula Project, OpenNebula Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	inventorycache "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/cache"
	"github.com/container-storage-interface/spec/lib/go/csi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"
)

const (
	DefaultDriverName         = "csi.opennebula.io" //TODO: get from a repo metadata file or from a build flag
	DefaultGRPCServerEndpoint = "unix:///tmp/csi.sock"
	DefaultVolumeSizeBytes    = 1 * 1024 * 1024 * 1024
	GracefulShutdownTimeout   = 25 * time.Second
)

var (
	driverVersion   = "dev"
	driverCommit    = "unknown"
	driverBuildDate = "unknown"
)

type Driver struct {
	name               string
	grpcServerEndpoint string
	nodeID             string
	version            string
	commit             string
	buildDate          string

	PluginConfig config.CSIPluginConfig

	controllerServerCapabilities []*csi.ControllerServiceCapability

	operationLocks       *OperationLocks
	hotplugGuard         *HotplugGuard
	controllerLeadership *ControllerLeadership

	maxVolumesPerNode int64

	mounter *mount.SafeFormatAndMount

	featureGates      FeatureGates
	metrics           *DriverMetrics
	kubeRuntime       *KubeRuntime
	stickyAttachments *StickyAttachmentManager
	hotplugQueue      *HotplugQueueManager
	adaptiveTimeouts  *opennebula.AdaptiveTimeoutTracker

	inventoryEligibility inventorycache.DatastoreEligibilityProvider
}

type DriverOptions struct {
	NodeID             string
	DriverName         string
	MaxVolumesPerNode  int64
	GRPCServerEndpoint string
	PluginConfig       config.CSIPluginConfig
	Mounter            *mount.SafeFormatAndMount
}

func NewDriver(options *DriverOptions) *Driver {
	return &Driver{
		name:               options.DriverName,
		version:            driverVersion,
		commit:             driverCommit,
		buildDate:          driverBuildDate,
		nodeID:             options.NodeID,
		grpcServerEndpoint: options.GRPCServerEndpoint,
		PluginConfig:       options.PluginConfig,
		maxVolumesPerNode:  options.MaxVolumesPerNode,
		mounter:            options.Mounter,
		featureGates:       loadFeatureGates(options.PluginConfig),
		operationLocks:     NewOperationLocks(),
		hotplugGuard:       NewHotplugGuard(loadHotplugCooldown(options.PluginConfig)),
	}
}

func (d *Driver) Run(ctx context.Context) error {
	klog.InfoS("Starting OpenNebula CSI driver",
		"name", d.name,
		"version", d.version,
		"commit", d.commit,
		"buildDate", d.buildDate,
		"nodeID", d.nodeID,
		"featureGates", d.featureGates)

	d.metrics = NewDriverMetrics(d.version, d.commit)
	d.kubeRuntime = NewKubeRuntime(d.name)
	d.stickyAttachments = NewStickyAttachmentManager(d.kubeRuntime, "")
	if err := d.stickyAttachments.LoadFromConfigMap(ctx); err != nil {
		klog.V(2).InfoS("Failed to load sticky attachment state", "err", err)
	}
	d.hotplugQueue = NewHotplugQueueManager(d.kubeRuntime, "", d.metrics, loadHotplugQueueMaxWait(d.PluginConfig), loadHotplugQueueAgeBoost(d.PluginConfig))
	d.adaptiveTimeouts = opennebula.NewAdaptiveTimeoutTracker(loadAdaptiveTimeoutConfig(d.PluginConfig))
	if err := d.loadAdaptiveTimeoutObservations(ctx); err != nil {
		klog.V(2).InfoS("Failed to load adaptive timeout observations", "err", err)
	}

	if enabled, _ := d.PluginConfig.GetBool(config.InventoryControllerEnabledVar); enabled && strings.TrimSpace(d.nodeID) == "" {
		restConfig, err := rest.InClusterConfig()
		if err != nil {
			return fmt.Errorf("failed to initialize Kubernetes config for inventory eligibility cache: %w", err)
		}
		authorityMode, _ := d.PluginConfig.GetString(config.InventoryDatastoreAuthorityModeVar)
		resyncSeconds, ok := d.PluginConfig.GetInt(config.InventoryControllerResyncDatastoresVar)
		if !ok || resyncSeconds <= 0 {
			resyncSeconds = 60
		}
		provider, err := inventorycache.NewProvider(restConfig, time.Duration(resyncSeconds)*time.Second, authorityMode)
		if err != nil {
			return fmt.Errorf("failed to initialize inventory eligibility cache: %w", err)
		}
		if err := provider.Start(ctx); err != nil {
			return fmt.Errorf("failed to start inventory eligibility cache: %w", err)
		}
		d.inventoryEligibility = provider
	}

	controllerLeadership, err := NewControllerLeadership(ctx, d.PluginConfig)
	if err != nil {
		return err
	}
	d.controllerLeadership = controllerLeadership

	grpcServer := NewGRPCServer()

	endpoint, ok := d.PluginConfig.GetString(config.OpenNebulaRPCEndpointVar)
	if !ok {
		return fmt.Errorf("failed to get %s endpoint from config", config.OpenNebulaRPCEndpointVar)
	}

	credentials, ok := d.PluginConfig.GetString(config.OpenNebulaCredentialsVar)
	if !ok {
		return fmt.Errorf("failed to get %s credentials from config", config.OpenNebulaCredentialsVar)
	}
	volumeProvider, err := opennebula.NewPersistentDiskVolumeProvider(
		opennebula.NewClient(opennebula.OpenNebulaConfig{
			Endpoint:    endpoint,
			Credentials: credentials,
		}),
		loadHotplugTimeoutPolicy(d.PluginConfig),
	)
	if err != nil || volumeProvider == nil {
		return fmt.Errorf("failed to create PersistentDiskVolumeProvider: %v", err)
	}

	sharedFilesystemProvider, err := opennebula.NewCephFSVolumeProvider(
		opennebula.NewClient(opennebula.OpenNebulaConfig{
			Endpoint:    endpoint,
			Credentials: credentials,
		}),
		d.mounter.Exec,
	)
	if err != nil || sharedFilesystemProvider == nil {
		return fmt.Errorf("failed to create CephFSVolumeProvider: %v", err)
	}

	controllerServer := NewControllerServer(d, volumeProvider, sharedFilesystemProvider)
	grpcServer.Start(
		d.grpcServerEndpoint,
		NewIdentityServer(d),
		NewNodeServer(d, d.mounter),
		controllerServer,
		d.controllerLeadership,
	)
	controllerServer.StartBackgroundWorkers(ctx)
	d.maybeStartWebhookServer(ctx)

	metricsServer := NewMetricsServer(d.PluginConfig, d.metrics)
	metricsServer.Start()

	go func() {
		<-ctx.Done()
		klog.Info("Received shutdown signal, stopping driver...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), GracefulShutdownTimeout)
		defer cancel()
		metricsServer.Stop(shutdownCtx)
		grpcServer.Stop(shutdownCtx)
	}()

	grpcServer.Wait()

	return nil
}

func loadHotplugTimeoutPolicy(cfg config.CSIPluginConfig) opennebula.HotplugTimeoutPolicy {
	baseTimeout, baseOK := cfg.GetInt(config.VMHotplugTimeoutBaseVar)
	if !baseOK || baseTimeout <= 0 {
		if legacyTimeout, legacyOK := cfg.GetInt(config.VMHotplugTimeoutVar); legacyOK && legacyTimeout > 0 {
			baseTimeout = legacyTimeout
		} else {
			baseTimeout = 120
		}
	}

	per100Gi, ok := cfg.GetInt(config.VMHotplugTimeoutPer100GiVar)
	if !ok || per100Gi < 0 {
		per100Gi = 60
	}

	maxTimeout, ok := cfg.GetInt(config.VMHotplugTimeoutMaxVar)
	if !ok || maxTimeout <= 0 {
		maxTimeout = 900
	}
	if maxTimeout < baseTimeout {
		maxTimeout = baseTimeout
	}

	return opennebula.HotplugTimeoutPolicy{
		BaseTimeout:  time.Duration(baseTimeout) * time.Second,
		Per100GiB:    time.Duration(per100Gi) * time.Second,
		MaxTimeout:   time.Duration(maxTimeout) * time.Second,
		PollInterval: time.Second,
	}
}

func loadHotplugCooldown(cfg config.CSIPluginConfig) time.Duration {
	cooldownSeconds, ok := cfg.GetInt(config.VMHotplugStuckCooldownSecondsVar)
	if !ok || cooldownSeconds <= 0 {
		cooldownSeconds = 300
	}

	return time.Duration(cooldownSeconds) * time.Second
}

func loadHotplugQueueMaxWait(cfg config.CSIPluginConfig) time.Duration {
	seconds, ok := cfg.GetInt(config.HotplugQueueMaxWaitSecondsVar)
	if !ok || seconds <= 0 {
		seconds = 180
	}
	return time.Duration(seconds) * time.Second
}

func loadHotplugQueueAgeBoost(cfg config.CSIPluginConfig) time.Duration {
	seconds, ok := cfg.GetInt(config.HotplugQueueAgeBoostSecondsVar)
	if !ok || seconds <= 0 {
		seconds = 30
	}
	return time.Duration(seconds) * time.Second
}

func loadAdaptiveTimeoutConfig(cfg config.CSIPluginConfig) opennebula.AdaptiveTimeoutConfig {
	enabled, ok := cfg.GetBool(config.HotplugAdaptiveTimeoutEnabledVar)
	if !ok {
		enabled = true
	}
	minSamples, ok := cfg.GetInt(config.HotplugAdaptiveMinSamplesVar)
	if !ok || minSamples <= 0 {
		minSamples = 8
	}
	sampleWindow, ok := cfg.GetInt(config.HotplugAdaptiveSampleWindowVar)
	if !ok || sampleWindow <= 0 {
		sampleWindow = 20
	}
	multiplier, ok := cfg.GetInt(config.HotplugAdaptiveP95MultiplierPercentVar)
	if !ok || multiplier <= 0 {
		multiplier = 400
	}
	maxSeconds, ok := cfg.GetInt(config.HotplugAdaptiveMaxSecondsVar)
	if !ok || maxSeconds <= 0 {
		maxSeconds = 1800
	}
	return opennebula.AdaptiveTimeoutConfig{
		Enabled:              enabled,
		MinSamples:           minSamples,
		SampleWindow:         sampleWindow,
		P95MultiplierPercent: multiplier,
		MaxTimeout:           time.Duration(maxSeconds) * time.Second,
	}
}

func (d *Driver) inventoryAuthorityMode() string {
	value, ok := d.PluginConfig.GetString(config.InventoryDatastoreAuthorityModeVar)
	if !ok {
		return inventorycache.AuthorityModeStrict
	}
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return inventorycache.AuthorityModeStrict
	}
	return value
}

const adaptiveTimeoutObservationsConfigMapName = "opennebula-csi-hotplug-observations"

func (d *Driver) adaptiveTimeoutSourceID() string {
	if d == nil {
		return ""
	}
	if strings.TrimSpace(d.nodeID) == "" {
		return "controller"
	}
	return "node:" + strings.TrimSpace(d.nodeID)
}

func (d *Driver) loadAdaptiveTimeoutObservations(ctx context.Context) error {
	if d == nil || d.kubeRuntime == nil || !d.kubeRuntime.enabled || d.adaptiveTimeouts == nil {
		return nil
	}
	cm, err := d.kubeRuntime.GetConfigMap(ctx, namespaceFromServiceAccount(), adaptiveTimeoutObservationsConfigMapName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	payload, ok := cm.Data[d.adaptiveTimeoutSourceID()]
	if !ok || strings.TrimSpace(payload) == "" {
		return nil
	}
	return d.adaptiveTimeouts.UnmarshalJSON([]byte(payload))
}

func (d *Driver) persistAdaptiveTimeoutObservations(ctx context.Context) {
	if d == nil || d.kubeRuntime == nil || !d.kubeRuntime.enabled || d.adaptiveTimeouts == nil {
		return
	}
	payload, err := d.adaptiveTimeouts.MarshalJSON()
	if err != nil {
		klog.V(4).InfoS("Failed to marshal adaptive timeout observations", "source", d.adaptiveTimeoutSourceID(), "err", err)
		return
	}
	if err := d.kubeRuntime.UpsertConfigMapData(ctx, namespaceFromServiceAccount(), adaptiveTimeoutObservationsConfigMapName, map[string]string{
		d.adaptiveTimeoutSourceID(): string(payload),
	}); err != nil {
		klog.V(4).InfoS("Failed to persist adaptive timeout observations", "source", d.adaptiveTimeoutSourceID(), "err", err)
	}
}

func (d *Driver) observeAdaptiveTimeout(ctx context.Context, operation, backend string, sizeBytes int64, duration time.Duration) {
	if d == nil || d.adaptiveTimeouts == nil || duration <= 0 {
		return
	}
	key := opennebula.NormalizeObservationKey(operation, backend, sizeBytes)
	d.adaptiveTimeouts.Observe(key, duration)
	d.persistAdaptiveTimeoutObservations(ctx)
}
