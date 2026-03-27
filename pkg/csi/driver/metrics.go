package driver

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"
)

const (
	driverMetricsPath = "/metrics"
)

type DriverMetrics struct {
	registry *prometheus.Registry

	operationTotal               *prometheus.CounterVec
	operationDuration            *prometheus.HistogramVec
	datastoreSelectionTotal      *prometheus.CounterVec
	attachValidationTotal        *prometheus.CounterVec
	cephFSSubvolumeTotal         *prometheus.CounterVec
	snapshotTotal                *prometheus.CounterVec
	preflightTotal               *prometheus.CounterVec
	hotplugGuardTotal            *prometheus.CounterVec
	hotplugTimeoutTotal          *prometheus.CounterVec
	controllerPublishDuration    *prometheus.HistogramVec
	nodeStageDuration            *prometheus.HistogramVec
	nodeDeviceResolutionDuration *prometheus.HistogramVec
	hotplugRecoveryTotal         *prometheus.CounterVec
	stickyDetachTotal            *prometheus.CounterVec
	stickyDetachResidency        *prometheus.HistogramVec
	sameNodeRestartReuseTotal    *prometheus.CounterVec
	datastoreFreeBytes           *prometheus.GaugeVec
	datastoreTotalBytes          *prometheus.GaugeVec
	buildInfo                    *prometheus.GaugeVec
}

func NewDriverMetrics(version, commit string) *DriverMetrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	metrics := &DriverMetrics{
		registry: registry,
		operationTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opennebula_csi_operation_total",
			Help: "Total number of CSI driver operations partitioned by operation, backend, and outcome.",
		}, []string{"operation", "backend", "outcome"}),
		operationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "opennebula_csi_operation_duration_seconds",
			Help:    "Duration of CSI driver operations partitioned by operation, backend, and outcome.",
			Buckets: prometheus.DefBuckets,
		}, []string{"operation", "backend", "outcome"}),
		datastoreSelectionTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opennebula_csi_datastore_selection_total",
			Help: "Total number of datastore selection outcomes by policy, backend, datastore, and outcome.",
		}, []string{"policy", "backend", "datastore_id", "outcome"}),
		attachValidationTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opennebula_csi_attach_validation_total",
			Help: "Total number of attach validation outcomes by backend and deployment mode.",
		}, []string{"backend", "mode", "outcome"}),
		cephFSSubvolumeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opennebula_csi_cephfs_subvolume_total",
			Help: "Total number of CephFS subvolume operations by operation and outcome.",
		}, []string{"operation", "outcome"}),
		snapshotTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opennebula_csi_snapshot_total",
			Help: "Total number of snapshot operations by backend, operation, and outcome.",
		}, []string{"backend", "operation", "outcome"}),
		preflightTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opennebula_csi_preflight_total",
			Help: "Total number of preflight checks by check name and outcome.",
		}, []string{"check", "outcome"}),
		hotplugGuardTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opennebula_csi_hotplug_guard_total",
			Help: "Total number of hotplug guard decisions by operation, reason, and outcome.",
		}, []string{"operation", "reason", "outcome"}),
		hotplugTimeoutTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opennebula_csi_hotplug_timeout_total",
			Help: "Total number of block hotplug timeout outcomes by operation, backend, and outcome.",
		}, []string{"operation", "backend", "outcome"}),
		controllerPublishDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "opennebula_csi_controller_publish_duration_seconds",
			Help:    "Duration of ControllerPublishVolume operations by backend and outcome.",
			Buckets: prometheus.DefBuckets,
		}, []string{"backend", "outcome"}),
		nodeStageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "opennebula_csi_node_stage_duration_seconds",
			Help:    "Duration of NodeStageVolume operations by backend and outcome.",
			Buckets: prometheus.DefBuckets,
		}, []string{"backend", "outcome"}),
		nodeDeviceResolutionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "opennebula_csi_node_device_resolution_duration_seconds",
			Help:    "Duration of node device path resolution by backend and outcome.",
			Buckets: prometheus.DefBuckets,
		}, []string{"backend", "outcome"}),
		hotplugRecoveryTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opennebula_csi_hotplug_recovery_total",
			Help: "Total number of hotplug recovery path decisions by operation, reason, and outcome.",
		}, []string{"operation", "reason", "outcome"}),
		stickyDetachTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opennebula_csi_sticky_detach_total",
			Help: "Total number of sticky detach lifecycle outcomes by outcome and reason.",
		}, []string{"outcome", "reason"}),
		stickyDetachResidency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "opennebula_csi_sticky_detach_residency_seconds",
			Help:    "How long a sticky detach state remained active before completion.",
			Buckets: prometheus.DefBuckets,
		}, []string{"outcome"}),
		sameNodeRestartReuseTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opennebula_csi_same_node_restart_reuse_total",
			Help: "Total number of same-node restart attachment reuses by backend.",
		}, []string{"backend"}),
		datastoreFreeBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "opennebula_csi_datastore_free_bytes",
			Help: "Latest observed free capacity for a datastore by backend and datastore ID.",
		}, []string{"backend", "datastore_id"}),
		datastoreTotalBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "opennebula_csi_datastore_total_bytes",
			Help: "Latest observed total capacity for a datastore by backend and datastore ID.",
		}, []string{"backend", "datastore_id"}),
		buildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "opennebula_csi_build_info",
			Help: "Build metadata for the SparkAI OpenNebula CSI driver.",
		}, []string{"version", "commit"}),
	}

	registry.MustRegister(
		metrics.operationTotal,
		metrics.operationDuration,
		metrics.datastoreSelectionTotal,
		metrics.attachValidationTotal,
		metrics.cephFSSubvolumeTotal,
		metrics.snapshotTotal,
		metrics.preflightTotal,
		metrics.hotplugGuardTotal,
		metrics.hotplugTimeoutTotal,
		metrics.controllerPublishDuration,
		metrics.nodeStageDuration,
		metrics.nodeDeviceResolutionDuration,
		metrics.hotplugRecoveryTotal,
		metrics.stickyDetachTotal,
		metrics.stickyDetachResidency,
		metrics.sameNodeRestartReuseTotal,
		metrics.datastoreFreeBytes,
		metrics.datastoreTotalBytes,
		metrics.buildInfo,
	)

	metrics.buildInfo.WithLabelValues(version, commit).Set(1)

	return metrics
}

func (m *DriverMetrics) RecordOperation(operation, backend, outcome string, duration time.Duration) {
	if m == nil {
		return
	}
	m.operationTotal.WithLabelValues(operation, backend, outcome).Inc()
	m.operationDuration.WithLabelValues(operation, backend, outcome).Observe(duration.Seconds())
}

func (m *DriverMetrics) RecordDatastoreSelection(policy, backend string, datastoreID int, outcome string) {
	if m == nil {
		return
	}
	m.datastoreSelectionTotal.WithLabelValues(policy, backend, strconv.Itoa(datastoreID), outcome).Inc()
}

func (m *DriverMetrics) RecordAttachValidation(backend, mode, outcome string) {
	if m == nil {
		return
	}
	m.attachValidationTotal.WithLabelValues(backend, mode, outcome).Inc()
}

func (m *DriverMetrics) RecordCephFSSubvolume(operation, outcome string) {
	if m == nil {
		return
	}
	m.cephFSSubvolumeTotal.WithLabelValues(operation, outcome).Inc()
}

func (m *DriverMetrics) RecordSnapshot(backend, operation, outcome string) {
	if m == nil {
		return
	}
	m.snapshotTotal.WithLabelValues(backend, operation, outcome).Inc()
}

func (m *DriverMetrics) RecordPreflight(check, outcome string) {
	if m == nil {
		return
	}
	m.preflightTotal.WithLabelValues(check, outcome).Inc()
}

func (m *DriverMetrics) RecordHotplugGuard(operation, reason, outcome string) {
	if m == nil {
		return
	}
	m.hotplugGuardTotal.WithLabelValues(operation, reason, outcome).Inc()
}

func (m *DriverMetrics) RecordHotplugTimeout(operation, backend, outcome string) {
	if m == nil {
		return
	}
	m.hotplugTimeoutTotal.WithLabelValues(operation, backend, outcome).Inc()
}

func (m *DriverMetrics) RecordControllerPublishDuration(backend, outcome string, duration time.Duration) {
	if m == nil {
		return
	}
	m.controllerPublishDuration.WithLabelValues(backend, outcome).Observe(duration.Seconds())
}

func (m *DriverMetrics) RecordNodeStageDuration(backend, outcome string, duration time.Duration) {
	if m == nil {
		return
	}
	m.nodeStageDuration.WithLabelValues(backend, outcome).Observe(duration.Seconds())
}

func (m *DriverMetrics) RecordNodeDeviceResolutionDuration(backend, outcome string, duration time.Duration) {
	if m == nil {
		return
	}
	m.nodeDeviceResolutionDuration.WithLabelValues(backend, outcome).Observe(duration.Seconds())
}

func (m *DriverMetrics) RecordHotplugRecovery(operation, reason, outcome string) {
	if m == nil {
		return
	}
	m.hotplugRecoveryTotal.WithLabelValues(operation, reason, outcome).Inc()
}

func (m *DriverMetrics) RecordStickyDetach(outcome, reason string) {
	if m == nil {
		return
	}
	m.stickyDetachTotal.WithLabelValues(outcome, reason).Inc()
}

func (m *DriverMetrics) RecordStickyDetachResidency(outcome string, duration time.Duration) {
	if m == nil {
		return
	}
	m.stickyDetachResidency.WithLabelValues(outcome).Observe(duration.Seconds())
}

func (m *DriverMetrics) RecordSameNodeRestartReuse(backend string) {
	if m == nil {
		return
	}
	m.sameNodeRestartReuseTotal.WithLabelValues(backend).Inc()
}

func (m *DriverMetrics) SetDatastoreCapacity(backend string, datastoreID int, freeBytes, totalBytes int64) {
	if m == nil {
		return
	}
	labels := []string{backend, strconv.Itoa(datastoreID)}
	m.datastoreFreeBytes.WithLabelValues(labels...).Set(float64(freeBytes))
	m.datastoreTotalBytes.WithLabelValues(labels...).Set(float64(totalBytes))
}

type MetricsServer struct {
	endpoint string
	server   *http.Server
}

func NewMetricsServer(cfg config.CSIPluginConfig, metrics *DriverMetrics) *MetricsServer {
	endpoint, ok := cfg.GetString(config.MetricsEndpointVar)
	if !ok {
		endpoint = ""
	}
	if endpoint == "" || metrics == nil {
		return &MetricsServer{}
	}

	mux := http.NewServeMux()
	mux.Handle(driverMetricsPath, promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	return &MetricsServer{
		endpoint: endpoint,
		server: &http.Server{
			Addr:              endpoint,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
}

func (s *MetricsServer) Start() {
	if s == nil || s.server == nil || s.endpoint == "" {
		return
	}

	go func() {
		klog.InfoS("Starting driver metrics server", "endpoint", s.endpoint, "path", driverMetricsPath)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.ErrorS(err, "Driver metrics server failed", "endpoint", s.endpoint)
		}
	}()
}

func (s *MetricsServer) Stop(ctx context.Context) {
	if s == nil || s.server == nil {
		return
	}
	if err := s.server.Shutdown(ctx); err != nil {
		klog.ErrorS(err, "Failed to stop driver metrics server")
	}
}
