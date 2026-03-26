package controller

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/OpenNebula/one/src/oca/go/src/goca"
	datastoreSchema "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/datastore"
	"github.com/OpenNebula/one/src/oca/go/src/goca/schemas/shared"
	vmSchema "github.com/OpenNebula/one/src/oca/go/src/goca/schemas/vm"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/config"
	"github.com/SparkAIUR/storage-provider-opennebula/pkg/csi/opennebula"
	inventoryv1alpha1 "github.com/SparkAIUR/storage-provider-opennebula/pkg/inventory/apis/storageprovider/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

const (
	validationNamespace                                = "kube-system"
	validationPVCPrefix                                = "one-ds-validate-pvc-"
	validationJobPrefix                                = "one-ds-validate-job-"
	eventReasonValidationTriggered                     = "ValidationTriggered"
	eventReasonValidationSucceeded                     = "ValidationSucceeded"
	eventReasonValidationFailed                        = "ValidationFailed"
	eventReasonProvisioningDisabled                    = "ProvisioningDisabled"
	eventReasonDiscoveryMismatch                       = "DiscoveryMismatch"
	eventReasonInventoryVMNotFound                     = "VMNotFound"
	eventReasonInventoryNodeNotFound                   = "NodeNotFound"
	annotationDatastoreID                              = "storage-provider.opennebula.sparkaiur.io/datastore-id"
	topologySystemDSLabel                              = "topology.opennebula.sparkaiur.io/system-ds"
	defaultValidationImagePullPolicy corev1.PullPolicy = corev1.PullIfNotPresent
)

type Options struct {
	Namespace         string
	ValidationEnabled bool
	DefaultImage      string
}

type Syncer struct {
	client                 ctrlclient.Client
	scheme                 *runtime.Scheme
	kube                   kubernetes.Interface
	ctrl                   *goca.Controller
	namespace              string
	resyncDatastores       time.Duration
	resyncNodes            time.Duration
	validationEnabled      bool
	defaultValidationImage string
}

func Run(ctx context.Context, cfg config.CSIPluginConfig, options Options) error {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := storagev1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := inventoryv1alpha1.AddToScheme(scheme); err != nil {
		return err
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to initialize in-cluster config for inventory controller: %w", err)
	}

	metricsAddr := "0"
	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                 scheme,
		LeaderElection:         true,
		LeaderElectionID:       loadInventoryLeaderElectionID(cfg),
		HealthProbeBindAddress: ":8081",
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
	})
	if err != nil {
		return err
	}

	endpoint, ok := cfg.GetString(config.OpenNebulaRPCEndpointVar)
	if !ok {
		return fmt.Errorf("failed to get %s endpoint from config", config.OpenNebulaRPCEndpointVar)
	}
	credentials, ok := cfg.GetString(config.OpenNebulaCredentialsVar)
	if !ok {
		return fmt.Errorf("failed to get %s credentials from config", config.OpenNebulaCredentialsVar)
	}

	kube, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}

	syncer := &Syncer{
		client:                 mgr.GetClient(),
		scheme:                 scheme,
		kube:                   kube,
		ctrl:                   goca.NewController(opennebula.NewClient(opennebula.OpenNebulaConfig{Endpoint: endpoint, Credentials: credentials}).Client),
		namespace:              options.Namespace,
		resyncDatastores:       time.Duration(loadResync(cfg, config.InventoryControllerResyncDatastoresVar, 60)) * time.Second,
		resyncNodes:            time.Duration(loadResync(cfg, config.InventoryControllerResyncNodesVar, 30)) * time.Second,
		validationEnabled:      options.ValidationEnabled,
		defaultValidationImage: options.DefaultImage,
	}

	if err := mgr.Add(syncer); err != nil {
		return err
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}

	return mgr.Start(ctx)
}

func (s *Syncer) NeedLeaderElection() bool {
	return true
}

func (s *Syncer) Start(ctx context.Context) error {
	dsTicker := time.NewTicker(s.resyncDatastores)
	defer dsTicker.Stop()
	nodeTicker := time.NewTicker(s.resyncNodes)
	defer nodeTicker.Stop()

	if err := s.syncDatastores(ctx); err != nil {
		ctrl.Log.Error(err, "initial datastore inventory sync failed")
	}
	if err := s.syncNodes(ctx); err != nil {
		ctrl.Log.Error(err, "initial node inventory sync failed")
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-dsTicker.C:
			if err := s.syncDatastores(ctx); err != nil {
				ctrl.Log.Error(err, "datastore inventory sync failed")
			}
		case <-nodeTicker.C:
			if err := s.syncNodes(ctx); err != nil {
				ctrl.Log.Error(err, "node inventory sync failed")
			}
		}
	}
}

func (s *Syncer) syncDatastores(ctx context.Context) error {
	dsPool, err := s.ctrl.Datastores().InfoContext(ctx)
	if err != nil {
		return err
	}
	pvs, err := s.kube.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	pvcs, err := s.kube.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	scs, err := s.kube.StorageV1().StorageClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	vas, err := s.kube.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	pvcByNSName := make(map[string]corev1.PersistentVolumeClaim, len(pvcs.Items))
	for _, pvc := range pvcs.Items {
		pvcByNSName[pvc.Namespace+"/"+pvc.Name] = pvc
	}

	discoveredIDs := make(map[int]datastoreSchema.Datastore, len(dsPool.Datastores))
	for _, ds := range dsPool.Datastores {
		discoveredIDs[ds.ID] = ds
		if err := s.ensureDatastoreObject(ctx, ds); err != nil {
			return err
		}
	}

	var dsList inventoryv1alpha1.OpenNebulaDatastoreList
	if err := s.client.List(ctx, &dsList); err != nil {
		return err
	}

	for _, item := range dsList.Items {
		ds, ok := discoveredIDs[item.Spec.Discovery.OpenNebulaDatastoreID]
		status := item.DeepCopy()
		if !ok {
			status.Status = inventoryv1alpha1.OpenNebulaDatastoreStatus{
				ObservedGeneration: item.Generation,
				Phase:              inventoryv1alpha1.DatastorePhaseNotFound,
				Backend:            item.Spec.Discovery.ExpectedBackend,
				Conditions: []metav1.Condition{
					newCondition("Discovered", metav1.ConditionFalse, "DatastoreMissing", "Datastore was not found in OpenNebula"),
				},
			}
			if err := s.client.Status().Update(ctx, status); err != nil {
				return err
			}
			continue
		}
		nextStatus := s.buildDatastoreStatus(item, ds, pvs.Items, pvcByNSName, scs.Items, vas.Items)
		status.Status = nextStatus
		if err := s.client.Status().Update(ctx, status); err != nil {
			return err
		}
		if s.validationEnabled {
			if err := s.reconcileValidation(ctx, status); err != nil {
				ctrl.Log.Error(err, "failed reconciling datastore validation", "datastore", status.Name)
			}
		}
	}
	return nil
}

func (s *Syncer) syncNodes(ctx context.Context) error {
	nodes, err := s.kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	vas, err := s.kube.StorageV1().VolumeAttachments().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	pvs, err := s.kube.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	pvByName := make(map[string]corev1.PersistentVolume, len(pvs.Items))
	for _, pv := range pvs.Items {
		pvByName[pv.Name] = pv
	}

	for _, node := range nodes.Items {
		if err := s.ensureNodeObject(ctx, node); err != nil {
			return err
		}
	}

	var nodeList inventoryv1alpha1.OpenNebulaNodeList
	if err := s.client.List(ctx, &nodeList); err != nil {
		return err
	}

	datastores, _ := s.ctrl.Datastores().InfoContext(ctx)
	dsNames := make(map[int]string)
	for _, ds := range datastores.Datastores {
		dsNames[ds.ID] = ds.Name
	}

	nodeByName := make(map[string]corev1.Node, len(nodes.Items))
	for _, node := range nodes.Items {
		nodeByName[node.Name] = node
	}

	for _, item := range nodeList.Items {
		status := item.DeepCopy()
		node, ok := nodeByName[item.Name]
		if !ok {
			status.Status = inventoryv1alpha1.OpenNebulaNodeStatus{
				ObservedGeneration: item.Generation,
				Phase:              inventoryv1alpha1.NodePhaseNotFound,
				Conditions: []metav1.Condition{
					newCondition("KubernetesNodeFound", metav1.ConditionFalse, "NodeMissing", "Kubernetes node was not found"),
				},
			}
			if err := s.client.Status().Update(ctx, status); err != nil {
				return err
			}
			continue
		}
		nextStatus := s.buildNodeStatus(ctx, item, node, vas.Items, pvByName, dsNames)
		status.Status = nextStatus
		if err := s.client.Status().Update(ctx, status); err != nil {
			return err
		}
	}

	return nil
}

func (s *Syncer) ensureDatastoreObject(ctx context.Context, ds datastoreSchema.Datastore) error {
	name := datastoreObjectName(ds.ID)
	current := &inventoryv1alpha1.OpenNebulaDatastore{}
	err := s.client.Get(ctx, types.NamespacedName{Name: name}, current)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	obj := &inventoryv1alpha1.OpenNebulaDatastore{
		TypeMeta: metav1.TypeMeta{
			APIVersion: inventoryv1alpha1.SchemeGroupVersion.String(),
			Kind:       "OpenNebulaDatastore",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: inventoryv1alpha1.OpenNebulaDatastoreSpec{
			DisplayName: ds.Name,
			Enabled:     true,
			Validation: inventoryv1alpha1.OpenNebulaDatastoreValidationSpec{
				Mode: inventoryv1alpha1.DatastoreValidationModeDisabled,
			},
			Discovery: inventoryv1alpha1.OpenNebulaDatastoreDiscoverySpec{
				OpenNebulaDatastoreID: ds.ID,
				ExpectedBackend:       normalizeDatastoreBackend(ds),
				Allowed:               true,
			},
		},
	}
	return s.client.Create(ctx, obj)
}

func (s *Syncer) ensureNodeObject(ctx context.Context, node corev1.Node) error {
	current := &inventoryv1alpha1.OpenNebulaNode{}
	err := s.client.Get(ctx, types.NamespacedName{Name: node.Name}, current)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	obj := &inventoryv1alpha1.OpenNebulaNode{
		TypeMeta: metav1.TypeMeta{
			APIVersion: inventoryv1alpha1.SchemeGroupVersion.String(),
			Kind:       "OpenNebulaNode",
		},
		ObjectMeta: metav1.ObjectMeta{Name: node.Name},
		Spec: inventoryv1alpha1.OpenNebulaNodeSpec{
			OpenNebulaVMName: node.Name,
			Enabled:          true,
		},
	}
	return s.client.Create(ctx, obj)
}

func (s *Syncer) buildDatastoreStatus(item inventoryv1alpha1.OpenNebulaDatastore, ds datastoreSchema.Datastore, pvs []corev1.PersistentVolume, pvcByNSName map[string]corev1.PersistentVolumeClaim, scs []storagev1.StorageClass, vas []storagev1.VolumeAttachment) inventoryv1alpha1.OpenNebulaDatastoreStatus {
	normalized := normalizeDatastore(ds)
	status := inventoryv1alpha1.OpenNebulaDatastoreStatus{
		ObservedGeneration: item.Generation,
		Backend:            normalized.Backend,
		OpenNebula: inventoryv1alpha1.OpenNebulaDatastoreOpenNebulaStatus{
			ID:                         normalized.ID,
			Name:                       normalized.Name,
			State:                      datastoreStateString(ds),
			DSMad:                      normalized.DSMad,
			TMMad:                      normalized.TMMad,
			Type:                       normalized.Type,
			CompatibleSystemDatastores: append([]int(nil), normalized.CompatibleSystemDatastores...),
		},
		Capacity: inventoryv1alpha1.OpenNebulaDatastoreCapacityStatus{
			TotalBytes:      normalized.TotalBytes,
			FreeBytes:       normalized.FreeBytes,
			UsedBytesApprox: maxInt64(normalized.TotalBytes-normalized.FreeBytes, 0),
		},
	}

	storageClasses := make(map[string]struct{})
	attachedHandles := make(map[string]struct{})
	for _, va := range vas {
		if va.Spec.Source.PersistentVolumeName != nil {
			attachedHandles[*va.Spec.Source.PersistentVolumeName] = struct{}{}
		}
	}

	for _, pv := range pvs {
		pvDatastoreID, ok := datastoreIDFromPV(pv)
		if !ok || pvDatastoreID != normalized.ID {
			continue
		}
		status.Usage.PersistentVolumeCount++
		if quantity, ok := pv.Spec.Capacity[corev1.ResourceStorage]; ok {
			status.Capacity.ProvisionedPVBytes += quantity.Value()
		}
		if _, ok := attachedHandles[pv.Name]; ok {
			status.Usage.AttachedVolumeCount++
		}
		if pv.Spec.StorageClassName != "" {
			storageClasses[pv.Spec.StorageClassName] = struct{}{}
		}
		if pv.Spec.ClaimRef != nil {
			status.Usage.BoundPVCCount++
			status.Usage.BoundClaims = append(status.Usage.BoundClaims, inventoryv1alpha1.ObjectRefSummary{
				Namespace: pv.Spec.ClaimRef.Namespace,
				Name:      pv.Spec.ClaimRef.Name,
				UID:       string(pv.Spec.ClaimRef.UID),
			})
			if pvc, ok := pvcByNSName[pv.Spec.ClaimRef.Namespace+"/"+pv.Spec.ClaimRef.Name]; ok {
				if quantity, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
					status.Capacity.RequestedPVCBytes += quantity.Value()
				}
			}
		}
	}
	for _, sc := range scs {
		if storageClassUsesDatastore(sc, normalized.ID) {
			storageClasses[sc.Name] = struct{}{}
		}
	}
	status.Usage.StorageClasses = sortedKeys(storageClasses)

	phase := inventoryv1alpha1.DatastorePhaseReady
	if !item.Spec.Enabled || !item.Spec.Discovery.Allowed {
		phase = inventoryv1alpha1.DatastorePhaseDisabled
	}
	if item.Spec.Discovery.ExpectedBackend != "" && item.Spec.Discovery.ExpectedBackend != "unknown" && item.Spec.Discovery.ExpectedBackend != normalized.Backend {
		phase = inventoryv1alpha1.DatastorePhaseDegraded
	}
	status.Phase = phase
	status.Validation = item.Status.Validation
	status.Conditions = []metav1.Condition{
		newCondition("Discovered", metav1.ConditionTrue, "DatastoreFound", "Datastore was discovered in OpenNebula"),
		newCondition("ProvisioningEnabled", boolCondition(item.Spec.Enabled && item.Spec.Discovery.Allowed), "ProvisioningEvaluated", "Datastore provisioning eligibility evaluated"),
		newCondition("BackendMatch", boolCondition(item.Spec.Discovery.ExpectedBackend == "" || item.Spec.Discovery.ExpectedBackend == "unknown" || item.Spec.Discovery.ExpectedBackend == normalized.Backend), "BackendEvaluated", "Datastore backend compatibility evaluated"),
		newCondition("CapacityReported", metav1.ConditionTrue, "CapacityObserved", "Datastore capacity reported from OpenNebula"),
	}
	return status
}

func (s *Syncer) buildNodeStatus(ctx context.Context, item inventoryv1alpha1.OpenNebulaNode, node corev1.Node, vas []storagev1.VolumeAttachment, pvByName map[string]corev1.PersistentVolume, datastoreNames map[int]string) inventoryv1alpha1.OpenNebulaNodeStatus {
	status := inventoryv1alpha1.OpenNebulaNodeStatus{
		ObservedGeneration: item.Generation,
		Kubernetes: inventoryv1alpha1.OpenNebulaNodeKubernetesStatus{
			Name:       node.Name,
			UID:        string(node.UID),
			ProviderID: node.Spec.ProviderID,
			Labels:     filteredNodeLabels(node.Labels),
			Taints:     append([]corev1.Taint(nil), node.Spec.Taints...),
			Addresses:  append([]corev1.NodeAddress(nil), node.Status.Addresses...),
			Conditions: convertNodeConditions(node.Status.Conditions),
		},
		Storage: inventoryv1alpha1.OpenNebulaNodeStorageStatus{
			TopologySystemDatastore: node.Labels[topologySystemDSLabel],
		},
	}

	vmName := item.Spec.OpenNebulaVMName
	if strings.TrimSpace(vmName) == "" {
		vmName = node.Name
	}
	vmID, err := s.ctrl.VMs().ByName(vmName)
	if err != nil {
		status.Phase = inventoryv1alpha1.NodePhaseNotFound
		status.Conditions = []metav1.Condition{
			newCondition("KubernetesNodeFound", metav1.ConditionTrue, "NodeFound", "Kubernetes node was found"),
			newCondition("OpenNebulaVMFound", metav1.ConditionFalse, "VMNotFound", "OpenNebula VM was not found"),
		}
		return status
	}
	vmInfo, err := s.ctrl.VM(vmID).InfoContext(ctx, true)
	if err != nil {
		status.Phase = inventoryv1alpha1.NodePhaseDegraded
		status.Conditions = []metav1.Condition{
			newCondition("KubernetesNodeFound", metav1.ConditionTrue, "NodeFound", "Kubernetes node was found"),
			newCondition("OpenNebulaVMFound", metav1.ConditionFalse, "VMLookupFailed", "OpenNebula VM lookup failed"),
		}
		return status
	}
	vmState, vmLCM, _ := vmInfo.State()
	systemDSID, _ := latestHistoryDatastoreIDForInventory(vmInfo)
	hostID := 0
	hostName := ""
	if len(vmInfo.HistoryRecords) > 0 {
		latest := vmInfo.HistoryRecords[len(vmInfo.HistoryRecords)-1]
		hostID = latest.HID
		hostName = latest.Hostname
	}
	status.OpenNebula = inventoryv1alpha1.OpenNebulaNodeOpenNebulaStatus{
		VMID:                vmInfo.ID,
		VMName:              vmInfo.Name,
		State:               vmState.String(),
		LCMState:            vmLCM.String(),
		HostID:              hostID,
		HostName:            hostName,
		SystemDatastoreID:   systemDSID,
		SystemDatastoreName: datastoreNames[systemDSID],
	}
	status.Hotplug.Ready = vmLCM == vmSchema.Running
	status.Hotplug.InCooldown = false

	for _, va := range vas {
		if va.Spec.NodeName != node.Name || va.Spec.Source.PersistentVolumeName == nil {
			continue
		}
		pv, ok := pvByName[*va.Spec.Source.PersistentVolumeName]
		if !ok || pv.Spec.CSI == nil {
			continue
		}
		status.Storage.AttachedVolumeCount++
		status.Storage.AttachedVolumeIDs = append(status.Storage.AttachedVolumeIDs, pv.Spec.CSI.VolumeHandle)
		diskID := persistentDiskIDFromVM(vmInfo, pv.Spec.CSI.VolumeHandle)
		summary := inventoryv1alpha1.AttachedDiskSummary{
			VolumeHandle:     pv.Spec.CSI.VolumeHandle,
			PersistentDiskID: diskID,
		}
		if pv.Spec.ClaimRef != nil {
			summary.ClaimNamespace = pv.Spec.ClaimRef.Namespace
			summary.ClaimName = pv.Spec.ClaimRef.Name
		}
		status.Storage.AttachedPersistentDisks = append(status.Storage.AttachedPersistentDisks, summary)
	}

	phase := inventoryv1alpha1.NodePhaseReady
	if !item.Spec.Enabled {
		phase = inventoryv1alpha1.NodePhaseDisabled
	} else if !status.Hotplug.Ready {
		phase = inventoryv1alpha1.NodePhaseDegraded
	}
	status.Phase = phase
	status.Conditions = []metav1.Condition{
		newCondition("KubernetesNodeFound", metav1.ConditionTrue, "NodeFound", "Kubernetes node was found"),
		newCondition("OpenNebulaVMFound", metav1.ConditionTrue, "VMFound", "OpenNebula VM was found"),
		newCondition("VMReady", boolCondition(status.Hotplug.Ready), "VMReadinessEvaluated", "OpenNebula VM readiness evaluated"),
		newCondition("TopologyResolved", boolCondition(status.Storage.TopologySystemDatastore != ""), "TopologyEvaluated", "Topology label evaluated"),
	}
	return status
}

func (s *Syncer) reconcileValidation(ctx context.Context, ds *inventoryv1alpha1.OpenNebulaDatastore) error {
	if ds.Spec.Validation.Mode != inventoryv1alpha1.DatastoreValidationModeManual {
		return nil
	}
	if strings.TrimSpace(ds.Spec.Validation.RunNonce) == "" {
		return nil
	}
	if ds.Spec.Validation.RunNonce == ds.Status.Validation.LastRunNonce && ds.Status.Validation.Phase != inventoryv1alpha1.ValidationPhaseRunning {
		return nil
	}

	jobName := validationResourceName(validationJobPrefix, ds.Name, ds.Spec.Validation.RunNonce)
	pvcName := validationResourceName(validationPVCPrefix, ds.Name, ds.Spec.Validation.RunNonce)
	pvc := &corev1.PersistentVolumeClaim{}
	err := s.client.Get(ctx, types.NamespacedName{Namespace: s.namespace, Name: pvcName}, pvc)
	if err != nil && apierrors.IsNotFound(err) {
		if err := s.client.Create(ctx, s.buildValidationPVC(ds, pvcName)); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	job := &batchv1.Job{}
	err = s.client.Get(ctx, types.NamespacedName{Namespace: s.namespace, Name: jobName}, job)
	if err != nil && apierrors.IsNotFound(err) {
		if err := s.client.Create(ctx, s.buildValidationJob(ds, jobName, pvcName)); err != nil {
			return err
		}
		now := metav1.Now()
		ds.Status.Validation.LastRunTime = &now
		ds.Status.Validation.LastRunNonce = ds.Spec.Validation.RunNonce
		ds.Status.Validation.Phase = inventoryv1alpha1.ValidationPhaseRunning
		ds.Status.Validation.JobName = jobName
		ds.Status.Validation.Message = "validation job created"
		return s.client.Status().Update(ctx, ds)
	} else if err != nil {
		return err
	}

	if job.Status.Succeeded > 0 {
		now := metav1.Now()
		ds.Status.Validation.LastCompletedTime = &now
		ds.Status.Validation.LastRunNonce = ds.Spec.Validation.RunNonce
		ds.Status.Validation.Phase = inventoryv1alpha1.ValidationPhaseSucceeded
		ds.Status.Validation.Message = "validation completed successfully"
		if result, err := s.parseValidationJobLogs(ctx, job); err == nil {
			ds.Status.Validation.Result = result
		}
		if err := s.client.Status().Update(ctx, ds); err != nil {
			return err
		}
		return s.client.Delete(ctx, pvc)
	}
	if job.Status.Failed > 0 {
		now := metav1.Now()
		ds.Status.Validation.LastCompletedTime = &now
		ds.Status.Validation.LastRunNonce = ds.Spec.Validation.RunNonce
		ds.Status.Validation.Phase = inventoryv1alpha1.ValidationPhaseFailed
		ds.Status.Validation.Message = "validation failed"
		if err := s.client.Status().Update(ctx, ds); err != nil {
			return err
		}
		return s.client.Delete(ctx, pvc)
	}

	ds.Status.Validation.Phase = inventoryv1alpha1.ValidationPhaseRunning
	ds.Status.Validation.JobName = jobName
	return s.client.Status().Update(ctx, ds)
}

func (s *Syncer) buildValidationPVC(ds *inventoryv1alpha1.OpenNebulaDatastore, pvcName string) *corev1.PersistentVolumeClaim {
	size := ds.Spec.Validation.JobTemplate.Size
	if strings.TrimSpace(size) == "" {
		size = "4Gi"
	}
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: s.namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: stringPtr(ds.Spec.Validation.JobTemplate.StorageClassName),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resourceMustParse(size),
				},
			},
		},
	}
}

func (s *Syncer) buildValidationJob(ds *inventoryv1alpha1.OpenNebulaDatastore, jobName, pvcName string) *batchv1.Job {
	image := ds.Spec.Validation.JobTemplate.Image
	if strings.TrimSpace(image) == "" {
		image = s.defaultValidationImage
	}
	policy := ds.Spec.Validation.JobTemplate.ImagePullPolicy
	if policy == "" {
		policy = defaultValidationImagePullPolicy
	}
	ttl := ds.Spec.Validation.JobTemplate.TTLSecondsAfterFinished
	command := []string{"/bin/sh", "-c"}
	args := []string{buildValidationCommand(ds.Spec.Validation.JobTemplate.FioArgs)}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: s.namespace,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ttl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					NodeSelector:  ds.Spec.Validation.JobTemplate.NodeSelector,
					Tolerations:   ds.Spec.Validation.JobTemplate.Tolerations,
					Containers: []corev1.Container{{
						Name:            "fio",
						Image:           image,
						ImagePullPolicy: policy,
						Command:         command,
						Args:            args,
						Resources:       ds.Spec.Validation.JobTemplate.Resources,
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "data",
							MountPath: "/data",
						}},
					}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvcName,
							},
						},
					}},
				},
			},
		},
	}
}

func (s *Syncer) parseValidationJobLogs(ctx context.Context, job *batchv1.Job) (inventoryv1alpha1.ValidationResult, error) {
	pods, err := s.kube.CoreV1().Pods(job.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "job-name=" + job.Name,
	})
	if err != nil || len(pods.Items) == 0 {
		return inventoryv1alpha1.ValidationResult{}, fmt.Errorf("validation pod was not found")
	}
	req := s.kube.CoreV1().Pods(job.Namespace).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{Container: "fio"})
	stream, err := req.Stream(ctx)
	if err != nil {
		return inventoryv1alpha1.ValidationResult{}, err
	}
	defer stream.Close()
	body, err := io.ReadAll(stream)
	if err != nil {
		return inventoryv1alpha1.ValidationResult{}, err
	}
	return parseFIOResult(body)
}

func parseFIOResult(payload []byte) (inventoryv1alpha1.ValidationResult, error) {
	var decoded struct {
		Jobs []struct {
			Read struct {
				IOPS    float64 `json:"iops"`
				BWBytes int64   `json:"bw_bytes"`
				ClatNS  struct {
					Percentile map[string]float64 `json:"percentile"`
				} `json:"clat_ns"`
			} `json:"read"`
			Write struct {
				IOPS    float64 `json:"iops"`
				BWBytes int64   `json:"bw_bytes"`
				ClatNS  struct {
					Percentile map[string]float64 `json:"percentile"`
				} `json:"clat_ns"`
			} `json:"write"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return inventoryv1alpha1.ValidationResult{}, err
	}
	if len(decoded.Jobs) == 0 {
		return inventoryv1alpha1.ValidationResult{}, fmt.Errorf("fio result did not include jobs")
	}
	job := decoded.Jobs[0]
	readIops := int64(job.Read.IOPS)
	writeIops := int64(job.Write.IOPS)
	readP50 := percentileMicros(job.Read.ClatNS.Percentile, "50.000000")
	readP99 := percentileMicros(job.Read.ClatNS.Percentile, "99.000000")
	return inventoryv1alpha1.ValidationResult{
		ReadIops:         &readIops,
		WriteIops:        &writeIops,
		ReadBwBytes:      &job.Read.BWBytes,
		WriteBwBytes:     &job.Write.BWBytes,
		LatencyP50Micros: &readP50,
		LatencyP99Micros: &readP99,
	}, nil
}

func buildValidationCommand(args []string) string {
	defaultArgs := []string{
		"--name=validate",
		"--filename=/data/validation.bin",
		"--rw=randrw",
		"--bs=4k",
		"--size=256Mi",
		"--iodepth=16",
		"--runtime=30",
		"--time_based",
	}
	if len(args) == 0 {
		args = defaultArgs
	}
	return "set -eu; fio " + strings.Join(args, " ") + " --output-format=json"
}

func datastoreObjectName(id int) string {
	return fmt.Sprintf("ds-%d", id)
}

func validationResourceName(prefix, name, nonce string) string {
	hash := sha1.Sum([]byte(name + ":" + nonce))
	suffix := hex.EncodeToString(hash[:])[:8]
	trimmed := name
	if len(trimmed) > 40 {
		trimmed = trimmed[:40]
	}
	trimmed = strings.Trim(trimmed, "-")
	return prefix + trimmed + "-" + suffix
}

func normalizeDatastore(ds datastoreSchema.Datastore) opennebula.Datastore {
	return opennebula.Datastore{
		ID:                         ds.ID,
		Name:                       ds.Name,
		Type:                       normalizeDatastoreBackend(ds),
		Backend:                    normalizeDatastoreBackend(ds),
		DSMad:                      strings.ToLower(strings.TrimSpace(ds.DSMad)),
		TMMad:                      strings.ToLower(strings.TrimSpace(ds.TMMad)),
		FreeBytes:                  int64(ds.FreeMB) * 1024 * 1024,
		TotalBytes:                 int64(ds.TotalMB) * 1024 * 1024,
		CompatibleSystemDatastores: compatibleSystemDatastoresForInventory(ds),
	}
}

func normalizeDatastoreBackend(ds datastoreSchema.Datastore) string {
	normalized := strings.ToLower(strings.TrimSpace(ds.TMMad))
	switch normalized {
	case "ceph":
		return "ceph"
	case "cephfs":
		return "cephfs"
	default:
		return "local"
	}
}

func compatibleSystemDatastoresForInventory(ds datastoreSchema.Datastore) []int {
	values := make([]int, 0)
	compatibleRaw, _ := ds.Template.GetStr("COMPATIBLE_SYS_DS")
	for _, candidate := range strings.Split(strings.TrimSpace(compatibleRaw), ",") {
		trimmed := strings.TrimSpace(candidate)
		if trimmed == "" {
			continue
		}
		id, err := strconv.Atoi(trimmed)
		if err != nil {
			continue
		}
		values = append(values, id)
	}
	return values
}

func datastoreStateString(ds datastoreSchema.Datastore) string {
	state, err := ds.State()
	if err != nil {
		return "unknown"
	}
	return state.String()
}

func datastoreIDFromPV(pv corev1.PersistentVolume) (int, bool) {
	if pv.Annotations == nil {
		return 0, false
	}
	value := strings.TrimSpace(pv.Annotations[annotationDatastoreID])
	if value == "" {
		return 0, false
	}
	id, err := strconv.Atoi(value)
	return id, err == nil
}

func storageClassUsesDatastore(sc storagev1.StorageClass, datastoreID int) bool {
	identifiers := strings.Split(sc.Parameters["datastoreIDs"], ",")
	for _, identifier := range identifiers {
		id, err := strconv.Atoi(strings.TrimSpace(identifier))
		if err == nil && id == datastoreID {
			return true
		}
	}
	return false
}

func filteredNodeLabels(labels map[string]string) map[string]string {
	filtered := make(map[string]string)
	for key, value := range labels {
		switch {
		case strings.HasPrefix(key, "kubernetes.io/"),
			strings.HasPrefix(key, "node.kubernetes.io/"),
			strings.HasPrefix(key, "topology.kubernetes.io/"),
			strings.HasPrefix(key, "topology.opennebula.sparkaiur.io/"):
			filtered[key] = value
		}
	}
	return filtered
}

func convertNodeConditions(conditions []corev1.NodeCondition) []inventoryv1alpha1.NodeConditionSummary {
	converted := make([]inventoryv1alpha1.NodeConditionSummary, 0, len(conditions))
	for _, condition := range conditions {
		converted = append(converted, inventoryv1alpha1.NodeConditionSummary{
			Type:               string(condition.Type),
			Status:             string(condition.Status),
			Reason:             condition.Reason,
			Message:            condition.Message,
			LastTransitionTime: condition.LastTransitionTime,
		})
	}
	return converted
}

func newCondition(conditionType string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}
}

func boolCondition(value bool) metav1.ConditionStatus {
	if value {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func latestHistoryDatastoreIDForInventory(vmInfo *vmSchema.VM) (int, error) {
	var latest *vmSchema.HistoryRecord
	for idx := range vmInfo.HistoryRecords {
		record := &vmInfo.HistoryRecords[idx]
		if record.DSID < 0 {
			continue
		}
		if latest == nil || record.SEQ > latest.SEQ {
			latest = record
		}
	}
	if latest == nil {
		return 0, fmt.Errorf("vm %d has no datastore history", vmInfo.ID)
	}
	return latest.DSID, nil
}

func persistentDiskIDFromVM(vmInfo *vmSchema.VM, volumeHandle string) int {
	for _, disk := range vmInfo.Template.GetDisks() {
		imageID, err := disk.GetI(shared.ImageID)
		if err != nil {
			continue
		}
		if volumeHandle != fmt.Sprintf("pvc-%d", imageID) {
			// best effort only; most CSI volume handles are PVC IDs rather than image IDs
			continue
		}
		return imageID
	}
	return 0
}

func percentileMicros(values map[string]float64, key string) int64 {
	if values == nil {
		return 0
	}
	return int64(values[key] / 1000.0)
}

func stringPtr(value string) *string {
	return &value
}

func resourceMustParse(value string) resource.Quantity {
	return resource.MustParse(value)
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func loadInventoryLeaderElectionID(cfg config.CSIPluginConfig) string {
	value, ok := cfg.GetString(config.InventoryControllerLeaderElectionIDVar)
	if !ok || strings.TrimSpace(value) == "" {
		return "opennebula-csi-inventory-controller"
	}
	return strings.TrimSpace(value)
}

func loadResync(cfg config.CSIPluginConfig, key string, fallback int) int {
	value, ok := cfg.GetInt(key)
	if !ok || value <= 0 {
		return fallback
	}
	return value
}
