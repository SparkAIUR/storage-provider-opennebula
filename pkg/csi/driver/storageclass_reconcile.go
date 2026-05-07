package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	storageClassReconcileDataKey = "storageclasses.json"

	storageClassAnnotationChartManaged     = "storage-provider.opennebula.sparkaiur.io/chart-managed"
	storageClassAnnotationReleaseName      = "storage-provider.opennebula.sparkaiur.io/release-name"
	storageClassAnnotationReleaseNamespace = "storage-provider.opennebula.sparkaiur.io/release-namespace"
	storageClassAnnotationSpecHash         = "storage-provider.opennebula.sparkaiur.io/spec-hash"
	storageClassAnnotationAppliedSpecHash  = "storage-provider.opennebula.sparkaiur.io/applied-spec-hash"

	storageClassManualMutationPolicyFail = "fail"
	storageClassManualMutationPolicySkip = "skip"
)

type StorageClassReconcileOptions struct {
	Namespace            string
	ConfigMapName        string
	ManualMutationPolicy string
	AdoptUnannotated     bool
}

type StorageClassReconcileDesired struct {
	Name              string            `json:"name"`
	ReleaseName       string            `json:"releaseName"`
	ReleaseNamespace  string            `json:"releaseNamespace"`
	SpecHash          string            `json:"specHash,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Provisioner       string            `json:"provisioner"`
	ReclaimPolicy     string            `json:"reclaimPolicy,omitempty"`
	AllowExpansion    bool              `json:"allowVolumeExpansion"`
	MountOptions      []string          `json:"mountOptions,omitempty"`
	VolumeBindingMode string            `json:"volumeBindingMode,omitempty"`
	Parameters        map[string]string `json:"parameters,omitempty"`
}

type storageClassSpecFingerprint struct {
	Provisioner       string            `json:"provisioner"`
	ReclaimPolicy     string            `json:"reclaimPolicy,omitempty"`
	AllowExpansion    bool              `json:"allowVolumeExpansion"`
	MountOptions      []string          `json:"mountOptions,omitempty"`
	VolumeBindingMode string            `json:"volumeBindingMode,omitempty"`
	Parameters        map[string]string `json:"parameters,omitempty"`
}

type storageClassReconcileResult struct {
	Name   string `json:"name"`
	Action string `json:"action"`
	Reason string `json:"reason,omitempty"`
}

func RunStorageClassReconcileCommand(ctx context.Context, opts StorageClassReconcileOptions, w io.Writer) error {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	return RunStorageClassReconcile(ctx, client, opts, w)
}

func RunStorageClassReconcile(ctx context.Context, client kubernetes.Interface, opts StorageClassReconcileOptions, w io.Writer) error {
	if client == nil {
		return fmt.Errorf("kubernetes client is required")
	}
	opts.Namespace = strings.TrimSpace(opts.Namespace)
	opts.ConfigMapName = strings.TrimSpace(opts.ConfigMapName)
	if opts.Namespace == "" {
		opts.Namespace = namespaceFromServiceAccount()
	}
	if opts.ConfigMapName == "" {
		return fmt.Errorf("storageclass reconcile configmap name is required")
	}
	policy := normalizeStorageClassManualMutationPolicy(opts.ManualMutationPolicy)
	cm, err := client.CoreV1().ConfigMaps(opts.Namespace).Get(ctx, opts.ConfigMapName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	desired, err := decodeStorageClassReconcileDesired(cm.Data[storageClassReconcileDataKey])
	if err != nil {
		return err
	}
	results := make([]storageClassReconcileResult, 0, len(desired))
	for _, item := range desired {
		result, err := reconcileOneStorageClass(ctx, client, item, policy, opts.AdoptUnannotated)
		if result.Name != "" {
			results = append(results, result)
		}
		if err != nil {
			writeStorageClassReconcileResults(w, results)
			return err
		}
	}
	writeStorageClassReconcileResults(w, results)
	return nil
}

func reconcileOneStorageClass(ctx context.Context, client kubernetes.Interface, desired StorageClassReconcileDesired, policy string, adoptUnannotated bool) (storageClassReconcileResult, error) {
	desired.Name = strings.TrimSpace(desired.Name)
	if desired.Name == "" {
		return storageClassReconcileResult{}, fmt.Errorf("desired storageclass name is required")
	}
	desiredHash := strings.TrimSpace(desired.SpecHash)
	if desiredHash == "" {
		desiredHash = storageClassDesiredSpecHash(desired)
	}
	result := storageClassReconcileResult{Name: desired.Name}
	live, err := client.StorageV1().StorageClasses().Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return result, err
		}
		if _, createErr := client.StorageV1().StorageClasses().Create(ctx, desiredStorageClassObject(desired, desiredHash), metav1.CreateOptions{}); createErr != nil {
			return result, createErr
		}
		result.Action = "created"
		return result, nil
	}

	liveHash := storageClassLiveSpecHash(live)
	appliedHash := strings.TrimSpace(live.Annotations[storageClassAnnotationAppliedSpecHash])
	owned := storageClassOwnedByRelease(live, desired.ReleaseName, desired.ReleaseNamespace)
	if !owned {
		if adoptUnannotated && appliedHash == "" && liveHash == desiredHash {
			updated := live.DeepCopy()
			ensureStorageClassReconcileAnnotations(updated, desired, desiredHash)
			if _, err := client.StorageV1().StorageClasses().Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
				return result, err
			}
			result.Action = "adopted"
			result.Reason = "live spec matched desired chart spec"
			return result, nil
		}
		result.Action = "blocked"
		result.Reason = "not chart-owned"
		if policy == storageClassManualMutationPolicySkip {
			return result, nil
		}
		return result, fmt.Errorf("storageclass %s is not chart-owned by release %s/%s", desired.Name, desired.ReleaseNamespace, desired.ReleaseName)
	}
	if appliedHash == "" {
		result.Action = "blocked"
		result.Reason = "missing applied spec hash"
		if policy == storageClassManualMutationPolicySkip {
			return result, nil
		}
		return result, fmt.Errorf("storageclass %s is chart-owned but missing %s", desired.Name, storageClassAnnotationAppliedSpecHash)
	}
	if liveHash != appliedHash {
		result.Action = "blocked"
		result.Reason = "manual mutation detected"
		if policy == storageClassManualMutationPolicySkip {
			return result, nil
		}
		return result, fmt.Errorf("storageclass %s was manually modified: live spec hash %s does not match applied chart hash %s", desired.Name, liveHash, appliedHash)
	}
	if desiredHash == appliedHash {
		result.Action = "unchanged"
		return result, nil
	}
	if err := client.StorageV1().StorageClasses().Delete(ctx, desired.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return result, err
	}
	if _, err := client.StorageV1().StorageClasses().Create(ctx, desiredStorageClassObject(desired, desiredHash), metav1.CreateOptions{}); err != nil {
		return result, err
	}
	result.Action = "recreated"
	result.Reason = "chart spec hash changed"
	return result, nil
}

func decodeStorageClassReconcileDesired(raw string) ([]StorageClassReconcileDesired, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var desired []StorageClassReconcileDesired
	if err := json.Unmarshal([]byte(raw), &desired); err != nil {
		return nil, fmt.Errorf("failed to decode storageclass reconcile payload: %w", err)
	}
	return desired, nil
}

func desiredStorageClassObject(desired StorageClassReconcileDesired, desiredHash string) *storagev1.StorageClass {
	annotations := cloneStringMap(desired.Annotations)
	labels := cloneStringMap(desired.Labels)
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:        strings.TrimSpace(desired.Name),
			Labels:      labels,
			Annotations: annotations,
		},
		Provisioner:  firstNonEmpty(strings.TrimSpace(desired.Provisioner), DefaultDriverName),
		Parameters:   cloneStringMap(desired.Parameters),
		MountOptions: normalizeStorageClassMountOptions(desired.MountOptions),
	}
	if reclaim := strings.TrimSpace(desired.ReclaimPolicy); reclaim != "" {
		value := corev1.PersistentVolumeReclaimPolicy(reclaim)
		sc.ReclaimPolicy = &value
	}
	allow := desired.AllowExpansion
	sc.AllowVolumeExpansion = &allow
	if mode := strings.TrimSpace(desired.VolumeBindingMode); mode != "" {
		value := storagev1.VolumeBindingMode(mode)
		sc.VolumeBindingMode = &value
	}
	ensureStorageClassReconcileAnnotations(sc, desired, desiredHash)
	return sc
}

func ensureStorageClassReconcileAnnotations(sc *storagev1.StorageClass, desired StorageClassReconcileDesired, desiredHash string) {
	if sc.Annotations == nil {
		sc.Annotations = map[string]string{}
	}
	sc.Annotations[storageClassAnnotationChartManaged] = "true"
	sc.Annotations[storageClassAnnotationReleaseName] = strings.TrimSpace(desired.ReleaseName)
	sc.Annotations[storageClassAnnotationReleaseNamespace] = strings.TrimSpace(desired.ReleaseNamespace)
	sc.Annotations[storageClassAnnotationSpecHash] = desiredHash
	sc.Annotations[storageClassAnnotationAppliedSpecHash] = desiredHash
}

func storageClassOwnedByRelease(sc *storagev1.StorageClass, releaseName, releaseNamespace string) bool {
	if sc == nil || sc.Annotations == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(sc.Annotations[storageClassAnnotationChartManaged]), "true") &&
		strings.TrimSpace(sc.Annotations[storageClassAnnotationReleaseName]) == strings.TrimSpace(releaseName) &&
		strings.TrimSpace(sc.Annotations[storageClassAnnotationReleaseNamespace]) == strings.TrimSpace(releaseNamespace)
}

func storageClassDesiredSpecHash(desired StorageClassReconcileDesired) string {
	return storageClassSpecHash(storageClassSpecFingerprint{
		Provisioner:       firstNonEmpty(strings.TrimSpace(desired.Provisioner), DefaultDriverName),
		ReclaimPolicy:     strings.TrimSpace(desired.ReclaimPolicy),
		AllowExpansion:    desired.AllowExpansion,
		MountOptions:      normalizeStorageClassMountOptions(desired.MountOptions),
		VolumeBindingMode: strings.TrimSpace(desired.VolumeBindingMode),
		Parameters:        cloneStringMap(desired.Parameters),
	})
}

func storageClassLiveSpecHash(sc *storagev1.StorageClass) string {
	if sc == nil {
		return ""
	}
	reclaim := ""
	if sc.ReclaimPolicy != nil {
		reclaim = string(*sc.ReclaimPolicy)
	}
	allow := false
	if sc.AllowVolumeExpansion != nil {
		allow = *sc.AllowVolumeExpansion
	}
	mode := ""
	if sc.VolumeBindingMode != nil {
		mode = string(*sc.VolumeBindingMode)
	}
	return storageClassSpecHash(storageClassSpecFingerprint{
		Provisioner:       strings.TrimSpace(sc.Provisioner),
		ReclaimPolicy:     reclaim,
		AllowExpansion:    allow,
		MountOptions:      normalizeStorageClassMountOptions(sc.MountOptions),
		VolumeBindingMode: mode,
		Parameters:        cloneStringMap(sc.Parameters),
	})
}

func storageClassSpecHash(spec storageClassSpecFingerprint) string {
	if spec.Parameters == nil {
		spec.Parameters = map[string]string{}
	}
	spec.MountOptions = normalizeStorageClassMountOptions(spec.MountOptions)
	payload, err := json.Marshal(map[string]interface{}{
		"allowVolumeExpansion": spec.AllowExpansion,
		"mountOptions":         spec.MountOptions,
		"parameters":           spec.Parameters,
		"provisioner":          spec.Provisioner,
		"reclaimPolicy":        spec.ReclaimPolicy,
		"volumeBindingMode":    spec.VolumeBindingMode,
	})
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func normalizeStorageClassMountOptions(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	return append([]string(nil), values...)
}

func normalizeStorageClassManualMutationPolicy(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case storageClassManualMutationPolicySkip:
		return storageClassManualMutationPolicySkip
	default:
		return storageClassManualMutationPolicyFail
	}
}

func writeStorageClassReconcileResults(w io.Writer, results []storageClassReconcileResult) {
	if w == nil {
		return
	}
	for _, result := range results {
		if result.Reason != "" {
			fmt.Fprintf(w, "storageclass %s: %s (%s)\n", result.Name, result.Action, result.Reason)
		} else {
			fmt.Fprintf(w, "storageclass %s: %s\n", result.Name, result.Action)
		}
	}
}
