package driver

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/releaseutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// This lifecycle fixture applies Helm3.21.4's cleanup semantics to actual rendered
// hooks parsed by Helm. It is contract evidence, not a live Helm/Kubernetes run.
func hookFailureLifecycleFixture(t *testing.T, objects map[string][]byte, store map[string]string, failedComponent string) []*release.Hook {
	t.Helper()
	files := map[string]string{}
	for key, raw := range objects {
		files[key] = string(raw)
	}
	parsed, _, err := releaseutil.SortManifests(files, chartutil.DefaultCapabilities.APIVersions, releaseutil.InstallOrder)
	require.NoError(t, err)
	hooks := []*release.Hook{}
	for _, hook := range parsed {
		for _, event := range hook.Events {
			if event == release.HookPreUpgrade {
				hooks = append(hooks, hook)
				break
			}
		}
	}
	sort.SliceStable(hooks, func(i, j int) bool {
		if hooks[i].Weight == hooks[j].Weight {
			return hooks[i].Name < hooks[j].Name
		}
		return hooks[i].Weight < hooks[j].Weight
	})
	hasPolicy := func(hook *release.Hook, policy release.HookDeletePolicy) bool {
		for _, current := range hook.DeletePolicies {
			if current == policy {
				return true
			}
		}
		return false
	}
	require.Len(t, hooks, 6)
	completed := []*release.Hook{}
	for _, hook := range hooks {
		// Helm adds before-hook-creation when no explicit policy is present.
		if len(hook.DeletePolicies) == 0 {
			hook.DeletePolicies = []release.HookDeletePolicy{release.HookBeforeHookCreation}
		}
		key := hook.Kind + "/" + hook.Name
		if hasPolicy(hook, release.HookBeforeHookCreation) {
			delete(store, key)
		}
		_, exists := store[key]
		require.False(t, exists, "same failed revision must never be reused: %s", key)
		store[key] = hook.Manifest
		var job batchv1.Job
		if hook.Kind == "Job" {
			require.NoError(t, yaml.Unmarshal([]byte(hook.Manifest), &job))
		}
		if hook.Kind == "Job" && job.Labels["app.kubernetes.io/component"] == failedComponent {
			if hasPolicy(hook, release.HookFailed) {
				delete(store, key)
			}
			// Helm cleans earlier successful hooks on a later hook failure.
			for _, earlier := range completed {
				if hasPolicy(earlier, release.HookSucceeded) {
					delete(store, earlier.Kind+"/"+earlier.Name)
				}
			}
			return hooks
		}
		completed = append(completed, hook)
	}
	t.Fatalf("failure component %s was not executed", failedComponent)
	return nil
}

func TestHookLifecycleFixturePreservesRC1FailureAndLaterInputs(t *testing.T) {
	store := map[string]string{}
	legacyName := "fixture-opennebula-csi-preflight"
	legacyInput := "fixture-opennebula-csi-storageclass-reconcile"
	oldJob := batchv1.Job{TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: metav1.ObjectMeta{Name: legacyName, Namespace: "fixture", UID: "rc1-job"}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{ServiceAccountName: legacyName}}}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}}
	oldObjects := map[string]any{
		"Job/" + legacyName:                oldJob,
		"ServiceAccount/" + legacyName:     corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: legacyName, Namespace: "fixture"}},
		"ClusterRole/" + legacyName:        rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: legacyName}},
		"ClusterRoleBinding/" + legacyName: rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: legacyName}, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: legacyName}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: legacyName, Namespace: "fixture"}}},
		"ConfigMap/" + legacyInput:         corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: legacyInput, Namespace: "fixture"}, Data: map[string]string{"storageclasses.json": `[{"name":"rc1-preserved-input"}]`}},
		"Pod/rc1-failed-pod":               corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "rc1-failed-pod", Namespace: "fixture", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: legacyName, UID: "rc1-job"}}}, Status: corev1.PodStatus{Phase: corev1.PodFailed}},
	}
	for key, object := range oldObjects {
		raw, err := json.Marshal(object)
		require.NoError(t, err)
		store[key] = string(raw)
	}
	initial := map[string]string{}
	for key, value := range store {
		initial[key] = value
	}
	previousInputs := map[string]string{}
	for index, revision := range []int{20, 21} {
		objects := renderRevisionHooks(t, "fixture", revision, true)
		failComponent := "preflight"
		if index == 1 {
			failComponent = "storageclass-reconcile"
		}
		hooks := hookFailureLifecycleFixture(t, objects, store, failComponent)
		for key, old := range initial {
			require.Equal(t, old, store[key], "RC1 failed resource/input changed: %s", key)
		}
		for key, old := range previousInputs {
			require.Equal(t, old, store[key], "earlier RC2 failed revision input changed: %s", key)
		}
		failedJobFound := false
		for _, hook := range hooks {
			key := hook.Kind + "/" + hook.Name
			var metadata struct {
				Metadata struct{ Labels map[string]string }
			}
			require.NoError(t, yaml.Unmarshal([]byte(hook.Manifest), &metadata))
			if hook.Kind == "Job" && metadata.Metadata.Labels["app.kubernetes.io/component"] == failComponent {
				failedJobFound = true
				require.Contains(t, store, key)
				var job batchv1.Job
				require.NoError(t, yaml.Unmarshal([]byte(store[key]), &job))
				require.Nil(t, job.Spec.TTLSecondsAfterFinished)
				if failComponent == "preflight" {
					require.Contains(t, store, "ServiceAccount/"+job.Spec.Template.Spec.ServiceAccountName)
				}
				if failComponent == "storageclass-reconcile" {
					for _, arg := range job.Spec.Template.Spec.Containers[0].Args {
						if name, ok := strings.CutPrefix(arg, "--storageclass-reconcile-configmap="); ok {
							require.Contains(t, store, "ConfigMap/"+name)
						}
					}
				}
			}
			if hook.Kind == "ConfigMap" || hook.Kind == "ServiceAccount" || hook.Kind == "ClusterRole" || hook.Kind == "ClusterRoleBinding" {
				require.Contains(t, store, key, "successful hook input/bootstrap must survive later Job failure")
				previousInputs[key] = store[key]
				require.Contains(t, hook.Name, fmt.Sprintf("-r%d", revision))
				if hook.Kind == "ClusterRoleBinding" {
					var binding rbacv1.ClusterRoleBinding
					require.NoError(t, yaml.Unmarshal([]byte(store[key]), &binding))
					require.Contains(t, store, "ClusterRole/"+binding.RoleRef.Name)
					require.Contains(t, store, "ServiceAccount/"+binding.Subjects[0].Name)
				}
			}
		}
		require.True(t, failedJobFound)
	}
}
