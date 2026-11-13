package driver

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

func renderRevisionHooks(t *testing.T, name string, revision int, mandatory bool, chartVersion ...string) map[string][]byte {
	t.Helper()
	chart, err := loader.Load("../../../helm/opennebula-csi")
	require.NoError(t, err)
	if len(chartVersion) > 0 {
		chart.Metadata.Version = chartVersion[0]
	}
	values := map[string]interface{}{"credentials": map[string]interface{}{"existingSecret": map[string]interface{}{"name": "fixture"}}, "preflight": map[string]interface{}{"enabled": true, "failReleaseOnError": mandatory}, "storageClassReconcile": map[string]interface{}{"enabled": true}, "storageClasses": []interface{}{map[string]interface{}{"name": "fixture", "parameters": map[string]interface{}{"datastoreIDs": "42"}}}}
	ctx, err := chartutil.ToRenderValues(chart, values, chartutil.ReleaseOptions{Name: name, Namespace: "fixture", Revision: revision, IsUpgrade: true}, chartutil.DefaultCapabilities)
	require.NoError(t, err)
	rendered, err := engine.Render(chart, ctx)
	require.NoError(t, err)
	out := map[string][]byte{}
	for _, raw := range rendered {
		for _, doc := range strings.Split(raw, "\n---") {
			var meta struct {
				Kind     string
				Metadata struct{ Name string }
			}
			require.NoError(t, yaml.Unmarshal([]byte(doc), &meta))
			if meta.Kind != "" {
				out[meta.Kind+"/"+meta.Metadata.Name] = []byte(doc)
			}
		}
	}
	return out
}

func TestHookRetentionAcrossReleaseRevisions(t *testing.T) {
	for _, name := range []string{"fixture", strings.Repeat("long-release-", 6)} {
		for _, mandatory := range []bool{true, false} {
			t.Run(fmt.Sprint(name, mandatory), func(t *testing.T) {
				previous := map[string]string{}
				for _, revision := range []int{2, 3} {
					objects := renderRevisionHooks(t, name, revision, mandatory)
					jobs := map[string]batchv1.Job{}
					var config corev1.ConfigMap
					var account corev1.ServiceAccount
					var role rbacv1.ClusterRole
					var binding rbacv1.ClusterRoleBinding
					for key, raw := range objects {
						switch strings.SplitN(key, "/", 2)[0] {
						case "Job":
							var job batchv1.Job
							require.NoError(t, yaml.Unmarshal(raw, &job))
							if component := job.Labels["app.kubernetes.io/component"]; component == "preflight" || component == "storageclass-reconcile" {
								jobs[component] = job
							}
						case "ConfigMap":
							var cm corev1.ConfigMap
							require.NoError(t, yaml.Unmarshal(raw, &cm))
							if cm.Labels["app.kubernetes.io/component"] == "storageclass-reconcile" {
								config = cm
							}
						case "ServiceAccount":
							var sa corev1.ServiceAccount
							require.NoError(t, yaml.Unmarshal(raw, &sa))
							if strings.Contains(sa.Name, "preflight") {
								account = sa
							}
						case "ClusterRole":
							var r rbacv1.ClusterRole
							require.NoError(t, yaml.Unmarshal(raw, &r))
							if strings.Contains(r.Name, "preflight") {
								role = r
							}
						case "ClusterRoleBinding":
							var b rbacv1.ClusterRoleBinding
							require.NoError(t, yaml.Unmarshal(raw, &b))
							if strings.Contains(b.Name, "preflight") {
								binding = b
							}
						}
					}
					require.Len(t, jobs, 2)
					for component, job := range jobs {
						require.LessOrEqual(t, len(job.Name), 63)
						require.Contains(t, job.Name, fmt.Sprintf("-r%d", revision))
						require.NotEqual(t, previous[component], job.Name)
						previous[component] = job.Name
						require.Nil(t, job.Spec.TTLSecondsAfterFinished)
						require.EqualValues(t, 0, *job.Spec.BackoffLimit)
						require.EqualValues(t, 300, *job.Spec.ActiveDeadlineSeconds)
						if mandatory || component == "storageclass-reconcile" {
							require.Equal(t, "hook-succeeded", job.Annotations["helm.sh/hook-delete-policy"])
						} else {
							require.Equal(t, "keep", job.Annotations["helm.sh/resource-policy"])
						}
					}
					require.Equal(t, account.Name, jobs["preflight"].Spec.Template.Spec.ServiceAccountName)
					require.Equal(t, role.Name, binding.RoleRef.Name)
					require.Len(t, binding.Subjects, 1)
					require.Equal(t, account.Name, binding.Subjects[0].Name)
					for _, resource := range []string{account.Name, role.Name, binding.Name, config.Name} {
						require.LessOrEqual(t, len(resource), 63)
						require.Contains(t, resource, fmt.Sprintf("-r%d", revision))
					}
					require.NotContains(t, config.Annotations["helm.sh/hook-delete-policy"], "hook-succeeded")
					require.NotContains(t, role.Annotations["helm.sh/hook-delete-policy"], "hook-succeeded")
					require.Contains(t, jobs["storageclass-reconcile"].Spec.Template.Spec.Containers[0].Args, "--storageclass-reconcile-configmap="+config.Name)
				}
			})
		}
	}
}

func TestBridgeIdentityCommonChartEnvironment(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(fmt.Sprint(configured), func(t *testing.T) {
			values := map[string]interface{}{"preflight": map[string]interface{}{"enabled": true, "failReleaseOnError": true}, "inventoryController": map[string]interface{}{"enabled": true}}
			want := ""
			if configured {
				want = `{"version":1,"datastores":[{"datastoreID":42,"configFingerprint":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","endpoints":[{"endpoint":"192.0.2.10","hostID":0,"hostName":"bridge-a.example.invalid"}]}]}`
				var binding map[string]interface{}
				require.NoError(t, json.Unmarshal([]byte(want), &binding))
				values["driver"] = map[string]interface{}{"cephBridgeIdentities": binding}
			}
			output, err := renderUpgradeChart(t, values)
			require.NoError(t, err)
			for _, name := range []string{"csi-controller-server.yaml", "csi-node-server.yaml", "inventory-controller.yaml", "preflight-job.yaml"} {
				var object struct {
					Spec struct{ Template struct{ Spec corev1.PodSpec } }
				}
				require.NoError(t, yaml.Unmarshal([]byte(output["opennebula-csi/templates/"+name]), &object))
				count := 0
				for _, container := range object.Spec.Template.Spec.Containers {
					for _, variable := range container.Env {
						if variable.Name == "ONE_CSI_CEPH_BRIDGE_IDENTITIES" {
							count++
							if configured {
								require.JSONEq(t, want, variable.Value)
							} else {
								require.Empty(t, variable.Value)
							}
						}
					}
				}
				require.Equal(t, 1, count, name)
			}
		})
	}
}

func TestHookObjectsDoNotChangeWhenChartVersionIsPromoted(t *testing.T) {
	candidate := renderRevisionHooks(t, "fixture", 7, true, "0.6.0-rc.2")
	stable := renderRevisionHooks(t, "fixture", 7, true, "0.6.0")
	compared := 0
	for key, object := range candidate {
		if strings.Contains(key, "preflight") || strings.Contains(key, "storageclass-reconcile") {
			require.Contains(t, stable, key)
			require.YAMLEq(t, string(object), string(stable[key]))
			compared++
		}
	}
	require.Equal(t, 6, compared) // two Jobs, three bootstrap objects and the paired input ConfigMap
}
