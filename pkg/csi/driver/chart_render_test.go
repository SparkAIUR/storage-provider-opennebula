package driver

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

func TestChartPreservesExplicitBooleanSettings(t *testing.T) {
	for _, setting := range []string{"absent", "true", "false"} {
		for _, metrics := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/metrics-%t", setting, metrics), func(t *testing.T) {
				chart, err := loader.Load("../../../helm/opennebula-csi")
				require.NoError(t, err)
				attacher := chart.Values["controller"].(map[string]interface{})["attacher"].(map[string]interface{})
				reconcile := chart.Values["storageClassReconcile"].(map[string]interface{})
				delete(attacher, "httpEndpointEnabled")
				delete(reconcile, "adoptUnannotated")
				want := setting != "false"
				if setting != "absent" {
					attacher["httpEndpointEnabled"] = want
					reconcile["adoptUnannotated"] = want
				}
				reconcile["enabled"] = true
				values, err := chartutil.ToRenderValues(chart, map[string]interface{}{
					"credentials":    map[string]interface{}{"existingSecret": map[string]interface{}{"name": "dummy", "userKey": "user", "passwordKey": "pass"}},
					"storageClasses": []interface{}{map[string]interface{}{"name": "local", "parameters": map[string]interface{}{"datastoreIDs": "100"}}},
					"metrics":        map[string]interface{}{"enabled": metrics, "controller": map[string]interface{}{"serviceMonitor": map[string]interface{}{"enabled": true}}},
				}, chartutil.ReleaseOptions{Name: "test", Namespace: "default", Revision: 1, IsUpgrade: true}, chartutil.DefaultCapabilities)
				require.NoError(t, err)
				rendered, err := engine.Render(chart, values)
				require.NoError(t, err)
				prefix := chart.Metadata.Name + "/templates/"
				var job batchv1.Job
				require.NoError(t, yaml.UnmarshalStrict([]byte(rendered[prefix+"storageclass-reconcile-job.yaml"]), &job))
				require.Equal(t, "Job", job.Kind)
				require.Len(t, job.Spec.Template.Spec.Containers, 1)
				require.Contains(t, job.Spec.Template.Spec.Containers[0].Args, fmt.Sprintf("--storageclass-reconcile-adopt-unannotated=%t", want))

				var controller appsv1.StatefulSet
				require.NoError(t, yaml.UnmarshalStrict([]byte(rendered[prefix+"csi-controller-server.yaml"]), &controller))
				foundAttacher := false
				for _, container := range controller.Spec.Template.Spec.Containers {
					if container.Name != "csi-attacher" {
						continue
					}
					foundAttacher = true
					hasEndpoint := false
					for _, arg := range container.Args {
						if strings.HasPrefix(arg, "--http-endpoint=") {
							hasEndpoint = true
						}
					}
					require.Equal(t, want, hasEndpoint)
					hasPort := false
					for _, port := range container.Ports {
						if port.Name == "att-metrics" {
							hasPort = true
						}
					}
					require.Equal(t, want, hasPort)
				}
				require.True(t, foundAttacher)

				services := k8syaml.NewYAMLOrJSONDecoder(strings.NewReader(rendered[prefix+"csi-metrics-services.yaml"]), 4096)
				foundService := false
				for {
					var service corev1.Service
					err := services.Decode(&service)
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					if service.Labels["app.kubernetes.io/component"] != "controller" {
						continue
					}
					foundService = true
					hasAttacher := false
					for _, port := range service.Spec.Ports {
						if port.Name == "att-metrics" {
							hasAttacher = true
							require.Equal(t, "att-metrics", port.TargetPort.StrVal)
						}
					}
					require.Equal(t, want, hasAttacher)
				}
				require.Equal(t, metrics, foundService)

				monitors := k8syaml.NewYAMLOrJSONDecoder(strings.NewReader(rendered[prefix+"csi-metrics-servicemonitors.yaml"]), 4096)
				foundMonitor := false
				for {
					var monitor struct {
						metav1.TypeMeta `json:",inline"`
						Metadata        metav1.ObjectMeta `json:"metadata"`
						Spec            struct {
							Endpoints []struct {
								Port string `json:"port"`
							} `json:"endpoints"`
						} `json:"spec"`
					}
					err := monitors.Decode(&monitor)
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					if monitor.Metadata.Labels["app.kubernetes.io/component"] != "controller" {
						continue
					}
					foundMonitor = true
					require.Equal(t, "ServiceMonitor", monitor.Kind)
					hasAttacher := false
					for _, endpoint := range monitor.Spec.Endpoints {
						if endpoint.Port == "att-metrics" {
							hasAttacher = true
						}
					}
					require.Equal(t, want, hasAttacher)
				}
				require.Equal(t, metrics, foundMonitor)
			})
		}
	}
}
