package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
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
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2"
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

const (
	reconcileTestDriverRepository = "registry.example.test/fixture/csi-driver"
	reconcileTestDriverTag        = "fixture-tag"
)

func parseStorageClassJobArgs(args []string) (string, StorageClassReconcileOptions, error) {
	flags := flag.NewFlagSet("storageclass-reconcile", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	klog.InitFlags(flags)
	mode, opts := BindStorageClassReconcileCLI(flags)
	err := flags.Parse(args)
	if err == nil && flags.NArg() != 0 {
		err = fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	return *mode, *opts, err
}

func TestStorageClassReconcileRenderContract(t *testing.T) {
	helm := os.Getenv("STORAGECLASS_RECONCILE_HELM")
	if helm == "" {
		t.Skip("set STORAGECLASS_RECONCILE_HELM to run the Helm CLI integration")
	}
	version, err := exec.Command(helm, "version", "--template", "{{.Version}}").Output()
	require.NoError(t, err)
	require.Equal(t, "v3.21.4", strings.TrimSpace(string(version)))
	chartDir := os.Getenv("STORAGECLASS_RECONCILE_CHART_DIR")
	if chartDir == "" {
		chartDir = "../../../helm/opennebula-csi"
	}
	for _, mode := range []string{"default", "disabled", "enabled", "no-classes"} {
		t.Run(mode, func(t *testing.T) {
			rendered := renderStorageClassReconcileContract(t, helm, chartDir, mode)
			var jobs []batchv1.Job
			var inputs []corev1.ConfigMap
			decoder := k8syaml.NewYAMLOrJSONDecoder(strings.NewReader(rendered), 4096)
			for {
				var raw json.RawMessage
				err := decoder.Decode(&raw)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				if len(bytes.TrimSpace(raw)) == 0 {
					continue
				}
				var meta metav1.TypeMeta
				require.NoError(t, json.Unmarshal(raw, &meta))
				switch meta.Kind {
				case "Job":
					var job batchv1.Job
					require.NoError(t, json.Unmarshal(raw, &job))
					reconcile := job.Labels["app.kubernetes.io/component"] == "storageclass-reconcile" ||
						job.Spec.Template.Labels["app.kubernetes.io/component"] == "storageclass-reconcile"
					for _, container := range job.Spec.Template.Spec.Containers {
						argv := append(append([]string(nil), container.Command...), container.Args...)
						runMode, _, _ := parseStorageClassJobArgs(argv)
						reconcile = reconcile || runMode == "storageclass-reconcile"
					}
					if reconcile {
						jobs = append(jobs, job)
					}
				case "ConfigMap":
					var cm corev1.ConfigMap
					require.NoError(t, json.Unmarshal(raw, &cm))
					_, hasInput := cm.Data["storageclasses.json"]
					if hasInput || cm.Labels["app.kubernetes.io/component"] == "storageclass-reconcile" {
						inputs = append(inputs, cm)
					}
				}
			}
			if mode != "enabled" {
				require.Empty(t, jobs)
				require.Empty(t, inputs)
				return
			}
			require.Len(t, jobs, 1)
			require.Len(t, inputs, 1)
			job, input := jobs[0], inputs[0]
			require.Equal(t, job.Namespace, input.Namespace)
			require.Len(t, job.Spec.Template.Spec.Containers, 1)
			container := job.Spec.Template.Spec.Containers[0]
			require.Equal(t, reconcileTestDriverRepository+":"+reconcileTestDriverTag, container.Image)
			require.Empty(t, container.Command, "the Job must use the CSI driver image entrypoint")
			args := container.Args
			runMode, opts, err := parseStorageClassJobArgs(args)
			require.NoError(t, err)
			require.Equal(t, "storageclass-reconcile", runMode)
			invalid := append(append([]string(nil), args...), "--storageclass-reconcile-configmap-unknown=input")
			_, _, err = parseStorageClassJobArgs(invalid)
			require.Error(t, err)
			require.Equal(t, input.Name, opts.ConfigMapName)
			require.Equal(t, input.Namespace, opts.Namespace)
			require.Equal(t, "pre-upgrade", job.Annotations["helm.sh/hook"])
			require.Equal(t, "pre-upgrade", input.Annotations["helm.sh/hook"])
			jobWeight, err := strconv.Atoi(job.Annotations["helm.sh/hook-weight"])
			require.NoError(t, err)
			inputWeight, err := strconv.Atoi(input.Annotations["helm.sh/hook-weight"])
			require.NoError(t, err)
			require.Less(t, inputWeight, jobWeight)
			client := fake.NewSimpleClientset(&input)
			var result bytes.Buffer
			require.NoError(t, RunStorageClassReconcile(context.Background(), client, opts, &result))
			class, err := client.StorageV1().StorageClasses().Get(context.Background(), "opennebula-local", metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, DefaultDriverName, class.Provisioner)
			require.Equal(t, "100", class.Parameters["datastoreIDs"])
		})
	}
}

func renderStorageClassReconcileContract(t *testing.T, helm, chartDir, mode string) string {
	t.Helper()
	args := []string{"template", "reconcile", chartDir, "--namespace", "default", "--is-upgrade",
		"--set", "credentials.existingSecret.name=dummy,credentials.existingSecret.userKey=user,credentials.existingSecret.passwordKey=pass",
		"--set-string", "image.repository=" + reconcileTestDriverRepository + ",image.tag=" + reconcileTestDriverTag + ",image.digest="}
	if mode != "no-classes" {
		args = append(args, "--set", "storageClasses[0].name=opennebula-local,storageClasses[0].parameters.datastoreIDs=100")
	} else {
		args = append(args, "--set-json", "storageClasses=[]")
	}
	if mode != "default" {
		args = append(args, "--set", fmt.Sprintf("storageClassReconcile.enabled=%t", mode != "disabled"))
	}
	output, err := exec.Command(helm, args...).Output()
	require.NoError(t, err)
	return string(output)
}
