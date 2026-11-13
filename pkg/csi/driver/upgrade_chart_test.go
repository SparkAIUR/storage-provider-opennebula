package driver

import (
	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/yaml"
	"strings"
	"testing"
)

func renderUpgradeChart(t *testing.T, values map[string]interface{}) (map[string]string, error) {
	t.Helper()
	chart, err := loader.Load("../../../helm/opennebula-csi")
	require.NoError(t, err)
	if _, ok := values["credentials"]; !ok {
		values["credentials"] = map[string]interface{}{"existingSecret": map[string]interface{}{"name": "fixture"}}
	}
	renderedValues, err := chartutil.ToRenderValues(chart, values, chartutil.ReleaseOptions{Name: "test", Namespace: "kube-system", Revision: 1, IsUpgrade: true}, chartutil.DefaultCapabilities)
	require.NoError(t, err)
	return engine.Render(chart, renderedValues)
}
func chartEnvironment(pod appsv1.DaemonSet) map[string]string {
	out := map[string]string{}
	for _, container := range pod.Spec.Template.Spec.Containers {
		if container.Name == "opennebula-csi" {
			for _, e := range container.Env {
				out[e.Name] = e.Value
			}
		}
	}
	return out
}

func TestUpgradeChartHonorsSafetyBooleans(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		values := map[string]interface{}{"driver": map[string]interface{}{"localDeviceRecovery": map[string]interface{}{"enabled": enabled}, "lastNodePreference": map[string]interface{}{"enabled": enabled, "webhook": map[string]interface{}{"enabled": enabled}}, "hotplugQueue": map[string]interface{}{"enabled": enabled}}, "controller": map[string]interface{}{"leaderElection": map[string]interface{}{"enabled": enabled}}}
		output, err := renderUpgradeChart(t, values)
		require.NoError(t, err)
		var node appsv1.DaemonSet
		require.NoError(t, yaml.Unmarshal([]byte(output["opennebula-csi/templates/csi-node-server.yaml"]), &node))
		want := "false"
		if enabled {
			want = "true"
		}
		env := chartEnvironment(node)
		for _, key := range []string{"ONE_CSI_LOCAL_DEVICE_RECOVERY_ENABLED", "ONE_CSI_LAST_NODE_PREFERENCE_ENABLED", "ONE_CSI_LAST_NODE_PREFERENCE_WEBHOOK_ENABLED", "ONE_CSI_HOTPLUG_QUEUE_ENABLED"} {
			require.Equal(t, want, env[key], key)
		}
		var controller appsv1.StatefulSet
		require.NoError(t, yaml.Unmarshal([]byte(output["opennebula-csi/templates/csi-controller-server.yaml"]), &controller))
		for _, e := range controller.Spec.Template.Spec.Containers[0].Env {
			if e.Name == "ONE_CSI_CONTROLLER_LEADER_ELECTION_ENABLED" {
				require.Equal(t, want, e.Value)
			}
		}
		require.Equal(t, enabled, strings.TrimSpace(output["opennebula-csi/templates/last-node-preference-webhook.yaml"]) != "")
	}
}

func TestUpgradeChartCustodyStrategyAndProbes(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	output, err := renderUpgradeChart(t, map[string]interface{}{"image": map[string]interface{}{"digest": digest}, "preflight": map[string]interface{}{"enabled": true, "failReleaseOnError": true}, "storageClassReconcile": map[string]interface{}{"enabled": true}, "storageClasses": []interface{}{map[string]interface{}{"name": "fixture", "parameters": map[string]interface{}{"datastoreIDs": "100"}}}, "sidecarImages": map[string]interface{}{"registrar": map[string]interface{}{"digest": digest}}, "probes": map[string]interface{}{"startup": map[string]interface{}{"failureThreshold": 100}}})
	require.NoError(t, err)
	var node appsv1.DaemonSet
	require.NoError(t, yaml.Unmarshal([]byte(output["opennebula-csi/templates/csi-node-server.yaml"]), &node))
	require.Equal(t, appsv1.OnDeleteDaemonSetStrategyType, node.Spec.UpdateStrategy.Type)
	require.Nil(t, node.Spec.UpdateStrategy.RollingUpdate)
	require.EqualValues(t, 100, node.Spec.Template.Spec.Containers[0].StartupProbe.FailureThreshold)
	require.Contains(t, node.Spec.Template.Spec.Containers[0].Image, "@"+digest)
	require.Contains(t, node.Spec.Template.Spec.Containers[1].Image, "@"+digest)
	for _, name := range []string{"preflight-job.yaml", "storageclass-reconcile-job.yaml"} {
		var job batchv1.Job
		require.NoError(t, yaml.Unmarshal([]byte(output["opennebula-csi/templates/"+name]), &job))
		require.Contains(t, job.Spec.Template.Spec.Containers[0].Image, "@"+digest)
		if name == "preflight-job.yaml" {
			require.Nil(t, job.Spec.TTLSecondsAfterFinished)
			require.EqualValues(t, 300, *job.Spec.ActiveDeadlineSeconds)
		}
	}
	var controller appsv1.StatefulSet
	require.NoError(t, yaml.Unmarshal([]byte(output["opennebula-csi/templates/csi-controller-server.yaml"]), &controller))
	require.Equal(t, "/readyz", controller.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet.Path)
}
func TestMandatoryPreflightRejectsUnbootstrappableInlineSecret(t *testing.T) {
	_, err := renderUpgradeChart(t, map[string]interface{}{"credentials": map[string]interface{}{"inlineAuth": "fixture:fixture", "existingSecret": map[string]interface{}{"name": ""}}, "preflight": map[string]interface{}{"enabled": true, "failReleaseOnError": true}})
	require.ErrorContains(t, err, "mandatory preflight requires")
}
func TestUpgradeWebhookExcludesOwnInfrastructure(t *testing.T) {
	output, err := renderUpgradeChart(t, map[string]interface{}{})
	require.NoError(t, err)
	// The generated document is a public serialized configuration contract.
	documents := strings.Split(output["opennebula-csi/templates/last-node-preference-webhook.yaml"], "---")
	var webhook admissionv1.MutatingWebhookConfiguration
	for _, document := range documents {
		var candidate admissionv1.MutatingWebhookConfiguration
		require.NoError(t, yaml.Unmarshal([]byte(document), &candidate))
		if candidate.Kind == "MutatingWebhookConfiguration" {
			webhook = candidate
		}
	}
	require.Len(t, webhook.Webhooks, 1)
	require.NotEmpty(t, webhook.Webhooks[0].MatchConditions)
	require.Equal(t, "exclude-driver-infrastructure", webhook.Webhooks[0].MatchConditions[0].Name)
	env, err := cel.NewEnv(cel.Variable("object", cel.DynType))
	require.NoError(t, err)
	ast, issues := env.Compile(webhook.Webhooks[0].MatchConditions[0].Expression)
	require.NoError(t, issues.Err())
	program, err := env.Program(ast)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, namespace string
		labels          map[string]interface{}
		admit           bool
	}{
		{"own infrastructure", "kube-system", map[string]interface{}{"app.kubernetes.io/instance": "test", "app.kubernetes.io/name": "opennebula-csi"}, false},
		{"protected workload", "kube-system", map[string]interface{}{"app.kubernetes.io/instance": "database"}, true},
		{"missing labels", "kube-system", nil, true},
		{"other namespace", "default", map[string]interface{}{"app.kubernetes.io/instance": "test", "app.kubernetes.io/name": "opennebula-csi"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := map[string]interface{}{"namespace": tc.namespace}
			if tc.labels != nil {
				metadata["labels"] = tc.labels
			}
			value, _, err := program.Eval(map[string]interface{}{"object": map[string]interface{}{"metadata": metadata}})
			require.NoError(t, err)
			require.Equal(t, tc.admit, value.Value())
		})
	}

}

func TestUpgradeChartSnapshotterImageSelection(t *testing.T) {
	digest := "sha256:" + strings.Repeat("b", 64)
	for _, tc := range []struct {
		name   string
		values map[string]interface{}
		image  string
	}{
		{"default", map[string]interface{}{}, "registry.k8s.io/sig-storage/csi-snapshotter:v8.7.0"},
		{"digest", map[string]interface{}{"sidecarImages": map[string]interface{}{"snapshotter": map[string]interface{}{"digest": digest}}}, "registry.k8s.io/sig-storage/csi-snapshotter@" + digest},
		{"legacy", map[string]interface{}{"snapshotter": map[string]interface{}{"image": "registry.example/snapshotter:custom"}, "sidecarImages": map[string]interface{}{"snapshotter": map[string]interface{}{"digest": digest}}}, "registry.example/snapshotter:custom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := renderUpgradeChart(t, tc.values)
			require.NoError(t, err)
			var controller appsv1.StatefulSet
			require.NoError(t, yaml.Unmarshal([]byte(output["opennebula-csi/templates/csi-controller-server.yaml"]), &controller))
			images := map[string]string{}
			for _, container := range controller.Spec.Template.Spec.Containers {
				images[container.Name] = container.Image
			}
			require.Equal(t, tc.image, images["csi-snapshotter"])
		})
	}
}
