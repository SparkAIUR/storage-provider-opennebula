package driver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestLocalDiskIdentityMatchesRequiresObservedEvidence(t *testing.T) {
	expected := &LocalDiskIdentity{
		Version: stateObjectVersion,
		AssertedByController: &LocalDiskControllerAssertion{
			DeviceSerial:      "onecsi-42",
			OpenNebulaImageID: "42",
			DiskTarget:        "vdb",
		},
	}
	observed := &LocalDiskIdentity{
		Version: stateObjectVersion,
		AssertedByController: &LocalDiskControllerAssertion{
			DeviceSerial:      "onecsi-42",
			OpenNebulaImageID: "42",
			DiskTarget:        "vdb",
		},
	}

	matched, reason := localDiskIdentityMatches(expected, observed)
	if matched {
		t.Fatalf("expected asserted-only identities to fail closed")
	}
	if reason != "expected identity lacks independently observed filesystem or block-device evidence" {
		t.Fatalf("unexpected reason: %q", reason)
	}
}

func TestLocalDeviceReportReadySkipsWrongIdentity(t *testing.T) {
	driver := newLocalDeviceRecoveryTestDriver(t)
	server := NewControllerServer(driver, &MockOpenNebulaVolumeProviderTestify{}, nil)
	now := time.Now().UTC()

	report := LocalDeviceMissingReport{
		Node:            "node-a",
		VolumeID:        "vol-1",
		FailureClass:    localDeviceFailureClassWrongIdentity,
		FirstObservedAt: now.Add(-5 * time.Minute),
		LastObservedAt:  now.Add(-time.Minute),
		Attempts:        4,
	}

	if got := localDeviceFailureClass(report); got != localDeviceFailureClassWrongIdentity {
		t.Fatalf("expected wrong identity failure class to persist, got %q", got)
	}
	ready, reason := server.localDeviceReportReady(report, now)
	if ready {
		t.Fatalf("wrong-device identity should never become auto-recovery eligible")
	}
	if reason != "repair_required_wrong_identity" {
		t.Fatalf("unexpected readiness reason: %q", reason)
	}
}

func TestDesiredNodeForVolumeClassifiesTransitionalAndStaleVolumeAttachments(t *testing.T) {
	t.Run("transitional volume attachment remains temporary demand", func(t *testing.T) {
		pv, pvc := newLocalPVAndPVC("vol-transitional", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
		pvc.Spec.VolumeName = pv.Name
		pvName := pv.Name
		va := &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "va-transitional",
				CreationTimestamp: metav1.NewTime(time.Now().Add(-30 * time.Second)),
			},
			Spec: storagev1.VolumeAttachmentSpec{
				Attacher: DefaultDriverName,
				NodeName: "node-a",
				Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
			},
			Status: storagev1.VolumeAttachmentStatus{Attached: true},
		}
		runtime := &KubeRuntime{
			client:       fake.NewSimpleClientset(pv, pvc, va),
			enabled:      true,
			staleVAGrace: 90 * time.Second,
		}

		state, err := runtime.DesiredNodeForVolume(context.Background(), "vol-transitional")
		if err != nil {
			t.Fatalf("DesiredNodeForVolume returned error: %v", err)
		}
		if !state.Desired || state.NodeName != "node-a" {
			t.Fatalf("expected transitional attachment demand on node-a, got %#v", state)
		}
		if state.DemandSource != volumeDemandSourceTransitionalVolumeAttachment {
			t.Fatalf("unexpected demand source: %#v", state)
		}
		if !state.AttachmentResidue || len(state.AttachmentNames) != 1 {
			t.Fatalf("expected attachment residue metadata, got %#v", state)
		}
	})

	t.Run("stale volume attachment resolves to parked workload", func(t *testing.T) {
		pv, pvc := newLocalPVAndPVC("vol-parked", []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, nil)
		pvc.Spec.VolumeName = pv.Name
		pvName := pv.Name
		va := &storagev1.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "va-parked",
				CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
			},
			Spec: storagev1.VolumeAttachmentSpec{
				Attacher: DefaultDriverName,
				NodeName: "node-a",
				Source:   storagev1.VolumeAttachmentSource{PersistentVolumeName: &pvName},
			},
			Status: storagev1.VolumeAttachmentStatus{Attached: true},
		}
		runtime := &KubeRuntime{
			client:       fake.NewSimpleClientset(pv, pvc, va),
			enabled:      true,
			staleVAGrace: 90 * time.Second,
		}

		state, err := runtime.DesiredNodeForVolume(context.Background(), "vol-parked")
		if err != nil {
			t.Fatalf("DesiredNodeForVolume returned error: %v", err)
		}
		if state.Desired {
			t.Fatalf("expected stale volume attachment to stop counting as demand, got %#v", state)
		}
		if state.Reason != volumeDemandSourceVolumeParked || state.DemandSource != volumeDemandSourceStaleVolumeAttachment {
			t.Fatalf("unexpected parked workload classification: %#v", state)
		}
		if !state.AttachmentResidue || len(state.AttachmentNodes) != 1 || state.AttachmentNodes[0] != "node-a" {
			t.Fatalf("expected residue node evidence, got %#v", state)
		}
		desired, reason, err := runtime.VolumeDesiredOnNode(context.Background(), "vol-parked", "node-a")
		if err != nil {
			t.Fatalf("VolumeDesiredOnNode returned error: %v", err)
		}
		if desired || reason != volumeDemandSourceStaleVolumeAttachment {
			t.Fatalf("unexpected per-node desired result desired=%v reason=%q", desired, reason)
		}
	})
}

func TestCollectLocalDiskSessionDiagnostics(t *testing.T) {
	originalRoot := localDiskSessionRootPath
	localDiskSessionRootPath = t.TempDir()
	t.Cleanup(func() {
		localDiskSessionRootPath = originalRoot
	})

	session := localDiskSession{
		VolumeID:          "vol-1",
		StagingTargetPath: "/stage/vol-1",
		PVCNamespace:      "default",
		PVCName:           "pvc-vol-1",
		PVName:            "pv-vol-1",
		PublishedTargets: []localDiskPublishedTarget{
			{TargetPath: "/pods/pod-a/vol"},
			{TargetPath: "/pods/pod-b/vol"},
		},
		Identity: &LocalDiskIdentity{
			Version: stateObjectVersion,
			AssertedByController: &LocalDiskControllerAssertion{
				DeviceSerial:      "onecsi-42",
				OpenNebulaImageID: "42",
				DiskTarget:        "vdb",
			},
			ObservedFromDevice: &LocalDiskObservedIdentity{
				Block: &LocalDiskObservedBlockIdentity{
					DevicePath:          "/dev/sdb",
					ByIDPath:            "/dev/disk/by-id/virtio-onecsi-42",
					DeviceSerial:        "onecsi-42",
					MountSourceIdentity: "/dev/sdb",
				},
				Filesystem: &LocalDiskObservedFilesystemIdentity{
					FilesystemUUID: "fs-uuid",
					FilesystemType: "xfs",
					PartitionUUID:  "part-uuid",
				},
			},
		},
		RecoveryAttempts:  2,
		LastRecoveryError: "wrong device identity",
	}
	if err := newLocalDiskSessionStore(localDiskSessionRootPath).Save(session); err != nil {
		t.Fatalf("failed to save session: %v", err)
	}

	diagnostics, err := collectLocalDiskSessionDiagnostics()
	if err != nil {
		t.Fatalf("collectLocalDiskSessionDiagnostics returned error: %v", err)
	}
	if len(diagnostics) != 1 {
		t.Fatalf("expected 1 diagnostic, got %d", len(diagnostics))
	}
	if diagnostics[0].VolumeID != "vol-1" || diagnostics[0].StagingTargetPath != "/stage/vol-1" {
		t.Fatalf("unexpected diagnostic content: %#v", diagnostics[0])
	}
	if len(diagnostics[0].PublishedTargets) != 2 {
		t.Fatalf("expected published targets to be preserved, got %#v", diagnostics[0])
	}
	if diagnostics[0].LastHealthyIdentity == nil || localDiskObservedByIDPath(diagnostics[0].LastHealthyIdentity) != "/dev/disk/by-id/virtio-onecsi-42" {
		t.Fatalf("expected last healthy identity in diagnostic, got %#v", diagnostics[0])
	}
}

func TestLocalRWOProtectionDecisionUsesAutomaticMaintenanceSource(t *testing.T) {
	node := newReadyNode("node-a", true)
	node.Spec.Unschedulable = true
	record := VolumeHistoryRecord{
		Version:                   stateObjectVersion,
		VolumeID:                  "vol-1",
		Backend:                   "local",
		LastSuccessfulNodeName:    "node-a",
		LastSuccessfulPublishTime: time.Now().Add(-5 * time.Minute),
	}
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("failed to marshal history payload: %v", err)
	}
	runtime := &KubeRuntime{
		client: fake.NewSimpleClientset(
			node,
			&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: volumeHistoryStateConfigMapName, Namespace: "default"},
				Data:       map[string]string{"vol-1": string(payload)},
			},
		),
		enabled: true,
	}
	driver := &Driver{
		kubeRuntime:   runtime,
		featureGates:  FeatureGates{LocalRWOAutoProtection: true},
		volumeHistory: NewVolumeHistoryManager(runtime, ""),
	}
	if err := driver.volumeHistory.LoadFromConfigMap(context.Background()); err != nil {
		t.Fatalf("failed to load volume history configmap: %v", err)
	}

	decision, err := localRWOProtectionDecisionForDriver(context.Background(), driver, "vol-1", "node-b")
	if err != nil {
		t.Fatalf("localRWOProtectionDecisionForDriver returned error: %v", err)
	}
	if !decision.Protected || decision.Source != protectionSourceAutomaticMaintenance {
		t.Fatalf("expected automatic maintenance protection, got %#v", decision)
	}
	if decision.AutomaticTrigger != protectionTriggerNodeUnschedulable {
		t.Fatalf("unexpected automatic trigger: %#v", decision)
	}
}
