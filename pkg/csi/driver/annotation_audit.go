package driver

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

type AnnotationAuditFinding struct {
	Scope        string `json:"scope"`
	Key          string `json:"key"`
	Value        string `json:"value,omitempty"`
	Severity     string `json:"severity"`
	Message      string `json:"message"`
	CanonicalKey string `json:"canonicalKey,omitempty"`
}

func auditVolumeAnnotations(scope string, annotations map[string]string) []AnnotationAuditFinding {
	findings := []AnnotationAuditFinding{}
	if len(annotations) == 0 {
		return findings
	}
	if value := strings.TrimSpace(annotations[annotationLegacyLastAttachedNode]); value != "" {
		findings = append(findings, AnnotationAuditFinding{
			Scope:        scope,
			Key:          annotationLegacyLastAttachedNode,
			Value:        value,
			Severity:     "warning",
			Message:      "legacy last-attached-node annotation is ignored by the driver",
			CanonicalKey: annotationLastAttachedNode,
		})
	}
	if value := strings.TrimSpace(annotations[annotationPreferredLastNode]); value != "" {
		findings = append(findings, AnnotationAuditFinding{
			Scope:        scope,
			Key:          annotationPreferredLastNode,
			Value:        value,
			Severity:     "warning",
			Message:      "preferred-last-node is deprecated and will be removed in v0.6.0",
			CanonicalKey: annotationPreferredNode,
		})
	}
	return findings
}

func auditSoftPlacementAnnotations(ctx context.Context, runtime *KubeRuntime, pv *corev1.PersistentVolume, pvc *corev1.PersistentVolumeClaim) []AnnotationAuditFinding {
	findings := []AnnotationAuditFinding{}
	if runtime == nil || !runtime.enabled || pv == nil || pv.Spec.CSI == nil || !eligibleForLastNodePreference(pv) {
		return findings
	}
	lastAttachedNode := strings.TrimSpace(pv.Annotations[annotationLastAttachedNode])
	if lastAttachedNode == "" {
		return findings
	}
	runtimeCtx := volumeRuntimeContextFromPVAndPVC(pv, pvc)
	validation := runtime.inspectNodeCandidate(ctx, runtimeCtx, lastAttachedNode, "last-attached-node")
	if validation.Valid() {
		return findings
	}
	findings = append(findings, AnnotationAuditFinding{
		Scope:    "pv",
		Key:      annotationLastAttachedNode,
		Value:    lastAttachedNode,
		Severity: "warning",
		Message:  fmt.Sprintf("last-attached-node is a stale soft-placement hint and will be ignored: %s", validation.Message),
	})
	return findings
}
