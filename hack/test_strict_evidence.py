import copy
import datetime as dt
import json
from pathlib import Path
import unittest

import yaml
import test_upgrade_evidence as fixtures
from upgrade_evidence import evaluate
from supervise_rollout import pod_ready, replacement_ready, validate_drain, validate_live


INVALID_BOOLEANS = [False, None, 0, 1, 2, "false", "true", [], {}, [True], {"value": True}]


def wire(value):
    return json.loads(json.dumps(value))


def assign(value, path, replacement):
    for key in path[:-1]:
        value = value[key]
    value[path[-1]] = replacement


class StrictEvidenceTest(unittest.TestCase):
    setUp = fixtures.EvidenceTest.setUp

    def test_evaluate_requires_literal_true_at_every_boolean_boundary(self):
        baseline = wire(self.bundle)
        baseline["replicaSets"]["items"] = [{"metadata": {"uid": "inventory-rs", "ownerReferences": [{"uid": "inventory", "controller": True}]}}]
        baseline["controlPods"]["items"][1]["metadata"]["ownerReferences"] = [{"uid": "inventory-rs", "controller": True}]
        self.assertTrue(evaluate(baseline, self.candidate)["ready"])
        paths = [
            ["registrationVerified"], ["readWriteVerified"],
            ["revisions", "items", 0, "metadata", "ownerReferences", 0, "controller"],
            ["nodePods", "items", 0, "metadata", "ownerReferences", 0, "controller"],
            ["controlPods", "items", 0, "metadata", "ownerReferences", 0, "controller"],
            ["controlPods", "items", 1, "metadata", "ownerReferences", 0, "controller"],
            ["replicaSets", "items", 0, "metadata", "ownerReferences", 0, "controller"],
            ["nodePods", "items", 0, "status", "containerStatuses", 0, "ready"],
            ["controlPods", "items", 0, "status", "containerStatuses", 0, "ready"],
            ["controlPods", "items", 1, "status", "containerStatuses", 0, "ready"],
        ]
        for path in paths:
            for invalid in INVALID_BOOLEANS:
                with self.subTest(path=path, invalid=invalid):
                    bundle = wire(baseline)
                    assign(bundle, path, invalid)
                    with self.assertRaises(ValueError):
                        evaluate(bundle, self.candidate)

    def drain(self):
        now = dt.datetime.now(dt.timezone.utc)
        proof = {"remainingFuseMounts": [], "remainingFuseProcesses": [], "command": "OFFLINE FIXTURE", "outputSHA256": "c" * 64, "namespaceIDs": ["mnt:fixture"]}
        return {
            "kind": "OpenNebulaFuseDrain/v1", "cluster": "hpldb", "clusterUID": "hpldb",
            "node": "0", "nodeUID": "nodeuid", "nodePodUID": "0",
            "candidateSHA256": self.bundle["candidateSHA256"], "verifiedAt": now.isoformat(),
            "operatorApproval": "replace hpldb/0/0", "consumerPods": [],
            "kernelMountsPreserved": True, "vmAttachmentsPreserved": True,
            "inspections": {key: copy.deepcopy(proof) for key in ("hostMountNamespace", "pluginMountNamespace", "consumerMountNamespaces")},
        }, now

    def test_drain_attestations_require_literal_true(self):
        receipt, now = self.drain()
        self.assertTrue(validate_drain(receipt, self.candidate, "hpldb", "0", "0", now))
        for key in ("kernelMountsPreserved", "vmAttachmentsPreserved"):
            for invalid in INVALID_BOOLEANS:
                with self.subTest(key=key, invalid=invalid):
                    changed = wire(receipt)
                    changed[key] = invalid
                    with self.assertRaises(ValueError):
                        validate_drain(changed, self.candidate, "hpldb", "0", "0", now)

    def test_supervision_readiness_and_ownership_require_literal_true(self):
        ds = wire(self.bundle["daemonset"])
        ds["spec"]["template"] = {"spec": {"containers": [{"name": "driver", "image": self.candidate["images"]["driver"]}]}}
        pods = wire(self.bundle["nodePods"]["items"])
        for i, pod in enumerate(pods):
            pod["metadata"]["uid"] = str(i)
            pod["metadata"]["labels"]["controller-revision-hash"] = "old"
            pod["status"]["conditions"] = [{"type": "Ready", "status": "True"}]
        node = {"metadata": {"uid": "nodeuid", "name": "0"}, "spec": {"unschedulable": True}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        receipt, _ = self.drain()
        self.assertEqual(validate_live(ds, pods, node, receipt, self.candidate, "hpldb")["metadata"]["uid"], "0")
        replacement = wire(pods[0])
        replacement["metadata"]["uid"] = "replacement"
        replacement["metadata"]["labels"]["controller-revision-hash"] = "ds-new"
        self.assertEqual(replacement_ready([replacement], "0", "ds", "0", "ds-new", self.candidate), "replacement")
        for path in (["status", "containerStatuses", 0, "ready"], ["metadata", "ownerReferences", 0, "controller"]):
            for invalid in INVALID_BOOLEANS:
                with self.subTest(path=path, invalid=invalid):
                    changed = wire(pods)
                    assign(changed[0], path, invalid)
                    with self.assertRaises(ValueError):
                        validate_live(ds, changed, node, receipt, self.candidate, "hpldb")
                    new = wire(replacement)
                    assign(new, path, invalid)
                    with self.assertRaises(ValueError):
                        replacement_ready([new], "0", "ds", "0", "ds-new", self.candidate)
                    if path[0] == "status":
                        self.assertFalse(pod_ready(new))


class HelmInstallerContractTest(unittest.TestCase):
    def test_all_workflow_installers_use_selected_helm_version(self):
        seen = []
        for path in (Path(__file__).resolve().parents[1] / ".github/workflows").glob("*.yaml"):
            workflow = yaml.safe_load(path.read_text())
            for job in workflow.get("jobs", {}).values():
                for step in job.get("steps", []):
                    if step.get("uses", "").split("@")[0] == "azure/setup-helm":
                        seen.append(path.name)
                        self.assertEqual(step.get("with", {}).get("version"), "v3.21.4", path.name)
        self.assertCountEqual(seen, ["ci.yaml", "promote-csi.yaml", "release-csi.yaml", "release-csi.yaml", "staging-release-validation.yaml", "staging-cephfs-validation.yaml"])


if __name__ == "__main__":
    unittest.main()
