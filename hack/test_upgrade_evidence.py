import copy
import datetime as dt
import hashlib
import json
import unittest
import yaml
from unittest.mock import patch
import subprocess
import sys
import tempfile
from pathlib import Path
from upgrade_evidence import collect
from upgrade_evidence import evaluate, validate_receipt
from supervise_rollout import validate_drain


class EvidenceTest(unittest.TestCase):
    def setUp(self):
        digest = "registry/driver@sha256:" + "a" * 64
        self.candidate = {
            "sourceCommit": "b" * 40,
            "sourceTree": "c" * 40,
            "images": {"driver": digest},
            "clusterUIDs": {c: c for c in ("hpldb", "hplsvc", "bravo")},
            "nodeRevisions": {c: "ds-new" for c in ("hpldb", "hplsvc", "bravo")},
            "chartVersion": "0.6.0-rc.1",
            "quarantineExceptions": {c: [] for c in ("hpldb", "hplsvc", "bravo")},
        }
        owner = {"uid": "ds", "controller": True}

        def pod(n):
            return {
                "metadata": {
                    "labels": {"controller-revision-hash": "ds-new"},
                    "ownerReferences": [owner],
                },
                "spec": {
                    "nodeName": n,
                    "containers": [{"name": "driver", "image": digest}],
                },
                "status": {
                    "phase": "Running",
                    "containerStatuses": [
                        {"name": "driver", "ready": True, "imageID": digest}
                    ],
                },
            }

        workload = {
            "metadata": {"generation": 1},
            "spec": {"replicas": 1},
            "status": {"observedGeneration": 1, "readyReplicas": 1},
        }
        flux = {
            "metadata": {"generation": 1},
            "status": {
                "observedGeneration": 1,
                "conditions": [{"type": "Ready", "status": "True"}],
                "lastAttemptedRevision": "0.6.0-rc.1",
            },
        }
        controller = copy.deepcopy(workload)
        controller["metadata"]["uid"] = "controller"
        controller["spec"]["template"] = {
            "spec": {"containers": [{"name": "driver", "image": digest}]}
        }
        controller["status"]["updateRevision"] = "ds-new"
        inventory = copy.deepcopy(controller)
        inventory["metadata"]["uid"] = "inventory"
        controls = [pod("controller"), pod("inventory")]
        for idx, uid in enumerate(("controller", "inventory")):
            controls[idx]["metadata"]["ownerReferences"] = [
                {"uid": uid, "controller": True}
            ]
        self.bundle = {
            "cluster": "hpldb",
            "clusterUID": "hpldb",
            "kubernetesVersion": "v1.36.2",
            "daemonset": {
                "metadata": {"uid": "ds", "name": "node", "generation": 1},
                "spec": {"updateStrategy": {"type": "OnDelete"}},
                "status": dict(
                    observedGeneration=1,
                    desiredNumberScheduled=3,
                    updatedNumberScheduled=3,
                    numberReady=3,
                    numberAvailable=3,
                ),
            },
            "revisions": {
                "items": [
                    {"metadata": {"name": "node-ds-new", "ownerReferences": [owner]}}
                ]
            },
            "nodePods": {"items": [pod(str(i)) for i in range(3)]},
            "controlPods": {"items": controls},
            "controller": controller,
            "inventory": inventory,
            "replicaSets": {"items": []},
            "helmRelease": flux,
            "kustomization": flux,
            "registrationVerified": True,
            "readWriteVerified": True,
            "quarantineExceptions": [],
        }
        self.bundle["capturedAt"] = dt.datetime.now(dt.timezone.utc).isoformat()
        self.bundle["candidateSHA256"] = hashlib.sha256(
            json.dumps(self.candidate, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def test_collect_uses_target_wrapper_and_real_workload_names(self):
        calls = []

        def output(command, **kwargs):
            calls.append(command)
            return json.dumps(
                {
                    "metadata": {"uid": "hpldb"},
                    "serverVersion": {"gitVersion": "v1.36.2"},
                }
            )

        with patch("upgrade_evidence.subprocess.check_output", side_effect=output):
            collect("hpldb")
        self.assertTrue(all(c[:4] == ["rtk", "proxy", "kc", "hpldb"] for c in calls))
        self.assertTrue(any("opennebula-csi-inventory" in c for c in calls))
        self.assertTrue(any("infra-opennebula-csi" in c for c in calls))
        self.assertFalse(any("secrets" in c for c in calls))

    def test_cli_consumes_serialized_realistic_revision_and_owners(self):
        with tempfile.TemporaryDirectory() as tmp:
            c = Path(tmp) / "candidate.json"
            b = Path(tmp) / "bundle.json"
            c.write_text(json.dumps(self.candidate))
            b.write_text(json.dumps(self.bundle))
            command = [
                sys.executable,
                str(Path(__file__).with_name("upgrade_evidence.py")),
                "check",
                "--candidate",
                str(c),
                "--evidence",
                str(b),
            ]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.bundle["controlPods"]["items"][0]["metadata"]["ownerReferences"][0][
                "uid"
            ] = "job"
            b.write_text(json.dumps(self.bundle))
            self.assertNotEqual(
                subprocess.run(command, capture_output=True).returncode, 0
            )

    def test_exact_candidate_accepted(self):
        self.assertTrue(evaluate(self.bundle, self.candidate)["ready"])

    def test_incomplete_and_wrong_identity_rejected(self):
        for field, value in [
            ("updatedNumberScheduled", 0),
            ("numberReady", 2),
            ("observedGeneration", 0),
        ]:
            b = copy.deepcopy(self.bundle)
            b["daemonset"]["status"][field] = value
            with self.assertRaises(ValueError):
                evaluate(b, self.candidate)
        b = copy.deepcopy(self.bundle)
        b["nodePods"]["items"][0]["status"]["containerStatuses"][0]["imageID"] = "old"
        with self.assertRaises(ValueError):
            evaluate(b, self.candidate)

    def test_missing_native_and_short_observation_rejected(self):
        with self.assertRaises((KeyError, ValueError)):
            validate_receipt({}, self.candidate)
        r = {
            "kind": "OpenNebulaNativeQualification/v1",
            "sourceCommit": self.candidate["sourceCommit"],
            "sourceTree": self.candidate["sourceTree"],
            "approval": {"operator": "operator", "changeReference": "change"},
            "images": self.candidate["images"],
            "candidateSHA256": hashlib.sha256(
                json.dumps(
                    self.candidate, sort_keys=True, separators=(",", ":")
                ).encode()
            ).hexdigest(),
            "providerVersion": "7.4.0",
            "clusters": {
                "hpldb": {
                    "result": "passed",
                    "observationStartedAt": "2026-01-01T00:00:00+00:00",
                    "observationEndedAt": "2026-01-01T01:00:00+00:00",
                }
            },
        }
        with self.assertRaisesRegex(ValueError, "24-hour"):
            validate_receipt(r, self.candidate)

    def test_drain_expiry_and_candidate_binding(self):
        now = dt.datetime.now(dt.timezone.utc)
        proof = {
            "remainingFuseMounts": [],
            "remainingFuseProcesses": [],
            "command": "nsenter read-only inspection",
            "outputSHA256": "c" * 64,
            "namespaceIDs": ["mnt:1"],
        }
        r = {
            "kind": "OpenNebulaFuseDrain/v1",
            "cluster": "hpldb",
            "clusterUID": "hpldb",
            "node": "node",
            "nodePodUID": "uid",
            "candidateSHA256": hashlib.sha256(
                json.dumps(
                    self.candidate, sort_keys=True, separators=(",", ":")
                ).encode()
            ).hexdigest(),
            "verifiedAt": now.isoformat(),
            "operatorApproval": "replace hpldb/node/uid",
            "inspections": {
                k: proof
                for k in (
                    "hostMountNamespace",
                    "pluginMountNamespace",
                    "consumerMountNamespaces",
                )
            },
            "kernelMountsPreserved": True,
            "vmAttachmentsPreserved": True,
            "consumerPods": [],
        }
        self.assertTrue(validate_drain(r, self.candidate, "hpldb", "node", "uid", now))
        with self.assertRaisesRegex(ValueError, "expired"):
            validate_drain(
                r, self.candidate, "hpldb", "node", "uid", now + dt.timedelta(minutes=6)
            )
        r["inspections"]["hostMountNamespace"]["remainingFuseProcesses"] = [123]
        with self.assertRaisesRegex(ValueError, "remain"):
            validate_drain(r, self.candidate, "hpldb", "node", "uid", now)


class SupervisionTest(unittest.TestCase):
    setUp = EvidenceTest.setUp

    def test_live_rejects_missing_status_uncordoned_and_uid_change(self):
        from supervise_rollout import validate_live, replacement_ready

        ds = copy.deepcopy(self.bundle["daemonset"])
        ds["metadata"]["resourceVersion"] = "1"
        ds["spec"]["template"] = {
            "spec": {
                "containers": [
                    {"name": "driver", "image": self.candidate["images"]["driver"]}
                ]
            }
        }
        pods = copy.deepcopy(self.bundle["nodePods"]["items"])
        for i, p in enumerate(pods):
            p["metadata"]["uid"] = str(i)
            p["metadata"]["labels"]["controller-revision-hash"] = "old"
            p["status"]["conditions"] = [{"type": "Ready", "status": "True"}]
        node = {
            "metadata": {"name": "0", "uid": "nodeuid"},
            "spec": {"unschedulable": True},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        }
        receipt = {"nodeUID": "nodeuid", "node": "0", "nodePodUID": "0"}
        self.assertEqual(
            validate_live(ds, pods, node, receipt, self.candidate, "hpldb")["metadata"][
                "uid"
            ],
            "0",
        )
        node["spec"]["unschedulable"] = False
        with self.assertRaisesRegex(ValueError, "cordoned"):
            validate_live(ds, pods, node, receipt, self.candidate, "hpldb")
        node["spec"]["unschedulable"] = True
        pods[1]["status"]["containerStatuses"] = []
        with self.assertRaisesRegex(ValueError, "incomplete"):
            validate_live(ds, pods, node, receipt, self.candidate, "hpldb")
        with self.assertRaisesRegex(ValueError, "old node pod remains"):
            replacement_ready(pods, "0", "ds", "0", "ds-new", self.candidate)

    def test_dry_run_rechecks_live_state_and_never_deletes(self):
        import io
        from contextlib import redirect_stdout
        from supervise_rollout import main

        ds = copy.deepcopy(self.bundle["daemonset"])
        ds["metadata"]["resourceVersion"] = "1"
        ds["spec"]["template"] = {
            "spec": {
                "containers": [
                    {"name": "driver", "image": self.candidate["images"]["driver"]}
                ]
            }
        }
        pods = copy.deepcopy(self.bundle["nodePods"]["items"])
        for i, p in enumerate(pods):
            p["metadata"].update(uid=str(i), name="plugin-" + str(i))
            p["metadata"]["labels"]["controller-revision-hash"] = "old"
            p["status"]["conditions"] = [{"type": "Ready", "status": "True"}]
        node = {
            "metadata": {"uid": "nodeuid", "name": "0"},
            "spec": {"unschedulable": True},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        }
        proof = {
            "remainingFuseMounts": [],
            "remainingFuseProcesses": [],
            "command": "inspection",
            "outputSHA256": "c" * 64,
            "namespaceIDs": ["mnt:1"],
        }
        receipt = {
            "kind": "OpenNebulaFuseDrain/v1",
            "cluster": "hpldb",
            "clusterUID": "hpldb",
            "node": "0",
            "nodeUID": "nodeuid",
            "nodePodUID": "0",
            "candidateSHA256": self.bundle["candidateSHA256"],
            "verifiedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
            "operatorApproval": "replace hpldb/0/0",
            "inspections": {
                k: proof
                for k in (
                    "hostMountNamespace",
                    "pluginMountNamespace",
                    "consumerMountNamespaces",
                )
            },
            "kernelMountsPreserved": True,
            "vmAttachmentsPreserved": True,
            "consumerPods": [],
        }
        calls = []

        def output(command, **kwargs):
            calls.append(command)
            if "ns" in command:
                return json.dumps({"metadata": {"uid": "hpldb"}})
            if "ds" in command:
                return json.dumps(ds)
            if "node" in command:
                return json.dumps(node)
            return json.dumps({"items": pods})

        with tempfile.TemporaryDirectory() as tmp:
            c = Path(tmp) / "c.json"
            r = Path(tmp) / "r.json"
            c.write_text(json.dumps(self.candidate))
            r.write_text(json.dumps(receipt))
            argv = [
                "supervise_rollout.py",
                "--context",
                "hpldb",
                "--node",
                "0",
                "--candidate",
                str(c),
                "--drain",
                str(r),
            ]
            with (
                patch("sys.argv", argv),
                patch("supervise_rollout.subprocess.check_output", side_effect=output),
                patch("supervise_rollout.subprocess.run") as mutate,
                redirect_stdout(io.StringIO()) as out,
            ):
                main()
                mutate.assert_not_called()
                self.assertFalse(json.loads(out.getvalue())["executed"])
            self.assertEqual(sum("ds" in command for command in calls), 2)
            self.assertTrue(
                all("--request-timeout=15s" in command for command in calls)
            )


class ChartPromotionTest(unittest.TestCase):
    def test_only_chart_metadata_is_ignored(self):
        from promote_chart import normalized

        original = "metadata:\n  labels:\n    helm.sh/chart: opennebula-csi-0.6.0-rc.1\nspec:\n  image: registry/image@sha256:abc\n  startupProbe:\n    failureThreshold: 60\n"
        self.assertEqual(
            normalized(original), normalized(original.replace("0.6.0-rc.1", "0.6.0"))
        )
        self.assertNotEqual(
            normalized(original),
            normalized(original.replace("failureThreshold: 60", "failureThreshold: 1")),
        )
        self.assertNotEqual(
            normalized(original),
            normalized(original.replace("sha256:abc", "sha256:other")),
        )


class PromoteCLITest(unittest.TestCase):
    setUp = EvidenceTest.setUp

    def test_actual_cli_uses_one_sealed_lookup_fixture_and_packages_original_template(
        self,
    ):
        import shutil
        import tarfile
        from upgrade_evidence import validate_receipt

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shutil.copytree(Path(__file__).resolve().parents[1] / "helm", root / "helm")
            chart_values = root / "helm/opennebula-csi/values.yaml"
            legacy_values = yaml.safe_load(chart_values.read_text())
            legacy_values["snapshotter"]["image"] = "registry.example/snapshotter:legacy"
            chart_values.write_text(yaml.safe_dump(legacy_values, sort_keys=False))

            def git(*args):
                return subprocess.check_output(
                    ["git", "-C", str(root), *args], text=True
                ).strip()

            git("init", "-q")
            git(
                "-c",
                "user.name=fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "add",
                "helm",
            )
            git(
                "-c",
                "user.name=fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-qm",
                "offline fixture",
            )
            candidate = copy.deepcopy(self.candidate)
            candidate["chartVersion"] = yaml.safe_load(
                (root / "helm/opennebula-csi/Chart.yaml").read_text()
            )["version"]
            candidate["sourceCommit"] = git("rev-parse", "HEAD")
            candidate["sourceTree"] = git("rev-parse", "HEAD^{tree}")
            for name in [
                "opennebula-csi",
                "csi-provisioner",
                "csi-attacher",
                "csi-resizer",
                "csi-node-driver-registrar",
                "liveness-probe",
                "csi-snapshotter",
            ]:
                candidate["images"][name] = "registry/" + name + "@sha256:" + "a" * 64
            binding = hashlib.sha256(
                json.dumps(candidate, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            receipt = {
                "kind": "OpenNebulaNativeQualification/v1",
                "sourceCommit": candidate["sourceCommit"],
                "sourceTree": candidate["sourceTree"],
                "images": candidate["images"],
                "candidateSHA256": binding,
                "providerVersion": "7.4.0",
                "approval": {
                    "operator": "OFFLINE FIXTURE",
                    "changeReference": "TEST ONLY",
                },
                "clusters": {},
            }
            for i, (cluster, count, minor) in enumerate(
                [("hpldb", 3, "1.36"), ("hplsvc", 10, "1.35"), ("bravo", 13, "1.35")]
            ):
                bundle = copy.deepcopy(self.bundle)
                bundle["helmRelease"]["status"]["lastAttemptedRevision"] = candidate[
                    "chartVersion"
                ]
                bundle["cluster"] = cluster
                bundle["clusterUID"] = cluster
                bundle["candidateSHA256"] = binding
                bundle["kubernetesVersion"] = "v" + minor + ".2"
                day = i * 2 + 1
                bundle["capturedAt"] = f"2026-01-{day + 1:02d}T00:00:00+00:00"
                for key in [
                    "desiredNumberScheduled",
                    "updatedNumberScheduled",
                    "numberReady",
                    "numberAvailable",
                ]:
                    bundle["daemonset"]["status"][key] = count
                prototype = bundle["nodePods"]["items"][0]
                bundle["nodePods"]["items"] = [
                    copy.deepcopy(prototype) for _ in range(count)
                ]
                for n, pod in enumerate(bundle["nodePods"]["items"]):
                    pod["spec"]["nodeName"] = str(n)
                gates = [
                    "createDelete",
                    "attachDetach",
                    "attachedExpandReattach",
                    "cloneResponseLoss",
                    "cephfsRecovery",
                    "lookupFaults",
                    "quarantinePreserved",
                    "fuseDrain",
                    "detachedGrowthRejected",
                    "diskSnapshotsRejected",
                ]
                receipt["clusters"][cluster] = {
                    "result": "passed",
                    "observationStartedAt": f"2026-01-{day:02d}T00:00:00+00:00",
                    "observationEndedAt": bundle["capturedAt"],
                    "disposableVolumesOnly": True,
                    "readback": bundle,
                    "tests": {
                        g: {
                            "result": "passed",
                            "commands": ["OFFLINE FIXTURE"],
                            "backendReceiptsSHA256": "a" * 64,
                            "artifactURI": "fixture:NOT-NATIVE",
                        }
                        for g in gates
                    },
                }
            validate_receipt(receipt, candidate, True)
            for cluster in ("hpldb", "hplsvc", "bravo"):
                substituted = copy.deepcopy(receipt)
                foreign = "hplsvc" if cluster == "hpldb" else "hpldb"
                readback = copy.deepcopy(receipt["clusters"][foreign]["readback"])
                readback["capturedAt"] = receipt["clusters"][cluster]["readback"]["capturedAt"]
                substituted["clusters"][cluster]["readback"] = readback
                with self.assertRaisesRegex(ValueError, "readback cluster differs"):
                    validate_receipt(substituted, candidate, True)
            c = root / "candidate.json"
            r = root / "native.json"
            c.write_text(json.dumps(candidate))
            r.write_text(json.dumps(receipt))
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).with_name("promote_chart.py").resolve()),
                    "--candidate",
                    str(c),
                    "--receipt",
                    str(r),
                    "--version",
                    "0.6.0",
                    "--output",
                    str(root / "out"),
                ],
                cwd=root,
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            with tarfile.open(root / "out/opennebula-csi-0.6.0.tgz") as archive:
                template = (
                    archive.extractfile(
                        "opennebula-csi/templates/last-node-preference-webhook.yaml"
                    )
                    .read()
                    .decode()
                )
                self.assertIn('lookup "v1" "Secret"', template)
                self.assertNotIn("$existing := fromJson", template)
                values = yaml.safe_load(
                    archive.extractfile("opennebula-csi/values.yaml")
                )
                self.assertTrue(
                    all(
                        v["digest"].startswith("sha256:")
                        for v in values["sidecarImages"].values()
                    )
                )
                self.assertEqual(values["snapshotter"]["image"], "")
            rendered = subprocess.check_output(
                ["helm", "template", "fixture", str(root / "out/opennebula-csi-0.6.0.tgz"),
                 "--set", "credentials.existingSecret.name=fixture"], text=True
            )
            controller = next(o for o in yaml.safe_load_all(rendered) if o and o.get("kind") == "StatefulSet")
            containers = controller["spec"]["template"]["spec"]["containers"]
            images = {c["name"]: c["image"] for c in containers}
            self.assertEqual(images["csi-snapshotter"], candidate["images"]["csi-snapshotter"])


class CandidateGuardTest(unittest.TestCase):
    def test_only_structured_registry_absence_permits_new_candidate(self):
        from guard_candidate_publication import assert_manifest_absent

        for status in (200, 401, 403, 500):
            with patch(
                "guard_candidate_publication.fetch", return_value=(status, {}, b"")
            ):
                with self.assertRaises((ValueError, KeyError)):
                    assert_manifest_absent("ghcr.io", "fixture", "v0.6.0-rc.1")
        with patch("guard_candidate_publication.fetch", return_value=(404, {}, b"")):
            assert_manifest_absent("ghcr.io", "fixture", "v0.6.0-rc.1")


if __name__ == "__main__":
    unittest.main()
