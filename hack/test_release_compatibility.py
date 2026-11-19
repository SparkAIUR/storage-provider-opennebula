import copy
import hashlib
import io
import json
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

import test_upgrade_evidence as fixtures
from release_compatibility import build_record, store_record


RUNS_URL = "https://github.com/SparkAIUR/storage-provider-opennebula/actions/runs"
NATIVE_CLUSTER_SCOPE = (
    ("hpldb", 3, "1.36"),
    ("hplsvc", 10, "1.35"),
    ("bravo", 13, "1.35"),
)


def raw_sha256(data):
    return hashlib.sha256(data).hexdigest()


def set_node_count(bundle, count):
    bundle["daemonset"]["status"].update(
        desiredNumberScheduled=count,
        updatedNumberScheduled=count,
        numberReady=count,
        numberAvailable=count,
    )
    template = bundle["nodePods"]["items"][0]
    pods = [copy.deepcopy(template) for _ in range(count)]
    for index, pod in enumerate(pods):
        pod["spec"]["nodeName"] = str(index)
    bundle["nodePods"]["items"] = pods


def set_pod_images(pod, images, names):
    containers = [{"name": name, "image": images[name]} for name in names]
    pod["spec"]["containers"] = containers
    pod["status"]["containerStatuses"] = [
        {"name": container["name"], "ready": True, "imageID": container["image"]}
        for container in containers
    ]


class ReleaseCompatibilityTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.assets = self.root / "assets"
        self.assets.mkdir()
        self.qualification = self.root / "qualification"
        self.qualification.mkdir()
        self.records = self.root / "records"
        self.records.mkdir()
        self.readback = {
            "csiTag": "v0.6.0-rc.4",
            "openNebulaVersion": "7.4.0",
            "profile": "opennebula-74-upgrade/v1",
            "exclusions": [
                "detached-disk-growth",
                "disk-snapshots",
                "affected-persistent-system-self-growth-reattach",
            ],
            "sourceCommit": "b" * 40,
            "sourceTree": "c" * 40,
            "imageDigest": "sha256:" + "a" * 64,
            "publication": {"releaseId": 314, "runId": 111, "runAttempt": 1},
        }

    def write_chart(self, version, app_version):
        path = self.assets / f"opennebula-csi-{version}.tgz"
        metadata = f"name: opennebula-csi\nversion: {version}\nappVersion: {app_version}\n"
        with tarfile.open(path, "w:gz") as archive:
            member = tarfile.TarInfo("opennebula-csi/Chart.yaml")
            member.size = len(metadata)
            archive.addfile(member, io.BytesIO(metadata.encode()))
        self.readback["chartSHA256"] = raw_sha256(path.read_bytes())

    def candidate_assets(self):
        self.write_chart("0.6.0-rc.4", "v0.6.0-rc.4")
        for name, value in {
            "source-commit.txt": self.readback["sourceCommit"],
            "source-tree.txt": self.readback["sourceTree"],
            "image.txt": "ghcr.io/sparkaiur/opennebula-csi@" + self.readback["imageDigest"],
        }.items():
            (self.assets / name).write_text(value + "\n")
        sums = "".join(
            f"{raw_sha256(p.read_bytes())}  _candidate/{p.name}\n"
            for p in sorted(self.assets.iterdir())
        )
        (self.assets / "SHA256SUMS").write_text(sums)
        self.readback["evidence"] = {"sha256": raw_sha256(sums.encode())}

    def stable_assets(self):
        fixtures.EvidenceTest.setUp(self)
        self.candidate["chartVersion"] = "0.6.0-rc.4"
        self.readback["imageDigest"] = "sha256:" + "7" * 64
        driver_image = "nudevco/opennebula-csi@" + self.readback["imageDigest"]
        images = {
            "opennebula-csi": driver_image,
            "inventory-controller": driver_image,
            **{
                name: f"registry.k8s.io/sig-storage/{repository}@sha256:{digit * 64}"
                for name, repository, digit in (
                    ("csi-provisioner", "csi-provisioner", "1"),
                    ("csi-attacher", "csi-attacher", "2"),
                    ("csi-resizer", "csi-resizer", "3"),
                    ("csi-node-driver-registrar", "csi-node-driver-registrar", "4"),
                    ("liveness-probe", "livenessprobe", "5"),
                    ("csi-snapshotter", "csi-snapshotter", "6"),
                )
            },
        }
        self.candidate["images"] = images
        self.bundle["daemonset"]["metadata"]["name"] = "opennebula-csi-node"
        self.bundle["revisions"]["items"][0]["metadata"]["name"] = "opennebula-csi-node-ds-new"
        for pod in self.bundle["nodePods"]["items"]:
            set_pod_images(pod, images, ("opennebula-csi", "csi-node-driver-registrar", "liveness-probe"))
        controller, inventory = self.bundle["controlPods"]["items"]
        set_pod_images(controller, images, (
            "opennebula-csi", "csi-provisioner", "csi-attacher", "csi-resizer", "liveness-probe",
        ))
        set_pod_images(inventory, images, ("inventory-controller",))
        for workload, pod in (("controller", controller), ("inventory", inventory)):
            resource = self.bundle[workload]
            resource["metadata"]["name"] = f"opennebula-csi-{workload}"
            resource["spec"]["template"]["spec"]["containers"] = copy.deepcopy(pod["spec"]["containers"])
        self.readback["csiTag"] = "v0.6.0"
        self.readback["exclusions"].pop()
        self.readback["publication"]["releaseId"] = 315
        self.readback["evidence"] = {"runId": 222, "runAttempt": 3}
        self.write_chart("0.6.0", "v0.6.0-rc.4")
        (self.qualification / "candidate.json").write_text(json.dumps(self.candidate))
        candidate_sha = raw_sha256(json.dumps(
            self.candidate, sort_keys=True, separators=(",", ":")
        ).encode())
        self.receipt = {
            "kind": "OpenNebulaNativeQualification/v1",
            "sourceCommit": self.candidate["sourceCommit"],
            "sourceTree": self.candidate["sourceTree"],
            "candidateSHA256": candidate_sha,
            "images": self.candidate["images"],
            "providerVersion": "7.4.0",
            "approval": {"operator": "OFFLINE FIXTURE", "changeReference": "TEST ONLY"},
            "clusters": {},
        }
        for i, (cluster, count, minor) in enumerate(NATIVE_CLUSTER_SCOPE, start=1):
            stamp = f"2026-01-{i + 1:02d}T00:00:00+00:00"
            bundle = copy.deepcopy(self.bundle)
            bundle.update(
                cluster=cluster,
                clusterUID=cluster,
                capturedAt=stamp,
                candidateSHA256=candidate_sha,
                kubernetesVersion=f"v{minor}.0",
            )
            bundle["helmRelease"]["status"]["lastAttemptedRevision"] = "0.6.0-rc.4"
            set_node_count(bundle, count)
            self.receipt["clusters"][cluster] = {
                "result": "passed",
                "observationStartedAt": f"2026-01-{i:02d}T00:00:00+00:00",
                "observationEndedAt": stamp,
                "disposableVolumesOnly": True,
                "readback": bundle,
                "tests": {gate: {
                    "result": "passed", "commands": ["OFFLINE FIXTURE"],
                    "backendReceiptsSHA256": "a" * 64, "artifactURI": "fixture:NOT-NATIVE",
                } for gate in (
                    "createDelete", "attachDetach", "attachedExpandReattach",
                    "cloneResponseLoss", "cephfsRecovery", "lookupFaults",
                    "quarantinePreserved", "fuseDrain", "detachedGrowthRejected",
                    "diskSnapshotsRejected",
                )},
            }
        self.write_receipt()

    def write_receipt(self):
        data = (json.dumps(self.receipt) + "\n").encode()
        (self.qualification / "native.json").write_bytes(data)
        self.readback["evidence"]["sha256"] = raw_sha256(data)

    def readback_bytes(self):
        return (json.dumps(self.readback) + "\n").encode()

    def record(self, qualification=None):
        data = self.readback_bytes()
        return build_record(data, raw_sha256(data), self.assets, qualification)

    def run_cli(self, *modes, qualification=None):
        data = self.readback_bytes()
        readback = self.root / "readback.json"
        readback.write_bytes(data)
        options = [
            "--readback", str(readback), "--approved-readback-sha256", raw_sha256(data),
            "--release-assets", str(self.assets), "--record-dir", str(self.records),
        ]
        if qualification is not None:
            options.extend(("--qualification", str(qualification)))
        for mode in modes:
            subprocess.run(
                [sys.executable, str(Path(__file__).with_name("release_compatibility.py")), mode, *options],
                check=True,
                capture_output=True,
            )
        return data

    def assert_hash_bindings(self, record, chart, readback, evidence):
        self.assertEqual(record["chart"]["sha256"], raw_sha256(chart.read_bytes()))
        self.assertEqual(record["publication"]["readbackSHA256"], raw_sha256(readback))
        self.assertEqual(record["evidence"]["sha256"], raw_sha256(evidence.read_bytes()))

    def test_candidate_cli_and_immutable_retry(self):
        self.candidate_assets()
        data = self.run_cli("author", "author", "verify")
        path = self.records / "release-compatibility-7.4.0-csi.0.6.0-rc.4.json"
        original = path.read_bytes()
        record = json.loads(original)
        self.assertEqual(record["status"], "target-only")
        publication_url = f"{RUNS_URL}/111/attempts/1"
        self.assertEqual(record["publication"]["releaseId"], 314)
        self.assertEqual(record["publication"]["url"], publication_url)
        self.assertEqual(record["evidence"]["url"], publication_url)
        self.assert_hash_bindings(
            record, self.assets / "opennebula-csi-0.6.0-rc.4.tgz", data, self.assets / "SHA256SUMS",
        )

        self.readback["publication"]["runAttempt"] = 2
        with self.assertRaisesRegex(ValueError, "approved readback hash differs"):
            build_record(self.readback_bytes(), raw_sha256(data), self.assets)
        with self.assertRaisesRegex(ValueError, "immutable record differs"):
            store_record(self.records, self.record(), verify=False)
        self.readback["publication"]["runAttempt"] = 1
        self.readback["publication"]["releaseId"] = 315
        for verify in (False, True):
            with self.assertRaisesRegex(ValueError, "immutable record differs"):
                store_record(self.records, self.record(), verify=verify)
        self.readback["publication"]["releaseId"] = 314
        self.assertEqual(path.read_bytes(), original)

        self.readback["openNebulaVersion"] = "7.4.2"
        with self.assertRaisesRegex(ValueError, "scope differs"):
            self.record()
        self.readback["exclusions"].pop()
        self.assertEqual(self.record()["status"], "target-only")
        (self.assets / "source-tree.txt").write_text("e" * 40 + "\n")
        with self.assertRaisesRegex(ValueError, "source-tree.txt differs"):
            self.record()

    def test_stable_requires_exact_native_patch_and_complete_contract(self):
        self.stable_assets()
        with self.assertRaisesRegex(ValueError, "requires protected native evidence"):
            self.record()
        data = self.run_cli("author", "verify", qualification=self.qualification)
        path = self.records / "release-compatibility-7.4.0-csi.0.6.0.json"
        record = json.loads(path.read_bytes())
        self.assertEqual(record["discoveryRef"], "7.4.0-csi.0.6.0")
        self.assertEqual(record["status"], "native-verified")
        self.assertEqual(record["chart"]["appVersion"], "v0.6.0-rc.4")
        self.assertEqual(record["publication"]["releaseId"], 315)
        self.assertEqual(record["publication"]["url"], f"{RUNS_URL}/111/attempts/1")
        self.assertEqual(record["evidence"]["url"], f"{RUNS_URL}/222/attempts/3")
        driver_digests = set()
        for cluster in self.receipt["clusters"].values():
            bundle = cluster["readback"]
            pods = bundle["nodePods"]["items"] + bundle["controlPods"]["items"]
            for pod in pods:
                (driver_status,) = (
                    status for status in pod["status"]["containerStatuses"]
                    if status["name"] in ("opennebula-csi", "inventory-controller")
                )
                driver_digests.add(driver_status["imageID"].split("@", 1)[1])
        (observed_digest,) = driver_digests
        self.assertEqual(record["images"], [
            f"ghcr.io/sparkaiur/opennebula-csi@{observed_digest}",
            f"docker.io/nudevco/opennebula-csi@{observed_digest}",
        ])
        self.assert_hash_bindings(
            record, self.assets / "opennebula-csi-0.6.0.tgz", data, self.qualification / "native.json",
        )
        self.assertNotIn("OFFLINE FIXTURE", json.dumps(record))
        self.assertNotIn("fixture:NOT-NATIVE", json.dumps(record))

        for field, changed, error in (
            ("sourceTree", "e" * 40, "qualified source identity differs"),
            ("imageDigest", "sha256:" + "e" * 64, "qualified image differs"),
            ("chartSHA256", "e" * 64, "published chart hash differs"),
        ):
            with self.subTest(field=field):
                original = self.readback[field]
                self.readback[field] = changed
                with self.assertRaisesRegex(ValueError, error):
                    self.record(self.qualification)
                self.readback[field] = original
        self.readback["openNebulaVersion"] = "7.4.2"
        with self.assertRaisesRegex(ValueError, "native provider patch differs"):
            self.record(self.qualification)
        self.readback["openNebulaVersion"] = "7.4.0"
        self.receipt["clusters"]["bravo"]["tests"]["attachedExpandReattach"]["result"] = "guarded"
        with self.assertRaisesRegex(ValueError, "native receipt hash differs"):
            (self.qualification / "native.json").write_text(json.dumps(self.receipt))
            self.record(self.qualification)
        self.write_receipt()
        with self.assertRaisesRegex(ValueError, "complete stable contract"):
            self.record(self.qualification)

    def test_release_id_must_be_a_positive_integer(self):
        self.candidate_assets()
        for release_id in (0, -1, True, 314.0, "314"):
            with self.subTest(release_id=release_id):
                self.readback["publication"]["releaseId"] = release_id
                with self.assertRaisesRegex(ValueError, "invalid GitHub release ID"):
                    self.record()

    def test_stable_rejects_nine_node_hplsvc(self):
        self.stable_assets()
        bundle = self.receipt["clusters"]["hplsvc"]["readback"]
        set_node_count(bundle, 9)
        self.write_receipt()
        with self.assertRaisesRegex(ValueError, "complete stable contract"):
            self.record(self.qualification)
