#!/usr/bin/env python3
"""Read-only rollout evidence and offline native receipt validation. Never reads Secrets."""

import argparse
import datetime as dt
import hashlib
import json
import re
import subprocess
from pathlib import Path

TARGETS = {"hpldb": (3, "1.36"), "hplsvc": (10, "1.35"), "bravo": (13, "1.35")}
SHA = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"[^\s]+@sha256:[0-9a-f]{64}")


def require(ok, message):
    if not ok:
        raise ValueError(message)


def ready(obj):
    return obj.get("status", {}).get("observedGeneration") == obj["metadata"][
        "generation"
    ] and any(
        c.get("type") == "Ready" and c.get("status") == "True"
        for c in obj.get("status", {}).get("conditions", [])
    )


def evaluate(bundle, candidate, max_age=300):
    """Consume serialized public API responses; absence or contradictory evidence fails closed."""
    cluster = bundle["cluster"]
    stamp = dt.datetime.fromisoformat(bundle["capturedAt"])
    require(
        stamp.tzinfo and stamp <= dt.datetime.now(dt.timezone.utc),
        "invalid readback timestamp",
    )
    if max_age is not None:
        require(
            (dt.datetime.now(dt.timezone.utc) - stamp).total_seconds() <= max_age,
            "readback expired",
        )
    require(
        bundle["candidateSHA256"]
        == hashlib.sha256(
            json.dumps(candidate, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "readback candidate binding differs",
    )
    count, minor = TARGETS[cluster]
    require(
        bundle["kubernetesVersion"].lstrip("v").startswith(minor + "."),
        "unexpected Kubernetes minor",
    )
    require(
        bundle["clusterUID"] == candidate["clusterUIDs"][cluster], "cluster UID differs"
    )
    require(
        SHA.fullmatch(candidate["sourceCommit"]) is not None, "invalid source commit"
    )
    images = candidate["images"]
    require(
        images and all(DIGEST.fullmatch(v) for v in images.values()),
        "all candidate containers require immutable digests",
    )
    ds = bundle["daemonset"]
    require(
        ds["spec"]["updateStrategy"]["type"] == "OnDelete",
        "unexpected rollout strategy",
    )
    require(
        ds["metadata"]["generation"] == ds["status"].get("observedGeneration"),
        "stale daemonset observation",
    )
    status = ds["status"]
    require(
        all(
            status.get(k) == count
            for k in (
                "desiredNumberScheduled",
                "updatedNumberScheduled",
                "numberReady",
                "numberAvailable",
            )
        ),
        "node rollout incomplete",
    )
    require(
        status.get("numberUnavailable", 0) == 0
        and status.get("numberMisscheduled", 0) == 0,
        "unavailable or misscheduled node pods",
    )
    revisions = {r["metadata"]["name"]: r for r in bundle["revisions"]["items"]}
    expected_revision = candidate["nodeRevisions"][cluster]
    revision = revisions[ds["metadata"]["name"] + "-" + expected_revision]
    require(
        any(
            o.get("uid") == ds["metadata"]["uid"] and o.get("controller")
            for o in revision["metadata"].get("ownerReferences", [])
        ),
        "revision not owned by daemonset",
    )
    nodes = set()
    for pod in bundle["nodePods"]["items"]:
        if pod["metadata"].get("deletionTimestamp"):
            raise ValueError("node pod terminating")
        require(
            any(
                o.get("uid") == ds["metadata"]["uid"] and o.get("controller")
                for o in pod["metadata"].get("ownerReferences", [])
            ),
            "unowned node pod",
        )
        require(
            pod["metadata"]["labels"].get("controller-revision-hash")
            == expected_revision,
            "old node revision",
        )
        require(pod["spec"]["nodeName"] not in nodes, "duplicate node pod")
        nodes.add(pod["spec"]["nodeName"])
        check_pod(pod, images, candidate.get("resolvedImageDigests", {}))
    require(len(nodes) == count, "eligible node coverage differs")
    for workload in (bundle["controller"], bundle["inventory"]):
        require(
            workload["status"].get("observedGeneration")
            == workload["metadata"]["generation"],
            "stale workload",
        )
        require(
            workload["status"].get("readyReplicas", 0)
            == workload["spec"].get("replicas", 1),
            "workload unready",
        )
    require(bundle["controlPods"]["items"], "missing controller/inventory pods")
    workload_uids = {
        w["metadata"]["uid"]: w for w in (bundle["controller"], bundle["inventory"])
    }
    counts = {uid: 0 for uid in workload_uids}
    replicasets = {r["metadata"]["uid"]: r for r in bundle["replicaSets"]["items"]}
    for pod in bundle["controlPods"]["items"]:
        owners = [
            o["uid"]
            for o in pod["metadata"].get("ownerReferences", [])
            if o.get("controller")
        ]
        require(len(owners) == 1, "control pod owner ambiguous")
        uid = owners[0]
        if uid in replicasets:
            owners = [
                o["uid"]
                for o in replicasets[uid]["metadata"].get("ownerReferences", [])
                if o.get("controller")
            ]
            require(len(owners) == 1, "replicaset owner ambiguous")
            uid = owners[0]
        require(uid in workload_uids, "unowned control pod")
        counts[uid] += 1
        expected_images = {
            c["name"]: c["image"]
            for c in workload_uids[uid]["spec"]["template"]["spec"]["containers"]
        }
        require(
            {c["name"]: c["image"] for c in pod["spec"]["containers"]}
            == expected_images,
            "control pod template differs",
        )
        if uid == bundle["controller"]["metadata"]["uid"]:
            require(
                pod["metadata"]["labels"].get("controller-revision-hash")
                == bundle["controller"]["status"]["updateRevision"],
                "old controller revision",
            )
        check_pod(pod, images, candidate.get("resolvedImageDigests", {}))
    require(
        all(
            counts[uid] == w["spec"].get("replicas", 1)
            for uid, w in workload_uids.items()
        ),
        "controller/inventory pod coverage differs",
    )
    require(
        ready(bundle["helmRelease"]) and ready(bundle["kustomization"]),
        "Flux objects not Ready at current generation",
    )
    require(
        bundle["helmRelease"]["status"].get("lastAttemptedRevision")
        == candidate["chartVersion"],
        "Flux chart version differs",
    )
    require(
        bundle["registrationVerified"] and bundle["readWriteVerified"],
        "registration/read-write receipts missing",
    )
    require(
        bundle["quarantineExceptions"] == candidate["quarantineExceptions"][cluster],
        "quarantine exception inventory changed",
    )
    return {
        "cluster": cluster,
        "nodes": sorted(nodes),
        "sourceCommit": candidate["sourceCommit"],
        "ready": True,
    }


def check_pod(pod, images, resolved):
    require(pod.get("status", {}).get("phase") == "Running", "pod not Running")
    statuses = {c["name"]: c for c in pod["status"].get("containerStatuses", [])}
    for container in pod["spec"]["containers"]:
        name = container["name"]
        require(
            name in images and container["image"] == images[name],
            "pod image reference differs: " + name,
        )
        actual = statuses.get(name, {})
        require(
            actual.get("ready")
            and any(
                d in actual.get("imageID", "")
                for d in [images[name].split("@", 1)[1]]
                + resolved.get(images[name], [])
            ),
            "container not ready at candidate digest: " + name,
        )


def validate_receipt(receipt, candidate, stable=False):
    require(
        receipt["kind"] == "OpenNebulaNativeQualification/v1",
        "not native qualification evidence",
    )
    require(
        receipt["sourceCommit"] == candidate["sourceCommit"], "source custody differs"
    )
    require(receipt["images"] == candidate["images"], "image custody differs")
    require(
        SHA.fullmatch(candidate["sourceTree"]) is not None
        and receipt["sourceTree"] == candidate["sourceTree"],
        "tree custody differs",
    )
    require(
        receipt["approval"]["operator"] and receipt["approval"]["changeReference"],
        "trusted operator approval missing",
    )
    require(
        receipt["candidateSHA256"]
        == hashlib.sha256(
            json.dumps(candidate, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "candidate digest differs",
    )
    require(receipt["providerVersion"].startswith("7.4."), "unsupported provider")
    clusters = ["hpldb", "hplsvc", "bravo"] if stable else ["hpldb"]
    previous_end = None
    for cluster in clusters:
        result = receipt["clusters"][cluster]
        require(result["result"] == "passed", "native qualification failed")
        begin = dt.datetime.fromisoformat(result["observationStartedAt"])
        end = dt.datetime.fromisoformat(result["observationEndedAt"])
        require(
            begin.tzinfo and end.tzinfo and (end - begin).total_seconds() >= 86400,
            "24-hour observation incomplete",
        )
        require(end <= dt.datetime.now(dt.timezone.utc), "future observation")
        require(
            previous_end is None or begin >= previous_end, "qualification order differs"
        )
        previous_end = end
        require(result["disposableVolumesOnly"] is True, "non-disposable qualification")
        captured = dt.datetime.fromisoformat(result["readback"]["capturedAt"])
        require(
            begin <= captured <= end and (end - captured).total_seconds() <= 300,
            "final readback is outside observation end",
        )
        require(result["readback"]["cluster"] == cluster, "readback cluster differs")
        evaluate(result["readback"], candidate, max_age=None)
        for gate in (
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
        ):
            evidence = result["tests"].get(gate, {})
            require(
                evidence.get("result") == "passed"
                and evidence.get("commands")
                and evidence.get("backendReceiptsSHA256")
                and evidence.get("artifactURI"),
                "missing captured native test evidence: " + gate,
            )
    return True


def collect(context, candidate=None):
    require(context in TARGETS, "unknown target")

    def get(*args):
        output = subprocess.check_output(
            [
                "rtk",
                "proxy",
                "kc",
                context,
                "--request-timeout=15s",
                *args,
                "-o",
                "json",
            ],
            text=True,
            timeout=20,
        )
        return json.loads(output)

    ns = get("get", "namespace", "kube-system")
    return {
        "capturedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
        "candidateSHA256": hashlib.sha256(
            json.dumps(candidate, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if candidate
        else None,
        "cluster": context,
        "clusterUID": ns["metadata"]["uid"],
        "kubernetesVersion": json.loads(
            subprocess.check_output(
                [
                    "rtk",
                    "proxy",
                    "kc",
                    context,
                    "--request-timeout=15s",
                    "version",
                    "-o",
                    "json",
                ],
                text=True,
                timeout=20,
            )
        )["serverVersion"]["gitVersion"],
        "daemonset": get(
            "-n", "kube-system", "get", "daemonset", "opennebula-csi-node"
        ),
        "controller": get(
            "-n", "kube-system", "get", "statefulset", "opennebula-csi-controller"
        ),
        "inventory": get(
            "-n", "kube-system", "get", "deployment", "opennebula-csi-inventory"
        ),
        "revisions": get("-n", "kube-system", "get", "controllerrevisions"),
        "replicaSets": get(
            "-n",
            "kube-system",
            "get",
            "replicasets",
            "-l",
            "app.kubernetes.io/instance=opennebula-csi",
        ),
        "nodePods": get(
            "-n",
            "kube-system",
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/instance=opennebula-csi,app.kubernetes.io/component=node",
        ),
        "controlPods": get(
            "-n",
            "kube-system",
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/instance=opennebula-csi,app.kubernetes.io/component in (controller,inventory-controller)",
        ),
        "helmRelease": get("-n", "kube-system", "get", "helmrelease", "opennebula-csi"),
        "kustomization": get(
            "-n", "flux-system", "get", "kustomization", "infra-opennebula-csi"
        ),
        "registrationVerified": False,
        "readWriteVerified": False,
        "quarantineExceptions": None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--context", choices=TARGETS, required=True)
    c.add_argument("--candidate", type=Path, required=True)
    for command in ("check", "qualify"):
        c = sub.add_parser(command)
        c.add_argument("--candidate", type=Path, required=True)
        c.add_argument("--evidence", type=Path, required=True)
        if command == "qualify":
            c.add_argument("--stable", action="store_true")
    args = parser.parse_args()
    if args.command == "collect":
        print(
            json.dumps(
                collect(args.context, json.loads(args.candidate.read_text())), indent=2
            )
        )
        return
    candidate = json.loads(args.candidate.read_text())
    evidence = json.loads(args.evidence.read_text())
    result = (
        evaluate(evidence, candidate)
        if args.command == "check"
        else validate_receipt(evidence, candidate, args.stable)
    )
    print(json.dumps(result))


if __name__ == "__main__":
    main()
