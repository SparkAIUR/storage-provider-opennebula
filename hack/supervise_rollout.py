#!/usr/bin/env python3
"""Supervise one approved OnDelete node replacement. Dry-run unless --execute is supplied.
Consumer shutdown and mount/process inspection are operator-owned. No forced cleanup or retry.
"""

import argparse
import datetime as dt
import hashlib
import json
import subprocess
import time
from pathlib import Path
from upgrade_evidence import DIGEST, TARGETS, require, check_pod


def validate_drain(receipt, candidate, context, node, uid, now):
    require(receipt["kind"] == "OpenNebulaFuseDrain/v1", "wrong drain receipt")
    require(
        receipt["cluster"] == context
        and receipt["clusterUID"] == candidate["clusterUIDs"][context],
        "cluster identity differs",
    )
    require(
        receipt["node"] == node and receipt["nodePodUID"] == uid,
        "node/pod identity differs",
    )
    digest = hashlib.sha256(
        json.dumps(candidate, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    require(receipt["candidateSHA256"] == digest, "candidate changed since approval")
    stamp = dt.datetime.fromisoformat(receipt["verifiedAt"])
    require(
        stamp.tzinfo and 0 <= (now - stamp).total_seconds() <= 300,
        "mount/process evidence expired",
    )
    require(
        receipt["operatorApproval"] == f"replace {context}/{node}/{uid}",
        "explicit one-node approval missing",
    )
    for key in (
        "hostMountNamespace",
        "pluginMountNamespace",
        "consumerMountNamespaces",
    ):
        evidence = receipt["inspections"][key]
        require(
            evidence["remainingFuseMounts"] == []
            and evidence["remainingFuseProcesses"] == [],
            "FUSE references remain",
        )
        require(
            evidence["command"]
            and evidence["outputSHA256"]
            and evidence["namespaceIDs"],
            "mount/process inspection missing",
        )
    require(
        receipt["kernelMountsPreserved"] and receipt["vmAttachmentsPreserved"],
        "kernel mounts/VM attachments not preserved",
    )
    require(
        receipt["consumerPods"] is not None, "named FUSE consumer inventory missing"
    )
    return True


def pod_ready(pod):
    containers = pod.get("spec", {}).get("containers", [])
    statuses = pod.get("status", {}).get("containerStatuses", [])
    return (
        bool(containers)
        and pod.get("status", {}).get("phase") == "Running"
        and not pod["metadata"].get("deletionTimestamp")
        and any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in pod["status"].get("conditions", [])
        )
        and {c["name"] for c in containers} == {c["name"] for c in statuses}
        and all(c.get("ready") for c in statuses)
    )


def validate_live(ds, pods, node, receipt, candidate, context):
    count = TARGETS[context][0]
    require(
        node["metadata"]["uid"] == receipt["nodeUID"]
        and node["metadata"]["name"] == receipt["node"],
        "node UID differs",
    )
    require(
        node["spec"].get("unschedulable") is True, "node must be cordoned before drain"
    )
    require(
        any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in node["status"].get("conditions", [])
        ),
        "node not Ready",
    )
    require(
        ds["spec"]["updateStrategy"]["type"] == "OnDelete", "wrong live update strategy"
    )
    require(
        ds["status"].get("observedGeneration") == ds["metadata"]["generation"],
        "stale daemonset",
    )
    require(
        all(
            ds["status"].get(k) == count
            for k in ("desiredNumberScheduled", "numberReady", "numberAvailable")
        ),
        "another node unavailable",
    )
    for container in ds["spec"]["template"]["spec"]["containers"]:
        require(
            DIGEST.fullmatch(container["image"])
            and container["image"] == candidate["images"][container["name"]],
            "template candidate differs",
        )
    require(
        len(pods) == count and len({p["spec"]["nodeName"] for p in pods}) == count,
        "eligible pod coverage differs",
    )
    for pod in pods:
        require(
            pod_ready(pod), "another node unavailable or incomplete container readiness"
        )
        require(
            any(
                o.get("uid") == ds["metadata"]["uid"] and o.get("controller")
                for o in pod["metadata"].get("ownerReferences", [])
            ),
            "unowned node pod",
        )
    matching = [p for p in pods if p["spec"]["nodeName"] == node["metadata"]["name"]]
    require(len(matching) == 1, "node pod ambiguous")
    pod = matching[0]
    require(pod["metadata"]["uid"] == receipt["nodePodUID"], "node pod changed")
    require(
        pod["metadata"]["labels"].get("controller-revision-hash")
        != candidate["nodeRevisions"][context],
        "node already upgraded",
    )
    return pod


def replacement_ready(pods, old_uid, ds_uid, node, revision, candidate):
    require(all(p["metadata"]["uid"] != old_uid for p in pods), "old node pod remains")
    matching = [p for p in pods if p["spec"]["nodeName"] == node]
    require(len(matching) == 1 and pod_ready(matching[0]), "replacement not ready")
    pod = matching[0]
    require(
        any(
            o.get("uid") == ds_uid and o.get("controller")
            for o in pod["metadata"].get("ownerReferences", [])
        ),
        "replacement unowned",
    )
    require(
        pod["metadata"]["labels"].get("controller-revision-hash") == revision,
        "replacement revision differs",
    )
    check_pod(pod, candidate["images"], candidate.get("resolvedImageDigests", {}))
    return pod["metadata"]["uid"]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--context", choices=TARGETS, required=True)
    p.add_argument("--node", required=True)
    p.add_argument("--candidate", type=Path, required=True)
    p.add_argument("--drain", type=Path, required=True)
    p.add_argument("--execute", action="store_true")
    args = p.parse_args()
    candidate = json.loads(args.candidate.read_text())
    receipt = json.loads(args.drain.read_text())

    def kc(*words):
        return subprocess.check_output(
            ["rtk", "proxy", "kc", args.context, "--request-timeout=15s", *words],
            text=True,
            timeout=20,
        )

    def get(*words):
        return json.loads(kc(*words, "-o", "json"))

    ns = get("get", "ns", "kube-system")
    require(
        ns["metadata"]["uid"] == candidate["clusterUIDs"][args.context],
        "wrong live cluster",
    )
    selector = (
        "app.kubernetes.io/component=node,app.kubernetes.io/instance=opennebula-csi"
    )

    def live():
        ds = get("-n", "kube-system", "get", "ds", "opennebula-csi-node")
        pods = get("-n", "kube-system", "get", "pods", "-l", selector)["items"]
        node = get("get", "node", args.node)
        return ds, validate_live(ds, pods, node, receipt, candidate, args.context)

    ds, pod = live()
    validate_drain(
        receipt,
        candidate,
        args.context,
        args.node,
        pod["metadata"]["uid"],
        dt.datetime.now(dt.timezone.utc),
    )
    for consumer in receipt["consumerPods"]:
        pods = get("-n", consumer["namespace"], "get", "pods")["items"]
        require(
            all(p["metadata"]["uid"] != consumer["uid"] for p in pods),
            "consumer still running",
        )
        require(
            all(
                p["spec"].get("nodeName") != args.node
                or p["metadata"].get("labels", {}).get(consumer["selectorKey"])
                != consumer["selectorValue"]
                for p in pods
            ),
            "consumer replacement on node",
        )
    # Recheck both external authority and original evidence expiry after all consumer awaits.
    current_ds, current_pod = live()
    require(
        current_ds["metadata"]["uid"] == ds["metadata"]["uid"]
        and current_ds["metadata"]["resourceVersion"]
        == ds["metadata"]["resourceVersion"],
        "daemonset changed during drain validation",
    )
    validate_drain(
        receipt,
        candidate,
        args.context,
        args.node,
        current_pod["metadata"]["uid"],
        dt.datetime.now(dt.timezone.utc),
    )
    options = {
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "preconditions": {"uid": pod["metadata"]["uid"]},
        "gracePeriodSeconds": 30,
    }
    action = {
        "node": args.node,
        "pod": pod["metadata"]["name"],
        "deleteOptions": options,
        "executed": args.execute,
    }
    if args.execute:
        subprocess.run(
            [
                "rtk",
                "proxy",
                "kc",
                args.context,
                "--request-timeout=15s",
                "delete",
                "--raw",
                "/api/v1/namespaces/kube-system/pods/" + pod["metadata"]["name"],
                "-f",
                "-",
            ],
            input=json.dumps(options),
            text=True,
            check=True,
            timeout=20,
        )
        deadline = time.monotonic() + 1800
        while True:
            pods = get("-n", "kube-system", "get", "pods", "-l", selector)["items"]
            try:
                action["replacementUID"] = replacement_ready(
                    pods,
                    pod["metadata"]["uid"],
                    ds["metadata"]["uid"],
                    args.node,
                    candidate["nodeRevisions"][args.context],
                    candidate,
                )
                break
            except ValueError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "replacement not verified within30m; stop and retain evidence"
                    )
                time.sleep(5)
        action["nextGate"] = (
            "Verify mount ownership, CSI registration and disposable read/write; restore only named consumers, then obtain approval for the next node."
        )
    print(json.dumps(action, indent=2))


if __name__ == "__main__":
    main()
