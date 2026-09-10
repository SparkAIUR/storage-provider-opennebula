#!/usr/bin/env python3
"""Validate an ext4 10Gi -> 40Gi expansion on an explicitly selected lab node.

Creates only disposable, uniquely named resources. On failure they remain for
inspection; the printed namespace and StorageClass identify the cleanup scope.
"""
import argparse
import datetime
import json
from pathlib import Path
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster", choices=["hplmon"], required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--storage-class", default="one")
    parser.add_argument("--expected-image-digest", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    name = "csi-expand-" + uuid.uuid4().hex[:10]
    print(f"Validation namespace and StorageClass: {name}", flush=True)

    def kc(*command, data=None, missing=False):
        result = subprocess.run(
            ["kc", args.cluster, *command],
            input=None if data is None else json.dumps(data),
            text=True, capture_output=True, timeout=60,
        )
        if missing and result.returncode and "NotFound" in result.stderr:
            return None
        if result.returncode:
            raise RuntimeError(f"kc {command}: {result.stderr}")
        return result.stdout

    def get(*command, missing=False):
        raw = kc("get", *command, "-o", "json", missing=missing)
        return None if raw is None else json.loads(raw)

    def wait(message, predicate, seconds=600):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                print(message, flush=True)
                return value
            time.sleep(5)
        raise RuntimeError(f"Timed out: {message}; inspect namespace {name}")

    def ready():
        pod = get("pod", "writer", "-n", name)
        return pod if any(c["type"] == "Ready" and c["status"] == "True"
                          for c in pod.get("status", {}).get("conditions", [])) else None

    plugins = [p for p in get("pods", "-n", "kube-system")["items"]
               if p["metadata"]["name"].startswith("opennebula-csi-node-")
               and p["spec"].get("nodeName") == args.node]
    assert len(plugins) == 1, "expected one CSI node plugin"
    plugin = plugins[0]
    driver = next(c for c in plugin["status"]["containerStatuses"] if c["name"] == "opennebula-csi")
    assert driver["ready"] and driver["imageID"].endswith(args.expected_image_digest), driver
    sc = get("storageclass", args.storage_class)
    assert sc["provisioner"] == "csi.opennebula.io"
    sc["parameters"]["fsType"] = "ext4"
    sc["metadata"] = {"name": name, "labels": {"app.kubernetes.io/name": name}}
    sc["reclaimPolicy"] = "Delete"
    sc["allowVolumeExpansion"] = True
    sc.pop("status", None)
    kc("create", "-f", "-", data=sc)
    kc("create", "-f", "-", data={"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}})
    kc("create", "-f", "-", data={
        "apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": "data", "namespace": name},
        "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": name,
                 "resources": {"requests": {"storage": "10Gi"}}},
    })
    pod_spec = {
        "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "writer", "namespace": name},
        "spec": {"nodeSelector": {"kubernetes.io/hostname": args.node},
                 "terminationGracePeriodSeconds": 10,
                 "containers": [{"name": "writer", "image": "alpine:3.22",
                                 "command": ["sleep", "36000"],
                                 "resources": {"requests": {"cpu": "10m", "memory": "32Mi"},
                                               "limits": {"cpu": "100m", "memory": "128Mi"}},
                                 "volumeMounts": [{"name": "data", "mountPath": "/data"}]}],
                 "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "data"}}]},
    }
    kc("create", "-f", "-", data=pod_spec)
    first_pod = wait("Initial pod Ready", ready)
    claim = get("pvc", "data", "-n", name)
    pv_name = claim["spec"]["volumeName"]
    kc("exec", "-n", name, "writer", "--", "dd", "if=/dev/urandom", "of=/data/probe.bin", "bs=1M", "count=16", "conv=fsync")
    checksum = kc("exec", "-n", name, "writer", "--", "sha256sum", "/data/probe.bin").split()[0]
    patch = [{"op": "test", "path": "/metadata/uid", "value": claim["metadata"]["uid"]},
             {"op": "replace", "path": "/spec/resources/requests/storage", "value": "40Gi"}]
    kc("patch", "pvc", "data", "-n", name, "--type=json", "-p", json.dumps(patch))

    def expanded():
        current = get("pvc", "data", "-n", name)
        status = current.get("status", {})
        return current if status.get("capacity", {}).get("storage") == "40Gi" and not status.get("conditions") else None

    expanded_claim = wait("PVC reports 40Gi without resize conditions", expanded)
    assert kc("exec", "-n", name, "writer", "--", "sha256sum", "/data/probe.bin").split()[0] == checksum
    filesystem = kc("exec", "-n", name, "writer", "--", "df", "-k", "/data")
    kc("delete", "pod", "writer", "-n", name, "--wait=true", "--timeout=45s")
    kc("create", "-f", "-", data=pod_spec)
    second_pod = wait("Replacement pod Ready on expanded PVC", ready)
    assert first_pod["metadata"]["uid"] != second_pod["metadata"]["uid"]
    assert kc("exec", "-n", name, "writer", "--", "sha256sum", "/data/probe.bin").split()[0] == checksum
    kc("exec", "-n", name, "writer", "--", "dd", "if=/dev/zero", "of=/data/after-resize.bin", "bs=1M", "count=1", "conv=fsync")
    logs = kc("logs", "-n", "kube-system", plugin["metadata"]["name"], "-c", "opennebula-csi", "--since=20m", "--tail=10000")
    evidence = [line for line in logs.splitlines() if pv_name in line and "NodeExpandVolume converged" in line]
    assert evidence, "missing successful node expansion log"
    kc("delete", "namespace", name, "--wait=false")
    wait("Disposable PV deleted by CSI", lambda: get("pv", pv_name, missing=True) is None)
    wait("Disposable namespace removed", lambda: get("namespace", name, missing=True) is None)
    kc("delete", "storageclass", name, "--wait=true", "--timeout=30s")
    report = {"cluster": args.cluster, "node": args.node, "source_storage_class": args.storage_class, "image_digest": args.expected_image_digest,
              "finished_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "initial_capacity": "10Gi", "final_capacity": "40Gi", "sha256": checksum,
              "pvc_uid": claim["metadata"]["uid"], "replacement_pod_uid": second_pod["metadata"]["uid"],
              "resize_conditions": expanded_claim["status"].get("conditions", []),
              "filesystem": filesystem, "convergence_logs": evidence, "cleanup_complete": True}
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    print(f"PASS: evidence saved to {args.output}", flush=True)


if __name__ == "__main__":
    main()
