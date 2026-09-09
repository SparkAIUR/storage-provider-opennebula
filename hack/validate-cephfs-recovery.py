#!/usr/bin/env python3
"""Exercise one failed FUSE client against two isolated lab volumes.

Requires an already deployed, immutable candidate image. Uses kc for every
Kubernetes operation and refuses a context other than the explicitly named lab.
"""

import argparse
import base64
import hashlib
import json
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster", required=True, help="kc cluster alias")
    parser.add_argument("--expected-context", default="hplcsi")
    parser.add_argument("--node", required=True)
    parser.add_argument("--peer-node", required=True)
    parser.add_argument("--storage-class", default="cephfs")
    parser.add_argument("--expected-image-digest", required=True)
    args = parser.parse_args()
    if not args.expected_image_digest.startswith("sha256:"):
        parser.error("an immutable image digest is required")
    if args.node == args.peer_node:
        parser.error("two different lab nodes are required")
    namespace = "csi-recovery-" + uuid.uuid4().hex[:12]
    labels = {"app.kubernetes.io/name": "cephfs-recovery-test", "test-run": namespace}

    def kc(*command, payload=None, timeout=30):
        result = subprocess.run(
            ["kc", args.cluster, *command],
            input=json.dumps(payload) if payload is not None else None,
            text=True, capture_output=True, timeout=timeout,
        )
        if result.returncode:
            raise RuntimeError(f"kc {command[0]} failed: {result.stderr.strip()}")
        return result.stdout.strip()

    def get(kind, name, ns=namespace):
        flags = ["-n", ns] if ns else []
        return json.loads(kc("get", kind, name, *flags, "-o", "json"))

    def eventually(check, description, timeout=180):
        deadline = time.monotonic() + timeout
        last_error = None
        while time.monotonic() < deadline:
            try:
                value = check()
                if value:
                    return value
            except (RuntimeError, subprocess.TimeoutExpired) as exc:
                last_error = exc
            time.sleep(2)
        raise RuntimeError(f"timed out: {description}; last error: {last_error}")

    def pod_for(name, previous_uid=None):
        data = json.loads(kc("get", "pods", "-n", namespace, "-l", "test-volume=" + name, "-o", "json"))
        for pod in data["items"]:
            if previous_uid and pod["metadata"]["uid"] == previous_uid:
                continue
            if any(c["type"] == "Ready" and c["status"] == "True" for c in pod.get("status", {}).get("conditions", [])):
                return pod
        return None

    def in_pod(pod, *command):
        return kc("exec", "-n", namespace, pod["metadata"]["name"], "--", *command)

    context = kc("config", "current-context")
    if context != args.expected_context or context in {"spark-bravo", "bravo"}:
        raise RuntimeError(f"refusing fault injection in context {context!r}; expected a lab")
    for node in [args.node, args.peer_node]:
        get("node", node, ns=None)
    driver_pods = json.loads(kc("get", "pods", "-n", "kube-system", "-l", "app.kubernetes.io/name=opennebula-csi,app.kubernetes.io/component=node", "--field-selector", "spec.nodeName=" + args.node, "-o", "json"))["items"]
    if len(driver_pods) != 1:
        raise RuntimeError("expected exactly one node plugin on the test node")
    driver_pod = driver_pods[0]
    image_id = next(c["imageID"] for c in driver_pod["status"]["containerStatuses"] if c["name"] == "opennebula-csi")
    if not image_id.endswith(args.expected_image_digest):
        raise RuntimeError("test node is not running the required immutable candidate")

    def driver_exec(code, *params):
        return kc("exec", "-n", "kube-system", driver_pod["metadata"]["name"], "-c", "opennebula-csi", "--", "python3", "-c", code, *params)

    kc("create", "-f", "-", payload={"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace, "labels": labels}})
    print(json.dumps({"phase": "created", "namespace": namespace}), flush=True)
    # On failure retain only this unique test namespace for diagnosis. No
    # cleanup trap can accidentally remove a pre-existing namespace or PVC.
    for name in ["a", "b"]:
        kc("create", "-f", "-", payload={
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": namespace, "labels": labels},
            "spec": {"accessModes": ["ReadWriteMany"], "storageClassName": args.storage_class, "resources": {"requests": {"storage": "1Gi"}}},
        })
    for name, node, claims in [("a", args.node, ["a"]), ("b", args.node, ["b"]), ("peer", args.peer_node, ["a", "b"])]:
        pod_labels = {**labels, "test-volume": name}
        kc("create", "-f", "-", payload={
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": name, "namespace": namespace, "labels": labels},
            "spec": {"replicas": 1, "selector": {"matchLabels": pod_labels}, "template": {
                "metadata": {"labels": pod_labels},
                "spec": {"nodeSelector": {"kubernetes.io/hostname": node}, "terminationGracePeriodSeconds": 10,
                    "containers": [{"name": "test", "image": "alpine:3.22", "command": ["sh", "-c", "trap 'exit 0' TERM; while :; do sleep 2; done"],
                        "volumeMounts": [{"name": v, "mountPath": "/" + v} for v in claims]}],
                    "volumes": [{"name": v, "persistentVolumeClaim": {"claimName": v}} for v in claims]},
            }},
        })
    pods = {name: eventually(lambda name=name: pod_for(name), name + " pod ready") for name in ["a", "b", "peer"]}
    handles, subpaths, expected, pv_names = {}, {}, {}, {}
    for name in ["a", "b"]:
        pvc = get("pvc", name)
        pv = get("pv", pvc["spec"]["volumeName"], ns=None)
        if pv["spec"].get("claimRef", {}).get("uid") != pvc["metadata"]["uid"]:
            raise RuntimeError("test PV claim ownership mismatch")
        handle = pv["spec"]["csi"]["volumeHandle"]
        if not handle.startswith("cephfs:"):
            raise RuntimeError("test storage class did not provision CephFS")
        handles[name] = handle
        pv_names[name] = pvc["spec"]["volumeName"]
        encoded = handle.split(":", 1)[1]
        subpaths[name] = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))["subpath"]
        payload = namespace + ":" + name
        expected[name] = hashlib.sha256(payload.encode()).hexdigest()
        in_pod(pods[name], "sh", "-c", 'printf %s "$1" > "$2"; sync "$2"', "test", payload, "/" + name + "/checkpoint")

    def checksum(pod, name):
        return in_pod(pod, "sha256sum", "/" + name + "/checkpoint").split()[0]

    for name in ["a", "b"]:
        if checksum(pods["peer"], name) != expected[name]:
            raise RuntimeError("cross-node checkpoint mismatch before injection")
    host_targets = {name: "/var/lib/kubelet/pods/" + pods[name]["metadata"]["uid"] + "/volumes/kubernetes.io~csi/" + pv_names[name] + "/mount" for name in ["a", "b"]}
    healthy_stage = "/var/lib/kubelet/plugins/kubernetes.io/csi/csi.opennebula.io/" + hashlib.sha256(handles["b"].encode()).hexdigest() + "/globalmount"
    mount_snapshot = "import json,pathlib,sys; print(json.dumps([r.split() for r in pathlib.Path('/proc/self/mountinfo').read_text().splitlines() if r.split()[4] in sys.argv[1:]]))"
    healthy_mounts = json.loads(driver_exec(mount_snapshot, healthy_stage, host_targets["b"]))
    if len(healthy_mounts) != 2:
        raise RuntimeError("healthy stage and target mount identities are ambiguous")
    find_client = """
import json,pathlib,sys
found=[]
for path in pathlib.Path('/proc').glob('[0-9]*/cmdline'):
 try:
  args=path.read_bytes().decode().strip('\\0').split('\\0')
  if pathlib.Path(args[0]).name != 'ceph-fuse' or '--client_mountpoint' not in args: continue
  if args[args.index('--client_mountpoint')+1] != sys.argv[1]: continue
  if '-f' not in args: raise RuntimeError('candidate is not supervising foreground FUSE')
  found.append({'pid':int(path.parent.name),'start':(path.parent/'stat').read_text().rsplit(')',1)[1].split()[19]})
 except (FileNotFoundError,ProcessLookupError): pass
if len(found) != 1: raise RuntimeError('expected exactly one matching FUSE client')
print(json.dumps(found[0]))
"""
    clients = {name: json.loads(driver_exec(find_client, subpaths[name])) for name in ["a", "b"]}
    if clients["a"]["pid"] == clients["b"]["pid"]:
        raise RuntimeError("two test volumes unexpectedly share one FUSE client")
    driver_exec("""
import os,pathlib,signal,sys
pid=int(sys.argv[1]); path=pathlib.Path('/proc')/str(pid)
args=(path/'cmdline').read_bytes().decode().strip('\\0').split('\\0')
assert pathlib.Path(args[0]).name == 'ceph-fuse'
assert args[args.index('--client_mountpoint')+1] == sys.argv[3]
assert (path/'stat').read_text().rsplit(')',1)[1].split()[19] == sys.argv[2]
os.kill(pid, signal.SIGKILL)
""", str(clients["a"]["pid"]), clients["a"]["start"], subpaths["a"])
    print(json.dumps({"phase": "injected", "namespace": namespace, "client": clients["a"]}), flush=True)
    stage = "/var/lib/kubelet/plugins/kubernetes.io/csi/csi.opennebula.io/" + hashlib.sha256(handles["a"].encode()).hexdigest() + "/globalmount"
    eventually(lambda: driver_exec("import os,sys; os.stat(sys.argv[1]); print('ready')", stage) == "ready", "host staging mount recovery")
    replacement_client = eventually(lambda: json.loads(driver_exec(find_client, subpaths["a"])), "replacement FUSE client")
    if replacement_client == clients["a"]:
        raise RuntimeError("failed FUSE client was not replaced")
    if json.loads(driver_exec(find_client, subpaths["b"])) != clients["b"]:
        raise RuntimeError("healthy volume's FUSE client changed")
    if json.loads(driver_exec(mount_snapshot, healthy_stage, host_targets["b"])) != healthy_mounts:
        raise RuntimeError("healthy volume mount identity changed")
    eventually(lambda: driver_exec("import hashlib,pathlib,sys; print(hashlib.sha256((pathlib.Path(sys.argv[1])/'checkpoint').read_bytes()).hexdigest())", host_targets["a"]) == expected["a"], "host target bind recovery")
    if checksum(pods["b"], "b") != expected["b"]:
        raise RuntimeError("healthy volume checksum changed")
    # Existing container namespaces can retain the dead bind. Recreate only the
    # failed consumer after proving CSI repaired the node's staging mount.
    kc("delete", "pod", pods["a"]["metadata"]["name"], "-n", namespace, "--wait=true", timeout=60)
    new_a = eventually(lambda: pod_for("a", pods["a"]["metadata"]["uid"]), "fresh recovered consumer")
    for pod, name in [(new_a, "a"), (pods["peer"], "a"), (pods["b"], "b"), (pods["peer"], "b")]:
        if checksum(pod, name) != expected[name]:
            raise RuntimeError("checksum changed after recovery")
    if pod_for("b")["metadata"]["uid"] != pods["b"]["metadata"]["uid"]:
        raise RuntimeError("healthy consumer restarted")
    print(json.dumps({"phase": "passed", "namespace": namespace, "imageID": image_id, "healthyClientUnchanged": True, "crossNodeChecksums": expected}), flush=True)
    if get("namespace", namespace, ns=None)["metadata"]["labels"].get("test-run") != namespace:
        raise RuntimeError("test namespace ownership changed; refusing cleanup")
    kc("delete", "namespace", namespace, "--wait=false")


if __name__ == "__main__":
    main()
