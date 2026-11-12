#!/usr/bin/env python3
"""Render only scoped non-secret CSI inputs; never invoke the secretGenerator."""

import argparse
import copy
import json
import subprocess
from pathlib import Path
import yaml
from upgrade_evidence import require


def apply_patch(document, patch):
    for change in patch:
        require(change["op"] in ("add", "replace"), "unexpected patch mutation")
        segments = [
            s.replace("~1", "/").replace("~0", "~")
            for s in change["path"].split("/")[1:]
        ]
        target = document
        for segment in segments[:-1]:
            target = (
                target[int(segment)] if isinstance(target, list) else target[segment]
            )
        if change["op"] == "replace":
            require(segments[-1] in target, "replace path missing")
        target[segments[-1]] = change["value"]
    return document


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gitops", type=Path, required=True)
    args = parser.parse_args()
    root = args.gitops
    base = yaml.safe_load(
        (root / "infrastructure/opennebula-csi/release.yaml").read_text()
    )
    for cluster, budget in [("hpldb", 150), ("hplsvc", 360), ("bravo", 450)]:
        parent = yaml.safe_load(
            (
                root / "clusters" / cluster / "kube-system/opennebula-csi.yaml"
            ).read_text()
        )
        require(
            parent["spec"]["wait"] and parent["spec"]["timeout"] == f"{budget + 15}m",
            "parent wait budget differs",
        )
        release = copy.deepcopy(base)
        for patch in parent["spec"]["patches"]:
            if patch["target"]["kind"] == "HelmRelease":
                apply_patch(release, yaml.safe_load(patch["patch"]))
        spec = release["spec"]
        require(
            spec["suspend"] is True,
            "preparation must remain suspended until candidate digest approval",
        )
        require(spec["timeout"] == f"{budget}m", "Helm wait budget differs")
        require(
            spec["chart"]["spec"]["version"] == "0.6.0-rc.1"
            and spec["chart"]["spec"]["sourceRef"]["name"]
            == "opennebula-csi-candidates",
            "candidate channel/version differs",
        )
        for action in ("install", "upgrade"):
            require(
                spec[action]["disableWait"] is False
                and spec[action]["disableWaitForJobs"] is False,
                "wait disabled",
            )
            require(
                spec[action]["remediation"]["retries"] == 0
                and spec[action]["remediation"]["remediateLastFailure"] is False,
                "automatic remediation enabled",
            )
        require(
            spec["upgrade"]["cleanupOnFail"] is False,
            "failed resources would be removed",
        )
        values = spec["values"]
        require(
            values["snapshotter"]["enabled"] is False
            and values["featureGates"]["detachedDiskExpansion"] is False,
            "unsupported feature enabled",
        )
        output = subprocess.check_output(
            [
                "rtk",
                "proxy",
                "helm",
                "template",
                "opennebula-csi",
                "helm/opennebula-csi",
                "--namespace",
                "kube-system",
                "-f",
                "-",
            ],
            input=yaml.safe_dump(values),
            text=True,
        )
        objects = [o for o in yaml.safe_load_all(output) if o]
        ds = next(o for o in objects if o["kind"] == "DaemonSet")
        require(
            ds["spec"]["updateStrategy"] == {"type": "OnDelete"},
            "rendered node strategy differs",
        )
        controller = next(o for o in objects if o["kind"] == "StatefulSet")
        require(
            all(
                c["name"] != "csi-snapshotter"
                for c in controller["spec"]["template"]["spec"]["containers"]
            ),
            "snapshotter rendered",
        )
        print(
            json.dumps(
                {
                    "cluster": cluster,
                    "suspended": True,
                    "helmMinutes": budget,
                    "parentMinutes": budget + 15,
                    "renderedObjects": len(objects),
                }
            )
        )


if __name__ == "__main__":
    main()
