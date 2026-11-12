#!/usr/bin/env python3
"""Package a stable chart around the exact qualified image; compare rendered behavior.
Requires PyYAML. Does not push or publish; binary VendorVersion remains its RC identity.
"""

import argparse
import copy
import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
import yaml
from upgrade_evidence import require, validate_receipt


def normalized(documents):
    out = []
    for obj in yaml.safe_load_all(documents):
        if obj is None:
            continue

        def walk(value):
            if isinstance(value, dict):
                return {k: walk(v) for k, v in value.items() if k != "helm.sh/chart"}
            if isinstance(value, list):
                return [walk(v) for v in value]
            return value

        out.append(walk(obj))
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--candidate", type=Path, required=True)
    p.add_argument("--receipt", type=Path, required=True)
    p.add_argument("--version", required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    require(
        re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", a.version),
        "stable semantic version required",
    )
    candidate = json.loads(a.candidate.read_text())
    validate_receipt(json.loads(a.receipt.read_text()), candidate, True)
    require(
        subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        == candidate["sourceCommit"],
        "source commit differs",
    )
    require(
        subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], text=True).strip()
        == candidate["sourceTree"],
        "source tree differs",
    )
    require(
        not subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=no"], text=True
        ).strip(),
        "source tree must be clean",
    )
    with tempfile.TemporaryDirectory() as tmp:
        source = Path("helm/opennebula-csi")
        frozen = Path(tmp) / "frozen"
        stable = Path(tmp) / "stable"
        shutil.copytree(source, frozen)
        values = yaml.safe_load((frozen / "values.yaml").read_text())
        repo, digest = candidate["images"]["opennebula-csi"].split("@")
        values["image"]["repository"] = repo
        values["image"]["digest"] = digest
        mapping = {
            "provisioner": "csi-provisioner",
            "attacher": "csi-attacher",
            "resizer": "csi-resizer",
            "registrar": "node-driver-registrar",
            "liveness": "liveness-probe",
            "snapshotter": "csi-snapshotter",
        }
        for key, name in mapping.items():
            if name in candidate["images"]:
                repo, digest = candidate["images"][name].split("@")
                values["sidecarImages"][key].update(repository=repo, digest=digest)
        (frozen / "values.yaml").write_text(yaml.safe_dump(values, sort_keys=False))
        shutil.copytree(frozen, stable)
        chart = yaml.safe_load((stable / "Chart.yaml").read_text())
        chart["version"] = a.version
        (stable / "Chart.yaml").write_text(yaml.safe_dump(chart, sort_keys=False))

        def render(path):
            return subprocess.check_output(
                [
                    "helm",
                    "template",
                    "opennebula-csi",
                    str(path),
                    "--namespace",
                    "kube-system",
                    "--set",
                    "credentials.existingSecret.name=qualification-existing-secret",
                    "--include-crds",
                ],
                text=True,
            )

        require(
            normalized(render(frozen)) == normalized(render(stable)),
            "stable chart changes rendered behavior",
        )
        a.output.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["helm", "package", str(stable), "--destination", str(a.output)], check=True
        )


if __name__ == "__main__":
    main()
