#!/usr/bin/env python3
"""Create or verify a public compatibility record from protected release readback."""

import argparse
import hashlib
import io
import json
import os
import re
import tarfile
import tempfile
from pathlib import Path

import yaml

from upgrade_evidence import SHA, require, validate_receipt


REPO = "https://github.com/SparkAIUR/storage-provider-opennebula"
CHARTS = "https://sparkaiur.github.io/storage-provider-opennebula/charts"
IMAGE_REPOS = ("ghcr.io/sparkaiur/opennebula-csi", "docker.io/nudevco/opennebula-csi")
HEX256 = r"[0-9a-f]{64}"
NUMBER = r"(?:0|[1-9][0-9]*)"
TAG = rf"v({NUMBER}\.{NUMBER}\.{NUMBER})(-rc\.[1-9][0-9]*)?"
PROFILE = "opennebula-74-upgrade/v1"


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def run_url(run):
    run_id, attempt = run["runId"], run["runAttempt"]
    require(
        all(type(n) is int and n > 0 for n in (run_id, attempt)),
        "invalid workflow run or attempt",
    )
    return f"{REPO}/actions/runs/{run_id}/attempts/{attempt}"


def chart_metadata(data):
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        members = [m for m in archive if m.name == "opennebula-csi/Chart.yaml"]
        require(len(members) == 1 and members[0].isfile(), "invalid chart metadata")
        return yaml.safe_load(archive.extractfile(members[0]))


def build_record(readback_bytes, readback_sha256, assets, qualification=None):
    require(sha256(readback_bytes) == readback_sha256, "approved readback hash differs")
    readback = json.loads(readback_bytes)
    tag = readback["csiTag"]
    match = re.fullmatch(TAG, tag)
    require(match, "expected a canonical CSI release tag")
    stable_version, rc = match.groups()
    version = tag[1:]
    stable = rc is None
    provider = readback["openNebulaVersion"]
    require(re.fullmatch(rf"7\.4\.{NUMBER}", provider), "expected exact OpenNebula patch")
    exclusions = ["detached-disk-growth", "disk-snapshots"]
    if not stable and provider == "7.4.0":
        exclusions.append("affected-persistent-system-self-growth-reattach")
    require(
        readback["profile"] == PROFILE and readback["exclusions"] == exclusions,
        "compatibility scope differs from the release profile",
    )

    commit, tree = readback["sourceCommit"], readback["sourceTree"]
    require(SHA.fullmatch(commit) and SHA.fullmatch(tree), "invalid source identity")
    image_digest = readback["imageDigest"]
    require(re.fullmatch(f"sha256:{HEX256}", image_digest), "invalid OCI index digest")
    images = [f"{repo}@{image_digest}" for repo in IMAGE_REPOS]

    chart_name = f"opennebula-csi-{version}.tgz"
    chart = (assets / chart_name).read_bytes()
    chart_sha256 = sha256(chart)
    require(chart_sha256 == readback["chartSHA256"], "published chart hash differs")
    metadata = chart_metadata(chart)
    require(
        metadata["name"] == "opennebula-csi" and metadata["version"] == version,
        "published chart name or version differs",
    )

    publication = readback["publication"]
    release_id = publication["releaseId"]
    require(type(release_id) is int and release_id > 0, "invalid GitHub release ID")
    publication_url = run_url(publication)
    evidence = readback["evidence"]
    evidence_url = run_url(evidence) if stable else publication_url
    if stable:
        require(qualification is not None, "stable requires protected native evidence")
        candidate = json.loads((qualification / "candidate.json").read_bytes())
        evidence_bytes = (qualification / "native.json").read_bytes()
        require(sha256(evidence_bytes) == evidence["sha256"], "native receipt hash differs")
        receipt = json.loads(evidence_bytes)
        require(receipt["providerVersion"] == provider, "native provider patch differs")
        require(
            candidate["sourceCommit"] == commit and candidate["sourceTree"] == tree,
            "qualified source identity differs",
        )
        candidate_tag = re.fullmatch(TAG, "v" + candidate["chartVersion"])
        require(
            candidate_tag and candidate_tag[1] == stable_version and candidate_tag[2],
            "qualified candidate is outside the stable release line",
        )
        require(
            candidate["images"]["opennebula-csi"]
            in (*images, images[1].removeprefix("docker.io/")),
            "qualified image differs",
        )
        require(metadata["appVersion"] == candidate_tag[0], "promoted appVersion differs")
        try:
            validate_receipt(receipt, candidate, stable=True)
        except (ValueError, TypeError, KeyError) as exc:
            raise ValueError("native receipt failed the complete stable contract") from exc
        evidence_kind = "OpenNebulaNativeQualification/v1"
    else:
        require(qualification is None, "candidate records describe an intended target")
        require(metadata["appVersion"] == tag, "candidate appVersion differs")
        evidence_bytes = (assets / "SHA256SUMS").read_bytes()
        require(sha256(evidence_bytes) == evidence["sha256"], "checksum asset hash differs")
        published = {
            "source-commit.txt": commit,
            "source-tree.txt": tree,
            "image.txt": images[0],
        }
        checksums = [f"{chart_sha256}  _candidate/{chart_name}"]
        for name, value in published.items():
            data = (assets / name).read_bytes()
            require(data == (value + "\n").encode(), f"published {name} differs")
            checksums.append(f"{sha256(data)}  _candidate/{name}")
        require(
            sorted(evidence_bytes.decode().splitlines()) == sorted(checksums),
            "published candidate assets differ from SHA256SUMS",
        )
        evidence_kind = "CandidateReleaseChecksums"

    channel = "qualified" if stable else "candidates"
    return {
        "kind": "OpenNebulaCSIReleaseCompatibility/v1",
        "status": "native-verified" if stable else "target-only",
        "openNebulaVersion": provider,
        "profile": PROFILE,
        "exclusions": exclusions,
        "discoveryRef": f"{provider}-csi.{version}",
        "csiTag": tag,
        "sourceCommit": commit,
        "sourceTree": tree,
        "images": images,
        "chart": {
            "version": version,
            "appVersion": metadata["appVersion"],
            "url": f"{CHARTS}/{channel}/{chart_name}",
            "sha256": chart_sha256,
        },
        "publication": {
            "releaseId": release_id,
            "url": publication_url,
            "readbackSHA256": readback_sha256,
        },
        "evidence": {
            "kind": evidence_kind,
            "url": evidence_url,
            "sha256": evidence["sha256"],
        },
    }


def store_record(directory, record, verify):
    path = directory / f"release-compatibility-{record['discoveryRef']}.json"
    data = (json.dumps(record, indent=2, sort_keys=True) + "\n").encode()
    if not verify:
        # Publish complete bytes atomically, without ever replacing an existing record.
        with tempfile.NamedTemporaryFile(dir=directory) as pending:
            pending.write(data)
            pending.flush()
            try:
                os.link(pending.name, path)
            except FileExistsError:
                pass
    require(path.is_file() and not path.is_symlink(), "expected a regular compatibility record")
    require(path.read_bytes() == data, "immutable record differs")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("author", "verify"))
    parser.add_argument("--readback", type=Path, required=True)
    parser.add_argument("--approved-readback-sha256", required=True)
    parser.add_argument("--release-assets", type=Path, required=True)
    parser.add_argument("--qualification", type=Path)
    parser.add_argument("--record-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        record = build_record(
            args.readback.read_bytes(),
            args.approved_readback_sha256,
            args.release_assets,
            args.qualification,
        )
        print(store_record(args.record_dir, record, verify=args.command == "verify"))
    except ValueError as exc:
        # Avoid printing parser errors that could include values from controlled inputs.
        if type(exc) is not ValueError:
            parser.exit(1, "invalid compatibility inputs\n")
        parser.exit(1, f"{exc}\n")
    except (KeyError, TypeError, OSError, tarfile.TarError, yaml.YAMLError):
        parser.exit(1, "missing or malformed compatibility inputs or record\n")


if __name__ == "__main__":
    main()
