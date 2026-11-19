# OpenNebula release compatibility

The installed validation target is OpenNebula **7.4.0**. Treat **7.4.2** as a new target that needs its own native evidence. A CSI release carries a compatibility claim for one exact provider patch and the recorded artifacts and profile.

| Publication | Canonical CSI tag | Helm chart version | Immutable discovery reference | Record status |
| --- | --- | --- | --- | --- |
| RC4 on the 7.4.0 target | `v0.6.0-rc.4` | `0.6.0-rc.4` | `7.4.0-csi.0.6.0-rc.4` | `target-only` |
| Stable after native qualification on 7.4.0 | `v0.6.0` | `0.6.0` | `7.4.0-csi.0.6.0` | `native-verified` |

These names describe the release policy. Candidate publication establishes artifact custody and target intent. Stable promotion retains the qualified RC image digest and binary `VendorVersion`. The stable chart retains the RC `appVersion` and uses the stable chart version. GitOps pins the exact Helm version and chart package SHA-256 plus the OCI image digests for the driver and sidecars. Verify the served package and its index entry against the recorded chart SHA-256 before accepting the pin.

The profile `opennebula-74-upgrade/v1` follows the [native qualification contract](opennebula-74-upgrade.md). Detached disk growth and disk-path snapshots are excluded from positive support and retain their rejection tests. On 7.4.0, affected persistent SYSTEM/SELF growth and growth on reattach are guarded. The candidate record lists this as a further exclusion. The current stable gate still requires positive attached growth and ordinary reattach on the affected path, with verified capacity and data. A successful guard rejection cannot satisfy that gate. Stable also requires all other native results and separate ordered 24-hour observations for hpldb, hplsvc, and Bravo. Review the approved provider-version readback for each qualifying environment. The receipt must report the exact observed patch.

Use a provider-qualified discovery reference to locate its immutable record. `7.4.0-stable` is an optional mutable discovery channel for an exact 7.4.0 qualified release. Keep it out of GitOps pins, Helm versions, and the RC publication workflow. Before moving it, require protected approval of the existing destination, the new immutable record, the exact artifact digests, and the complete native contract. Read it back after an approved update and verify its destination against the record.

## Read back the published release

Run these checks after the appropriate [candidate publication](../.github/workflows/release-csi.yaml) or [stable promotion](../.github/workflows/promote-csi.yaml) finishes. Record the exact workflow run and attempt, and confirm success, the expected workflow and event, the source commit, the release identity, and the required-reviewer environment approval. For stable, also verify the successful exact-head [native qualification](../.github/workflows/native-qualification.yaml) run and attempt referenced by the promotion. In the controlled workspace, retrieve the exact `candidate.json` and `native.json` used by that promotion. If run, attempt, and artifact bytes cannot be tied to the protected execution, stop.

Set `csi_tag`, `release_run`, `release_attempt`, `assets`, and `channel` from that readback. Use `candidates` for an RC or `qualified` for stable. Use a fresh local `assets` directory. The following operations read the release and its distribution endpoints. The tag fetch updates local `FETCH_HEAD` only.

```sh
repo=SparkAIUR/storage-provider-opennebula
chart=opennebula-csi-${csi_tag#v}.tgz
pages=https://sparkaiur.github.io/storage-provider-opennebula/charts/$channel
rtk proxy gh api "repos/$repo/actions/runs/$release_run/attempts/$release_attempt" \
  --jq '{path,event,head_sha,conclusion,run_attempt}'
rtk proxy git fetch --no-tags origin "refs/tags/$csi_tag"
rtk proxy git rev-parse 'FETCH_HEAD^{commit}' 'FETCH_HEAD^{tree}'
rtk proxy gh release download "$csi_tag" --repo "$repo" --dir "$assets" --pattern "$chart"
rtk proxy curl -fsSL "$pages/$chart" -o "$assets/served-chart.tgz"
rtk proxy cmp "$assets/$chart" "$assets/served-chart.tgz"
rtk proxy shasum -a 256 "$assets/$chart"
rtk proxy helm show chart "$assets/$chart"
rtk proxy curl -fsSL "$pages/index.yaml" -o "$assets/index.yaml"
for image in ghcr.io/sparkaiur/opennebula-csi docker.io/nudevco/opennebula-csi; do
  rtk proxy docker buildx imagetools inspect "$image:$csi_tag" --format '{{.Manifest.Digest}}'
done
```

Confirm the matching `opennebula-csi` index entry has the exact version, served archive URL and digest. Require both image digests to agree. For a candidate, download the remaining release assets with `rtk proxy gh release download "$csi_tag" --repo "$repo" --dir "$assets" --pattern image.txt --pattern source-commit.txt --pattern source-tree.txt --pattern SHA256SUMS`. For stable, require the promoted digest to match the qualified image and retain the successful promotion chart-equivalence result. Use the native receipt's exact version and confirm it against approved provider readback.

## Author and verify the record

Save `release-readback.json` in controlled storage with these fields. Values in angle brackets below are placeholders for the observed values. `runId` and `runAttempt` are positive JSON integers. `chartSHA256` and `evidence.sha256` are lowercase hex without a `sha256:` prefix.

```json
{
  "csiTag": "v0.6.0-rc.4",
  "openNebulaVersion": "7.4.0",
  "profile": "opennebula-74-upgrade/v1",
  "exclusions": ["detached-disk-growth", "disk-snapshots", "affected-persistent-system-self-growth-reattach"],
  "sourceCommit": "<tag commit>",
  "sourceTree": "<tag tree>",
  "imageDigest": "sha256:<matching OCI index digest>",
  "chartSHA256": "<downloaded and served chart SHA-256>",
  "publication": {"runId": 0, "runAttempt": 0},
  "evidence": {"runId": 0, "runAttempt": 0, "sha256": "<evidence SHA-256>"}
}
```

For a candidate, `evidence` names the same publication attempt and hashes the downloaded `SHA256SUMS` bytes. Include the affected SYSTEM/SELF exclusion only for a 7.4.0 candidate. For stable, change `csiTag` to `v0.6.0`, use only the first two exclusions, and set `evidence` to the protected qualification run and attempt and the raw-byte SHA-256 of its `native.json`. An authenticated reviewer approves the exact readback file hash in the protected release record after verifying the scope, authenticity, and publication checks. Retrieve `approved_readback_sha256` from that approval.

Run from a checkout containing the helper. Set `records` to an existing output directory. For stable, add `--qualification "$qualification"` to both invocations, where `qualification` is the controlled directory holding the two qualification inputs.

```sh
for mode in author verify; do
  rtk proxy uv run --no-project --with PyYAML==6.0.3 python hack/release_compatibility.py "$mode" \
    --readback release-readback.json --approved-readback-sha256 "$approved_readback_sha256" \
    --release-assets "$assets" --record-dir "$records" || exit
done
```

Publish only the generated `release-compatibility-<discovery-reference>.json` at its immutable discovery location after the release review. Download it and run `verify` against the downloaded copy using the same approved readback. Retries accept byte-identical records. Any changed scope, source, digests, run attempt or evidence needs separate review, and earlier immutable records stay in place. A changed artifact needs fresh custody and native qualification before any stable claim.

The helper checks local bytes, exact identity and version, and the complete native receipt contract for stable. Protected operators and workflows authenticate approvals, workflow runs, artifact origin, runtime version, and the actual native test results. Local fixtures cannot provide those facts. Keep credentials, operational receipts, and controlled qualification inputs in their approved storage. The existing promotion workflow attaches its qualification inputs to the GitHub release. Before promotion, verify whether those inputs are cleared for that release's audience and contain only approved material. If the workflow would expose controlled material, hold promotion until its publication step is addressed under separate review.
